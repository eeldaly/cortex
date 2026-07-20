package distributed_execution

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/cortexproject/cortex/pkg/distributed_execution/querierpb"
	"github.com/cortexproject/cortex/pkg/ring/client"
)

const (
	RemoteNode = "RemoteNode"
)

// (to verify interface implementations)
var _ logicalplan.Node = (*Remote)(nil)
var _ logicalplan.UserDefinedExpr = (*Remote)(nil)
var _ model.VectorOperator = (*DistributedRemoteExecution)(nil)

// Remote is a custom node that marks where the portion of logical plan
// that needs to be executed remotely
type Remote struct {
	Expr logicalplan.Node `json:"-"`

	FragmentKey FragmentKey

	clientPool *client.Pool
}

func NewRemoteNode(Expr logicalplan.Node) logicalplan.Node {
	return &Remote{
		// initialize the fragment key pointer first
		Expr:        Expr,
		FragmentKey: FragmentKey{},
	}
}
func (r *Remote) Clone() logicalplan.Node {
	return &Remote{Expr: r.Expr.Clone(), FragmentKey: r.FragmentKey, clientPool: r.clientPool}
}
func (r *Remote) Children() []*logicalplan.Node {
	return []*logicalplan.Node{&r.Expr}
}
func (r *Remote) String() string {
	return fmt.Sprintf("remote(%s)", r.Expr.String())
}
func (r *Remote) ReturnType() parser.ValueType {
	return r.Expr.ReturnType()
}
func (r *Remote) Type() logicalplan.NodeType { return RemoteNode }

// InsertClientPool injects the querier client pool used by the execution operator to
// pull intermediate results from the querier executing the child fragment.
func (r *Remote) InsertClientPool(clientPool *client.Pool) {
	r.clientPool = clientPool
}

type remote struct {
	QueryID    uint64
	FragmentID uint64
}

func (r *Remote) MarshalJSON() ([]byte, error) {
	return json.Marshal(remote{
		QueryID:    r.FragmentKey.queryID,
		FragmentID: r.FragmentKey.fragmentID,
	})
}

func (r *Remote) UnmarshalJSON(data []byte) error {
	re := remote{}
	if err := json.Unmarshal(data, &re); err != nil {
		return err
	}

	r.FragmentKey = MakeFragmentKey(re.QueryID, re.FragmentID)
	return nil
}

// MakeExecutionOperator creates a distributed execution operator from a Remote node.
// This implements the logicalplan.UserDefinedExpr interface, allowing Remote nodes
// to be transformed into custom distributed execution operators during query processing.
func (r *Remote) MakeExecutionOperator(
	ctx context.Context,
	opts *query.Options,
	hints storage.SelectHints,
) (model.VectorOperator, error) {
	remoteExec, err := newDistributedRemoteExecution(ctx, r.clientPool, r.FragmentKey, opts)
	if err != nil {
		return nil, err
	}

	return exchange.NewConcurrent(remoteExec, 2, opts), nil
}

// DistributedRemoteExecution is a Thanos engine volcano-model operator that pulls
// the results of a child fragment from a peer querier over gRPC, exposing them to the
// parent fragment's execution pipeline as if they were produced locally.
type DistributedRemoteExecution struct {
	client querierpb.QuerierClient

	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	numSteps    int

	stream      querierpb.Querier_NextClient
	buffer      []model.StepVector
	bufferIndex int

	batchSize   int64
	series      []labels.Labels
	fragmentKey FragmentKey
	addr        string
	initialized bool // track if stream is initialized
}

type QuerierAddrKey struct{}

// newDistributedRemoteExecution creates a DistributedRemoteExecution operator that executes
// queries across distributed queriers. It implements Thanos engine's logical plan execution by:
//  1. Streaming series metadata to discover the data shape
//  2. Fetching actual data values via subsequent Next calls
//
// Unlike local execution, this operator retrieves data from remote querier processes,
// enabling distributed query processing across multiple nodes.
func newDistributedRemoteExecution(ctx context.Context, pool *client.Pool, fragmentKey FragmentKey, queryOpts *query.Options) (*DistributedRemoteExecution, error) {
	_, _, _, childIDToAddr, _ := ExtractFragmentMetaData(ctx)

	addr := childIDToAddr[fragmentKey.fragmentID]
	poolClient, err := pool.GetClientFor(addr)
	if err != nil {
		return nil, err
	}

	client, ok := poolClient.(*querierClient)
	if !ok {
		return nil, fmt.Errorf("invalid client type from pool")
	}

	d := &DistributedRemoteExecution{
		client: client,

		mint:        queryOpts.Start.UnixMilli(),
		maxt:        queryOpts.End.UnixMilli(),
		step:        queryOpts.Step.Milliseconds(),
		currentStep: queryOpts.Start.UnixMilli(),
		numSteps:    queryOpts.TotalSteps(),

		batchSize:   1000,
		fragmentKey: fragmentKey,
		addr:        addr,
		buffer:      []model.StepVector{},
		bufferIndex: 0,
		initialized: false,
	}

	if d.step == 0 {
		d.step = 1
	}

	return d, nil
}

func (d *DistributedRemoteExecution) Series(ctx context.Context) ([]labels.Labels, error) {
	if d.series != nil {
		return d.series, nil
	}

	req := &querierpb.SeriesRequest{
		QueryID:    d.fragmentKey.queryID,
		FragmentID: d.fragmentKey.fragmentID,
		Batchsize:  d.batchSize,
	}

	stream, err := d.client.Series(ctx, req)
	if err != nil {
		return nil, err
	}

	var series []labels.Labels

	for {
		seriesBatch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		for _, s := range seriesBatch.OneSeries {
			oneSeries := make(map[string]string, len(s.Labels))
			for _, l := range s.Labels {
				oneSeries[l.Name] = l.Value
			}
			series = append(series, labels.FromMap(oneSeries))
		}
	}

	d.series = series
	return series, nil
}

// Next fills the caller-provided buffer with up to len(buf) StepVectors pulled from the
// remote querier and returns the number written. A return value of 0 signals completion.
func (d *DistributedRemoteExecution) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if len(buf) == 0 {
		return 0, nil
	}

	// Refill the internal buffer from the stream if it has been fully drained.
	// Loop to skip any empty (non-final) batches the server may send.
	for d.bufferIndex >= len(d.buffer) {
		eof, err := d.refill(ctx)
		if err != nil {
			return 0, err
		}
		if eof {
			// No more data available.
			return 0, nil
		}
	}

	// Copy as many step vectors as fit in the caller's buffer.
	n := 0
	for n < len(buf) && d.bufferIndex < len(d.buffer) {
		sv := d.buffer[d.bufferIndex]
		buf[n].T = sv.T
		buf[n].SampleIDs = sv.SampleIDs
		buf[n].Samples = sv.Samples
		buf[n].HistogramIDs = sv.HistogramIDs
		buf[n].Histograms = sv.Histograms
		n++
		d.bufferIndex++
	}

	return n, nil
}

// refill initializes the stream (on first use) and reads the next batch of step
// vectors from the remote querier into the internal buffer. It returns true when
// the stream has been exhausted (EOF).
func (d *DistributedRemoteExecution) refill(ctx context.Context) (bool, error) {
	if !d.initialized {
		req := &querierpb.NextRequest{
			QueryID:    d.fragmentKey.queryID,
			FragmentID: d.fragmentKey.fragmentID,
			Batchsize:  d.batchSize,
		}
		stream, err := d.client.Next(ctx, req)
		if err != nil {
			return false, fmt.Errorf("failed to initialize stream: %w", err)
		}
		d.stream = stream
		d.initialized = true
	}

	batch, err := d.stream.Recv()
	if err == io.EOF {
		d.buffer = nil
		d.bufferIndex = 0
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("error receiving from stream: %w", err)
	}

	d.buffer = make([]model.StepVector, len(batch.StepVectors))
	for i, sv := range batch.StepVectors {
		d.buffer[i] = model.StepVector{
			T:            sv.T,
			SampleIDs:    sv.Sample_IDs,
			Samples:      sv.Samples,
			HistogramIDs: sv.Histogram_IDs,
			Histograms:   floatHistogramProtoToFloatHistograms(sv.Histograms),
		}
	}
	d.bufferIndex = 0
	d.currentStep += d.step * int64(len(d.buffer))
	return false, nil
}

func (d *DistributedRemoteExecution) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{}
}

func (d *DistributedRemoteExecution) String() string {
	return "DistributedRemoteExecution(" + d.addr + ")"
}
