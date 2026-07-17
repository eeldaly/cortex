package distributed_execution

import (
	"context"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/distributed_execution/querierpb"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

// TestQuerierPool verifies that the querier service client pool correctly manages
// client connections by testing address addition and client retrieval functionality
func TestQuerierPool(t *testing.T) {
	tests := []struct {
		name      string
		poolSetup func() (*client.Pool, *mockServer)
		test      func(*testing.T, *client.Pool, *mockServer)
	}{
		{
			name: "pool creates and manages clients",
			poolSetup: func() (*client.Pool, *mockServer) {
				mockServer := newMockServer(t)

				cfg := grpcclient.Config{
					MaxRecvMsgSize: 1024,
					MaxSendMsgSize: 1024,
				}

				reg := prometheus.NewRegistry()
				logger := log.NewNopLogger()

				pool := NewQuerierPool(cfg, reg, logger)

				return pool, mockServer
			},
			test: func(t *testing.T, pool *client.Pool, mockServer *mockServer) {
				// test getting client
				client, err := pool.GetClientFor(":8005")
				assert.NoError(t, err)
				assert.NotNil(t, client)

				// test client is reused
				client2, err := pool.GetClientFor(":8005")
				assert.NoError(t, err)
				assert.Equal(t, client, client2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, mockServer := tt.poolSetup()
			defer mockServer.Stop()
			tt.test(t, pool, mockServer)
		})
	}
}

type mockQuerierServer struct {
	querierpb.UnimplementedQuerierServer
}

func (m *mockQuerierServer) Next(req *querierpb.NextRequest, stream querierpb.Querier_NextServer) error {
	return nil
}

func (m *mockQuerierServer) Series(req *querierpb.SeriesRequest, stream querierpb.Querier_SeriesServer) error {
	return nil
}

type mockHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (m *mockHealthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

type mockServer struct {
	server *grpc.Server
	addr   int
}

func newMockServer(t *testing.T) *mockServer {
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		LogSourceIPs:      true,
		MetricsNamespace:  "with_source_ip_extractor",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	mockQuerier := &mockQuerierServer{}
	querierpb.RegisterQuerierServer(server.GRPC, mockQuerier)
	grpc_health_v1.RegisterHealthServer(server.GRPC, &mockHealthServer{})

	return &mockServer{
		server: server.GRPC,
		addr:   serverCfg.GRPCListenPort,
	}
}

func (m *mockServer) Stop() {
	if m.server != nil {
		m.server.Stop()
	}
}

// TestClientBuffer verifies that the execution operator streams step vectors from the
// remote querier in order, filling the caller-provided buffer up to its length, and
// signals completion by returning zero.
func TestClientBuffer(t *testing.T) {
	tests := []struct {
		name       string
		batches    []*querierpb.StepVectorBatch
		bufSize    int
		wantTs     []int64
		wantSamps  [][]float64
		wantSampID [][]uint64
	}{
		{
			name: "single step per call",
			batches: []*querierpb.StepVectorBatch{
				{StepVectors: []*querierpb.StepVector{
					{T: 1000, Sample_IDs: []uint64{1}, Samples: []float64{10.0}},
					{T: 2000, Sample_IDs: []uint64{1}, Samples: []float64{20.0}},
				}},
			},
			bufSize:    1,
			wantTs:     []int64{1000, 2000},
			wantSamps:  [][]float64{{10.0}, {20.0}},
			wantSampID: [][]uint64{{1}, {1}},
		},
		{
			name: "multiple steps per call",
			batches: []*querierpb.StepVectorBatch{
				{StepVectors: []*querierpb.StepVector{
					{T: 1000, Sample_IDs: []uint64{1}, Samples: []float64{10.0}},
					{T: 2000, Sample_IDs: []uint64{1}, Samples: []float64{20.0}},
				}},
			},
			bufSize:    2,
			wantTs:     []int64{1000, 2000},
			wantSamps:  [][]float64{{10.0}, {20.0}},
			wantSampID: [][]uint64{{1}, {1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := &DistributedRemoteExecution{
				client:      &mockQuerierClient{batches: tt.batches},
				mint:        0,
				maxt:        5000,
				step:        1000,
				currentStep: 0,
				numSteps:    1,
				batchSize:   int64(tt.bufSize),
				buffer:      []model.StepVector{},
				bufferIndex: 0,
				initialized: false,
			}

			ctx := context.Background()

			var gotTs []int64
			var gotSamps [][]float64
			var gotSampID [][]uint64
			for {
				buf := make([]model.StepVector, tt.bufSize)
				n, err := exec.Next(ctx, buf)
				require.NoError(t, err)
				if n == 0 {
					break
				}
				for i := range n {
					gotTs = append(gotTs, buf[i].T)
					gotSamps = append(gotSamps, buf[i].Samples)
					gotSampID = append(gotSampID, buf[i].SampleIDs)
				}
			}

			assert.Equal(t, tt.wantTs, gotTs)
			assert.Equal(t, tt.wantSamps, gotSamps)
			assert.Equal(t, tt.wantSampID, gotSampID)
		})
	}
}

type mockQuerierClient struct {
	querierpb.QuerierClient
	batches []*querierpb.StepVectorBatch
}

func (m *mockQuerierClient) Series(ctx context.Context, req *querierpb.SeriesRequest, opts ...grpc.CallOption) (querierpb.Querier_SeriesClient, error) {
	return &mockSeriesStream{}, nil
}

func (m *mockQuerierClient) Next(ctx context.Context, req *querierpb.NextRequest, opts ...grpc.CallOption) (querierpb.Querier_NextClient, error) {
	return &mockNextStream{batches: m.batches}, nil
}

type mockSeriesStream struct {
	querierpb.Querier_SeriesClient
}

func (m *mockSeriesStream) Recv() (*querierpb.SeriesBatch, error) {
	return nil, io.EOF
}

type mockNextStream struct {
	querierpb.Querier_NextClient
	batches []*querierpb.StepVectorBatch
	idx     int
}

func (m *mockNextStream) Recv() (*querierpb.StepVectorBatch, error) {
	if m.idx >= len(m.batches) {
		return nil, io.EOF
	}
	b := m.batches[m.idx]
	m.idx++
	return b, nil
}
