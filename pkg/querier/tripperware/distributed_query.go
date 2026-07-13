package tripperware

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/distributed_execution"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	stepBatch = 10
)

func DistributedQueryMiddleware(defaultEvaluationInterval time.Duration, lookbackDelta time.Duration, baseOptimizers []logicalplan.Optimizer, limits Limits) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return distributedQueryMiddleware{
			next:                      next,
			lookbackDelta:             lookbackDelta,
			defaultEvaluationInterval: defaultEvaluationInterval,
			baseOptimizers:            baseOptimizers,
			limits:                    limits,
		}
	})
}

func getStartAndEnd(start time.Time, end time.Time, step time.Duration) (time.Time, time.Time) {
	if step == 0 {
		return start, start
	}
	return start, end
}

type distributedQueryMiddleware struct {
	next                      Handler
	defaultEvaluationInterval time.Duration
	lookbackDelta             time.Duration
	baseOptimizers            []logicalplan.Optimizer
	limits                    Limits
}

// shardCountForContext resolves the metric-name shard count for the request's
// tenant(s), matching the compactor's per-tenant metric-name-shard-size so the
// optimizer's per-shard subqueries route to disjoint blocks.
func (d distributedQueryMiddleware) shardCountForContext(ctx context.Context) int {
	tenantIDs, err := users.TenantIDs(ctx)
	if err != nil || len(tenantIDs) == 0 {
		return 0
	}
	return validation.SmallestPositiveIntPerTenant(tenantIDs, d.limits.GetMetricNameShardSize)
}

func (d distributedQueryMiddleware) newLogicalPlan(ctx context.Context, qs string, start time.Time, end time.Time, step time.Duration) (*logicalplan.Plan, error) {

	start, end = getStartAndEnd(start, end, step)

	qOpts := query.Options{
		Start:      start,
		End:        end,
		Step:       step,
		StepsBatch: stepBatch,
		NoStepSubqueryIntervalFn: func(duration time.Duration) time.Duration {
			return d.defaultEvaluationInterval
		},
		// Hardcoded value for execution-time-params that will be re-populated again in the querier stage
		LookbackDelta:      d.lookbackDelta,
		EnablePerStepStats: false,
	}

	expr, err := parser.NewParser(qs, parser.WithFunctions(parser.Functions)).ParseExpr()
	if err != nil {
		return nil, err
	}

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	logicalPlan, err := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	if err != nil {
		return nil, err
	}

	// Append the distributed optimizer configured with the tenant's metric-name
	// shard count. A non-positive shard count makes the optimizer a no-op.
	optimizers := make([]logicalplan.Optimizer, 0, len(d.baseOptimizers)+1)
	optimizers = append(optimizers, d.baseOptimizers...)
	optimizers = append(optimizers, &distributed_execution.DistributedOptimizer{ShardCount: d.shardCountForContext(ctx)})

	optimizedPlan, _ := logicalPlan.Optimize(optimizers)

	return &optimizedPlan, nil
}

func (d distributedQueryMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	promReq, ok := r.(*PrometheusRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}

	startTime := time.Unix(0, promReq.Start*int64(time.Millisecond))
	endTime := time.Unix(0, promReq.End*int64(time.Millisecond))
	step := time.Duration(promReq.Step) * time.Millisecond

	var err error

	newLogicalPlan, err := d.newLogicalPlan(ctx, promReq.Query, startTime, endTime, step)
	if err != nil {
		return nil, err
	}

	promReq.LogicalPlan = *newLogicalPlan

	return d.next.Do(ctx, r)
}
