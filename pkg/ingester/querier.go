package ingester

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

func NewShardByMetricNameBlockQuerier(b tsdb.BlockReader, mint, maxt int64, queriedBlocks, skippedBlocks prometheus.Counter) (storage.Querier, error) {
	q, err := tsdb.NewBlockQuerier(b, mint, maxt)
	if err != nil {
		return nil, fmt.Errorf("open querier for block %s: %w", b, err)
	}

	ok, shardCount, shardIdx := shardByMetricNameInfoFromBlockMeta(b.Meta())
	if ok {
		q = NewShardByMetricNameQuerier(q, shardCount, shardIdx, queriedBlocks, skippedBlocks)
	}
	return q, nil
}

func NewShardByMetricNameBlockChunkQuerier(b tsdb.BlockReader, q storage.ChunkQuerier, mint, maxt int64, queriedBlocks, skippedBlocks prometheus.Counter) (storage.ChunkQuerier, error) {
	ok, shardCount, shardIdx := shardByMetricNameInfoFromBlockMeta(b.Meta())
	if ok {
		q = NewShardByMetricNameChunkQuerier(q, shardCount, shardIdx, queriedBlocks, skippedBlocks)
	}
	return q, nil
}

func shardByMetricNameInfoFromBlockMeta(meta tsdb.BlockMeta) (bool, uint64, uint64) {
	hints := meta.Compaction.Hints
	for _, hint := range hints {
		if hint == tsdb.CompactionHintFromOutOfOrder {
			continue
		}
		// The hint will be in format `metric_name_shard_info=<shard_id>_<shard_count>`
		strs := strings.Split(hint, "=")
		// Safeguard.
		if len(strs) != 2 {
			continue
		}
		// Find the first metric name shard hint.
		if strs[0] == metricNameShardInfo {
			parts := strings.Split(strs[1], "_")
			// Safeguard.
			if len(parts) != 2 {
				continue
			}
			shardCount, err := strconv.Atoi(parts[1])
			if err != nil || shardCount < 0 {
				continue
			}
			shardID, err := strconv.Atoi(parts[0])
			if err != nil || shardID < 0 {
				continue
			}
			return true, uint64(shardCount), uint64(shardID)
		}
	}
	return false, 0, 0
}

// queryShardSelectorLabel is the synthetic label the distributed-execution
// optimizer injects into per-shard subqueries (value format "<count>_<idx>").
// The storage querier consumes it to route a subquery to only the blocks that
// belong to the requested shard, then strips it so it never reaches the
// underlying block querier (no real series carries this label).
const queryShardSelectorLabel = "__cortex_ingester_shard__"

// extractShardSelector removes the query-directed shard selector matcher (if
// present) from the matcher set and returns the parsed shard count/index.
func extractShardSelector(matchers []*labels.Matcher) (remaining []*labels.Matcher, count uint64, idx uint64, ok bool) {
	remaining = make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		if m.Name == queryShardSelectorLabel {
			if parts := strings.Split(m.Value, "_"); len(parts) == 2 {
				c, err1 := strconv.ParseUint(parts[0], 10, 64)
				i, err2 := strconv.ParseUint(parts[1], 10, 64)
				if err1 == nil && err2 == nil {
					count, idx, ok = c, i, true
				}
			}
			continue
		}
		remaining = append(remaining, m)
	}
	return remaining, count, idx, ok
}

// ShardByMetricNameQuerier is a querier for block sharded with known shard index and shard count.
// It can filter out queries with metric name specified using an equal matcher by hashmod.
type ShardByMetricNameQuerier struct {
	storage.Querier
	shardCount, shardIdx uint64
	skippedBlocks        prometheus.Counter
	queriedBlocks        prometheus.Counter
}

func NewShardByMetricNameQuerier(q storage.Querier, shardCount, shardIdx uint64, queriedBlocks, skippedBlocks prometheus.Counter) *ShardByMetricNameQuerier {
	return &ShardByMetricNameQuerier{
		Querier:       q,
		shardCount:    shardCount,
		shardIdx:      shardIdx,
		skippedBlocks: skippedBlocks,
		queriedBlocks: queriedBlocks,
	}
}

func (q *ShardByMetricNameQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	matchers, reqCount, reqIdx, hasReq := extractShardSelector(matchers)
	if q.shardCount > 0 {
		// Query-directed routing: when the optimizer requests a specific shard,
		// only serve this block if it belongs to that shard.
		if hasReq && reqCount == q.shardCount && reqIdx != q.shardIdx {
			q.skippedBlocks.Inc()
			return storage.EmptySeriesSet()
		}
		for _, matcher := range matchers {
			if matcher.Name == labels.MetricName && matcher.Type == labels.MatchEqual {
				if hash(matcher.Value)%q.shardCount != q.shardIdx {
					q.skippedBlocks.Inc()
					return storage.EmptySeriesSet()
				}
			}
		}
	}
	q.queriedBlocks.Inc()
	return q.Querier.Select(ctx, sortSeries, hints, matchers...)
}

type ShardByMetricNameChunkQuerier struct {
	storage.ChunkQuerier
	shardCount, shardIdx uint64
	skippedBlocks        prometheus.Counter
	queriedBlocks        prometheus.Counter
}

func NewShardByMetricNameChunkQuerier(q storage.ChunkQuerier, shardCount, shardIdx uint64, queriedBlocks, skippedBlocks prometheus.Counter) *ShardByMetricNameChunkQuerier {
	return &ShardByMetricNameChunkQuerier{
		ChunkQuerier:  q,
		shardCount:    shardCount,
		shardIdx:      shardIdx,
		skippedBlocks: skippedBlocks,
		queriedBlocks: queriedBlocks,
	}
}

func (q *ShardByMetricNameChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	matchers, reqCount, reqIdx, hasReq := extractShardSelector(matchers)
	if q.shardCount > 0 {
		if hasReq && reqCount == q.shardCount && reqIdx != q.shardIdx {
			q.skippedBlocks.Inc()
			return storage.EmptyChunkSeriesSet()
		}
		for _, matcher := range matchers {
			if matcher.Name == labels.MetricName && matcher.Type == labels.MatchEqual {
				if hash(matcher.Value)%q.shardCount != q.shardIdx {
					q.skippedBlocks.Inc()
					return storage.EmptyChunkSeriesSet()
				}
			}
		}
	}
	q.queriedBlocks.Inc()
	return q.ChunkQuerier.Select(ctx, sortSeries, hints, matchers...)
}
