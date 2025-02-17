package scanner

import (
	"context"
	"math"
	"math/rand"

	"github.com/justnoise/parallel"
)

type TokenRangeProducer struct {
	numTokenRanges uint64
}

type tokenRange struct {
	start int64
	end   int64
}

// Breaks the token space up into numTokenRanges ranges and pushes them onto the work queue
func (p *TokenRangeProducer) Produce(ctx context.Context, workQueue parallel.WorkQueue) error {
	tokenRanges := make([]tokenRange, p.numTokenRanges)
	// The last range will be a little larger than the rest since we do integer division
	rangeSize := uint64(math.MaxInt64*2) / uint64(p.numTokenRanges)
	for i := uint64(0); i < uint64(p.numTokenRanges); i++ {
		start := math.MinInt64 + int64(i*rangeSize)
		end := start + int64(rangeSize-1)
		if i == p.numTokenRanges-1 {
			end = math.MaxInt64
		}
		item := tokenRange{
			start: start,
			end:   end,
		}
		tokenRanges[i] = item
	}
	// Shuffle the token ranges to distribute work across the cluster/shards
	for i := uint64(0); i < p.numTokenRanges; i++ {
		j := i + uint64(rand.Intn(int(p.numTokenRanges-i)))
		tokenRanges[i], tokenRanges[j] = tokenRanges[j], tokenRanges[i]
	}
	for _, item := range tokenRanges {
		err := workQueue.Push(ctx, item)
		if err != nil {
			return err
		}
	}
	return nil
}
