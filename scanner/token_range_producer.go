package scanner

import (
	"context"
	"math"

	"github.com/justnoise/parallel"
)

type TokenRangeProducer struct {
	numTokenRanges uint64
}

type tokenRange struct {
	start int64
	end   int64
}

func (p *TokenRangeProducer) Produce(ctx context.Context, workQueue parallel.WorkQueue) error {
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
		err := workQueue.Push(ctx, item)
		if err != nil {
			return err
		}
	}
	return nil
}
