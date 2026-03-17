package api

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ScenarioResult reports metrics from a concurrent write scenario.
type ScenarioResult struct {
	TotalRecords int
	Workers      int
	ValueSize    int
	Duration     time.Duration
	Throughput   float64
}

// RunConcurrentWriteScenario writes totalRecords keys concurrently using workers goroutines.
// It is intended for stress/performance validation scenarios (e.g. 100k concurrent writes).
func RunConcurrentWriteScenario(db *DB, totalRecords, workers, valueSize int) (ScenarioResult, error) {
	if db == nil {
		return ScenarioResult{}, fmt.Errorf("db is nil")
	}
	if totalRecords <= 0 || workers <= 0 || valueSize <= 0 {
		return ScenarioResult{}, fmt.Errorf("invalid scenario configuration")
	}

	payload := make([]byte, valueSize)
	for i := range payload {
		payload[i] = byte((i * 31) % 251)
	}

	var counter atomic.Int64
	counter.Store(-1)

	start := time.Now()
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				id := int(counter.Add(1))
				if id >= totalRecords {
					return
				}
				key := fmt.Sprintf("scenario-100k-worker-%d-key-%d", workerID, id)
				if err := db.Put(key, payload); err != nil {
					if firstErr.Load() == nil {
						firstErr.Store(err)
					}
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return ScenarioResult{}, v.(error)
	}

	dur := time.Since(start)
	return ScenarioResult{
		TotalRecords: totalRecords,
		Workers:      workers,
		ValueSize:    valueSize,
		Duration:     dur,
		Throughput:   float64(totalRecords) / dur.Seconds(),
	}, nil
}
