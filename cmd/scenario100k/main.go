package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"

	"aetherdb/aether/api"
)

func main() {
	var (
		dbPath  = flag.String("db", "./aether_scenario_100k.db", "path to database file")
		total   = flag.Int("total", 100000, "total record count")
		workers = flag.Int("workers", runtime.NumCPU()*8, "number of concurrent goroutines")
		size    = flag.Int("value-size", 256, "value size in bytes")
	)
	flag.Parse()

	db, err := api.Open(*dbPath, 256<<20)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	result, err := api.RunConcurrentWriteScenario(db, *total, *workers, *size)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.SyncAll(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("scenario complete: records=%d workers=%d valueSize=%dB duration=%s throughput=%.2f rec/s\n",
		result.TotalRecords,
		result.Workers,
		result.ValueSize,
		result.Duration,
		result.Throughput,
	)
}
