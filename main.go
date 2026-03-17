package main

import (
	"fmt"
	"log"
	"runtime"

	"aetherdb/aether/api"
	aethernet "aetherdb/aether/net"
)

func main() {
	db, err := api.Open("./aether.db", 256<<20)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	server := aethernet.NewServer(":9090", db)
	go func() {
		log.Println("AetherDB server listening on :9090")
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// Required stress scenario: 100k concurrent writes via goroutines.
	result, err := api.RunConcurrentWriteScenario(db, 100000, runtime.NumCPU()*8, 256)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.SyncAll(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("100k concurrent write scenario => records=%d workers=%d valueSize=%dB duration=%s throughput=%.2f rec/s\n",
		result.TotalRecords,
		result.Workers,
		result.ValueSize,
		result.Duration,
		result.Throughput,
	)

	select {}
}
