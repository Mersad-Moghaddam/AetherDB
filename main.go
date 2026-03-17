package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"aetherdb/aether/api"
	aethernet "aetherdb/aether/net"
)

func main() {
	db, err := api.Open("./aether.db", 128<<20)
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

	// In-process concurrency demo using lock-free API calls.
	var wg sync.WaitGroup
	workers := 64
	opPerWorker := 2000
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for i := 0; i < opPerWorker; i++ {
				k := fmt.Sprintf("key-%d-%d", id, r.Intn(4000))
				v := []byte(fmt.Sprintf("value-from-%d-%d", id, i))
				_ = db.Put(k, v)
				_, _, _ = db.Get(k)
			}
		}(w)
	}
	wg.Wait()
	_ = db.SyncAll()
	fmt.Printf("completed %d operations in %s\n", workers*opPerWorker*2, time.Since(start))

	select {}
}
