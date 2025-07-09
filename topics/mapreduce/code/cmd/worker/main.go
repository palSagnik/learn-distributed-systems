package main

import (
	"log"
	"mapreduce/internal/job/invertedindex"
	"mapreduce/internal/mapreduce"
	"sync"
)

func main() {

	const numWorkers = 1

	worker, err := mapreduce.NewWorker("localhost:1234", invertedindex.ProcessingTask)
	if err != nil {
		log.Fatal(err)
	}
	
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func ()  {
			defer wg.Done()
			worker.Run()
		}()
	}
	wg.Wait()
} 