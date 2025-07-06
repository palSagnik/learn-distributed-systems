package main

import (
	"log"
	"mapreduce/internal/job/invertedindex"
	"mapreduce/internal/mapreduce"
)

func main() {
	worker, err := mapreduce.NewWorker("localhost:1234", invertedindex.ProcessingTask)
	if err != nil {
		log.Fatal(err)
	}
	
	worker.Run()
} 