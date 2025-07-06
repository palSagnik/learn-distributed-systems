package main

import (
	"log"
	"mapreduce/internal/mapreduce"
)

func main() {
	m := mapreduce.NewMaster()

	err := m.LoadTasks("input")
	if err != nil {
		log.Fatal(err)
	}
	m.Run()
} 