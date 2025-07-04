package main

import (
	"fmt"
	"log"
	"mapreduce/mr"

	"net/rpc"
)

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal(err)
	}

	var task mr.MapTask
	err = client.Call("Master.RequestMapTask", struct{}{}, &task)
	if task.TaskID == -1 {
		fmt.Println("No map tasks available; exiting")
        return
	}

	// Simulate map work:
    result := mr.MapResult{TaskID: task.TaskID}
    // e.g., read file, emit key/value pairs
    result.Pairs = append(result.Pairs, mr.KeyValue{Key: "example", Value: "1"})

    // Report back:
    err = client.Call("Master.ReportMapResult", result, &struct{}{})
    if err != nil {
        log.Fatal(err)
    }
}