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

	for 
	{
		var task mr.MapTask
		err = client.Call("Master.RequestMapTask", struct{}{}, &task)
		if err != nil {
			log.Fatal("RequestMapTask failed:", err)
		}

		if task.TaskID == -1 {
			fmt.Println("No map tasks available; exiting")
			return
		}

		fmt.Printf("Received task: %v\n", task)

		// Simulate processing
		pairs := []mr.KeyValue{
			{Key: "hello", Value: "1"},
			{Key: "world", Value: "1"},
		}

		result := mr.MapResult{
			TaskID: task.TaskID,
			Pairs:  pairs,
		}

		// Report back:
		err = client.Call("Master.ReportMapResult", result, &struct{}{})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Result sent back to master")
	}
}