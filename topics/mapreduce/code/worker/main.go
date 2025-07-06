package main

import (
	"fmt"
	"log"
	"mapreduce/mr"
	"os"
	"regexp"
	"strings"

	"net/rpc"
)

func processingTask(task *mr.MapTask) mr.MapResult {
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		log.Fatalf("Failed to read file: %s", task.Filename)
	}
	
	var pairs []mr.KeyValue
	wordRegex := regexp.MustCompile(`[a-zA-Z]+|\d{4}`)
	words := wordRegex.FindAllString(string(content), -1)
	for _, word := range words {
		word = strings.ToLower(word)
		
		// emit
		pairs = append(pairs, mr.KeyValue{Key: word, Value: task.Filename})
	}
	
	result := mr.MapResult{
		TaskID: task.TaskID,
		Pairs:  pairs,
	}

	return result
}



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

		// Processing Task
		result := processingTask(&task)

		// Report back:
		err = client.Call("Master.ReportMapResult", result, &struct{}{})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Result sent back to master")
	}
}