package mapreduce

import (
	"fmt"
	"log"
	"net/rpc"
)

type Worker struct {
	client *rpc.Client
	mapFunc func(*MapTask) MapResult
}

func NewWorker(address string, mapFunc func(*MapTask) MapResult) (*Worker, error) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	
	return &Worker{client: client, mapFunc: mapFunc}, nil
}

func (w *Worker) Run() {
	for {
		var task MapTask
		err := w.client.Call("Master.RequestMapTask", struct{}{}, &task)
		if err != nil {
			log.Fatal("RequestMapTask failed:", err)
		}

		if task.TaskID == -1 {
			fmt.Println("No map tasks available; exiting")
			return
		}

		fmt.Printf("Received task: %v\n", task)

		// Processing Task
		result := w.mapFunc(&task)

		// Report back:
		err = w.client.Call("Master.ReportMapResult", result, &struct{}{})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Result sent back to master")
	}
} 