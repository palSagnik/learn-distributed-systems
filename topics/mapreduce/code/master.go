package main

import (
	"fmt"
	"log"
	"mapreduce/mr"
	"net"
	"net/rpc"
	"sync"
)

// Master represents the coordinator in a MapReduce system that manages task distribution.
// It maintains a thread-safe queue of map tasks and tracks the next task to be assigned.
// The mutex ensures concurrent access safety when multiple workers request tasks.
type Master struct {
	mu sync.Mutex
	tasks []mr.MapTask
	nextTask int
}

// Task Assignment RPC
// passing an empty struct to fulfill net/rpc method signature
// func (t *T) MethodName(args ArgsType, reply *ReplyType) error
func (m *Master) RequestMapTask(_ struct{}, reply *mr.MapTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// check if next task is out of bounds
	if m.nextTask >= len(m.tasks) {
		reply.TaskID = -1 	// no more work
		return nil
	}

	// assign task
	// replacing the reply pointer with the pointer of the actual task
	*reply = m.tasks[m.nextTask]
	m.nextTask++
	return nil
}

func (m *Master) ReportMapResult(res mr.MapResult, _ *struct{}) error {
	fmt.Printf("Master received result for task %d with %d pairs\n", res.TaskID, len(res.Pairs))
	return nil
}

// RPC Server Loop
func (m *Master) Run() {
	rpc.Register(m)
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal(err)
	}

	for {
        conn, err := l.Accept()
        if err != nil {
            continue
        }
        go rpc.ServeConn(conn)
    }
}

func main() {
    m := &Master{
        tasks: []mr.MapTask{
            {TaskID: 0, Filename: "input1.txt"},
            {TaskID: 1, Filename: "input2.txt"},
			{TaskID: 2, Filename: "input3.txt"},
        },
    }
    m.Run()
}