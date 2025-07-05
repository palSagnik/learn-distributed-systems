package main

import (
	"encoding/json"
	"fmt"
	"log"
	"mapreduce/mr"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

// Master represents the coordinator in a MapReduce system that manages task distribution.
// It maintains a thread-safe queue of map tasks and tracks the next task to be assigned.
// The mutex ensures concurrent access safety when multiple workers request tasks.
type Master struct {
	mu             sync.Mutex
	tasks          []mr.MapTask
	completedTasks map[int]bool
	nextTask       int
	invertedIndex  map[string]map[string]struct{}
}

func NewMaster() *Master {
	return &Master{
		tasks:         []mr.MapTask{},
		completedTasks: make(map[int]bool),
		nextTask:      0,
		invertedIndex: make(map[string]map[string]struct{}),
	}
}

// Task Assignment RPC
// passing an empty struct to fulfill net/rpc method signature
// func (t *T) MethodName(args ArgsType, reply *ReplyType) error
func (m *Master) RequestMapTask(_ struct{}, reply *mr.MapTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// check if next task is out of bounds
	if m.nextTask >= len(m.tasks) {
		reply.TaskID = -1 // no more work
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

	m.mu.Lock()
	defer m.mu.Unlock()

	// mark task as completed
	m.completedTasks[res.TaskID] = true

	for _, kv := range res.Pairs {
		word := kv.Key
		filename := kv.Value

		if m.invertedIndex[word] == nil {
			m.invertedIndex[word] = make(map[string]struct{})
		}
		m.invertedIndex[word][filename] = struct{}{}
	}
	
	if m.allTasksCompleted() {
		fmt.Println("All tasks complete. Writing inverted index...")
        m.writeInvertedIndex()
	}

	return nil
}

func (m *Master) writeInvertedIndex() {
	outFile, err := os.Create("index.json")
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	flatIndex := make(map[string][]string)
	for word, fileset := range m.invertedIndex {
		for file := range fileset {
			flatIndex[word] = append(flatIndex[word], file)
		}
		sort.Strings(flatIndex[word])
	}

	encoder := json.NewEncoder(outFile)
	encoder.SetIndent("", "  ")
    if err := encoder.Encode(flatIndex); err != nil {
        log.Fatalf("Failed to write JSON: %v", err)
    }

    fmt.Println("Inverted index written to index.json")

}

func (m *Master) allTasksCompleted() bool {
	return len(m.completedTasks) == len(m.tasks)
}

// RPC Server Loop
func (m *Master) Run() {
	rpc.Register(m)
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Master is listening on port 1234")
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func main() {
	m := NewMaster()
	m.tasks = []mr.MapTask{
		{TaskID: 0, Filename: "input/input1.txt"},
		{TaskID: 1, Filename: "input/input2.txt"},
		{TaskID: 2, Filename: "input/input3.txt"},
	}
	m.Run()
}
