package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"mapreduce/mr"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// initialise partitions
const nReducers = 5

// Master represents the coordinator in a MapReduce system that manages task distribution.
type Master struct {
	mu             sync.Mutex
	tasks          []mr.MapTask
	completedTasks map[int]bool
	nextTask       int
	invertedIndex  map[string]map[string]struct{}
	mapPhaseOutput map[string][]string
}

func NewMaster() *Master {
	return &Master{
		tasks:         []mr.MapTask{},
		completedTasks: make(map[int]bool),
		nextTask:      0,
		invertedIndex: make(map[string]map[string]struct{}),
		mapPhaseOutput: make(map[string][]string),
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
	
	// grouping map result pairs
	for _, kv := range res.Pairs {
		word := kv.Key
		filename := kv.Value
		m.mapPhaseOutput[word] = append(m.mapPhaseOutput[word], filename)
	}

	if m.allTasksCompleted() {
		fmt.Println("All tasks complete. Partitioning...")

		// partitioning
		partitions := make([]map[string][]string, nReducers)
		for i := range partitions {
			partitions[i] = make(map[string][]string)
		}

		for word, filenames := range m.mapPhaseOutput {
			partitionID := int(hash(word)) % nReducers
			partitions[partitionID][word] = filenames
		}

		// allocation of tasks to reducers
		var reduceTasks []mr.ReduceTask
		for i := 0; i < nReducers; i++ {
			reduceTask := mr.ReduceTask{
				PartitionID: i,
				Data: partitions[i],
			}

			reduceTasks = append(reduceTasks, reduceTask)
		}

		var wg sync.WaitGroup
		for _, task := range reduceTasks {
			wg.Add(1)
			go func(t mr.ReduceTask) {
				defer wg.Done()
				reduceWorker(t)
			}(task)
		}
		wg.Wait()
	}

	return nil
}

// Reduce worker task
func reduceWorker(task mr.ReduceTask) {
	output := make(map[string][]string)

	for word, files := range task.Data {
		// dedup fileList
		uniqueList := make(map[string]struct{})
		for _, f := range files {
			uniqueList[f] = struct{}{}
		}

		// sort for determinism
		var deduped []string
		for f := range uniqueList {
			deduped = append(deduped, f)
		}
		sort.Strings(deduped)
		
		// output[word] = sorted-dedup-filelist
		output[word] = deduped
	}

	// write output to part-reduce-{task.PartitionID}
	file, err := os.Create(fmt.Sprintf("part-reduce-%d.json", task.PartitionID))
	if err != nil {
		log.Fatalf("Failed to create reduce-worker file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", " ")
	if err := encoder.Encode(output); err != nil {
		log.Fatalf("Failed to write reduce-worker file: %v", err)
	}
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

func discoverFiles(inputDir string) ([]mr.MapTask, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, err
	}

	var tasks []mr.MapTask
	taskID := 0
	for _, entry := range entries {
		
		// skip if isDir
		// TODO: implement recursive task assignment for directories also
		if entry.IsDir() {
			continue
		}

		// if file, assign it as a task
		path := filepath.Join(inputDir, entry.Name())
		tasks = append(tasks, mr.MapTask{
			TaskID: taskID,
			Filename: path,
		})
		taskID++
	}

	return tasks, nil
}

func main() {
	m := NewMaster()

	var err error
	m.tasks, err = discoverFiles("input")
	if err != nil {
		log.Fatal(err)
	}
	m.Run()
}


func hash(s string) uint32 {
	h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
} 
