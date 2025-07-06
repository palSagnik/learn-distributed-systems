package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
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
	tasks          []MapTask
	completedTasks map[int]bool
	nextTask       int
	invertedIndex  map[string]map[string]struct{}
	mapPhaseOutput map[string][]string
}

func NewMaster() *Master {
	return &Master{
		tasks:         []MapTask{},
		completedTasks: make(map[int]bool),
		nextTask:      0,
		invertedIndex: make(map[string]map[string]struct{}),
		mapPhaseOutput: make(map[string][]string),
	}
}

// Task Assignment RPC
// passing an empty struct to fulfill net/rpc method signature
// func (t *T) MethodName(args ArgsType, reply *ReplyType) error
func (m *Master) RequestMapTask(_ struct{}, reply *MapTask) error {
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

func (m *Master) ReportMapResult(res MapResult, _ *struct{}) error {
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
		var reduceTasks []ReduceTask
		for i := 0; i < nReducers; i++ {
			reduceTask := ReduceTask{
				PartitionID: i,
				Data: partitions[i],
			}

			reduceTasks = append(reduceTasks, reduceTask)
		}

		fmt.Println("work assigned to reducers")
		var wg sync.WaitGroup
		for _, task := range reduceTasks {
			wg.Add(1)
			go func(t ReduceTask) {
				defer wg.Done()
				reduceWorker(t)
			}(task)
		}
		wg.Wait()
	}

	fmt.Println("writing to index.json and cleaning up")
	// write to index
	m.writeInvertedIndex()

	// cleanup
	if err := cleanupReduceFiles(); err != nil {
		return  err
	}

	return nil
}

// Reduce worker task
func reduceWorker(task ReduceTask) {
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
	files, err := filepath.Glob("part-reduce-*.json")
	if err != nil {
		log.Fatalf("Failed to glob reduce-worker files: %v", err)
	}
	finalIndex := make(map[string][]string)

	for _, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			log.Fatalf("Failed to open file %s: %v", fname, err)
		}
		defer f.Close()

		var partial map[string][]string
		json.NewDecoder(f).Decode(&partial)

		for word, files := range partial {
			finalIndex[word] = files
		}
	}

	// writing to index
	file, err := os.Create("output/index.json")
	if err != nil {
		log.Fatalf("Failed to create index.json: %v", err)
	}

	encode := json.NewEncoder(file)
	encode.SetIndent("", " ")
	if err := encode.Encode(finalIndex); err != nil {
		log.Fatal("Failed to write to index.json")
	}
}

// clean-up
func cleanupReduceFiles() error {
    files, err := filepath.Glob("part-reduce-*.json")
    if err != nil {
        return fmt.Errorf("glob failed: %v", err)
    }

    for _, file := range files {
        err := os.Remove(file)
        if err != nil {
            log.Printf("Failed to delete %s: %v", file, err)
        } else {
            fmt.Printf("Deleted: %s\n", file)
        }
    }

    return nil
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

func (m *Master) LoadTasks(inputDir string) error {
	var err error
	m.tasks, err = discoverFiles(inputDir)
	return err
} 