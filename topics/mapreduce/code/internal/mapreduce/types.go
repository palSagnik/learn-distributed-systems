package mapreduce

import "time"

// enum for task states
type TaskStatus int

const (
	Idle TaskStatus = iota
	Active
	Completed
)

// Helps in building fault-tolerance logic based on Status as well as Start
type TaskMeta struct {
	Task   MapTask
	Status TaskStatus
	Start  time.Time
}

// MapTask defines a single Map Job
type MapTask struct {
	TaskID   int
	Filename string
}

// mapResult is sent back by the worker
// TaskID here acts as a trackingID for the work assigned
type MapResult struct {
	TaskID int
	Pairs  []KeyValue
}

type KeyValue struct {
	Key   string
	Value string
}

type ReduceTask struct {
	PartitionID int
	Data        map[string][]string
}
