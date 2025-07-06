package mr

// defining a map task
// MapTask defines a single Map Job
type MapTask struct {
	TaskID   int
	Filename string
}

// mapResult is sent bacy by the worker
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
	Data map[string][]string
}