package invertedindex

import (
	"encoding/json"
	"fmt"
	"log"
	"mapreduce/internal/mapreduce"
	"os"
	"sort"
)

// Reduce worker task
func ReduceWorker(task mapreduce.ReduceTask) {
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