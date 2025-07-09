package mapreduce

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
)

func hash(s string) uint32 {
	h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
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

// creating taskmetas
func discoverFiles(inputDir string) ([]TaskMeta, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, err
	}

	var taskmetas []TaskMeta
	taskID := 0
	for _, entry := range entries {
		
		// skip if isDir
		// TODO: implement recursive task assignment for directories also
		if entry.IsDir() {
			continue
		}

		// if file, assign it as a task
		path := filepath.Join(inputDir, entry.Name())
		taskmetas = append(taskmetas, TaskMeta{
			Task: MapTask{
				TaskID: taskID,
				Filename: path,
			},
			Status: Idle,
		})
		taskID++
	}

	return taskmetas, nil
} 