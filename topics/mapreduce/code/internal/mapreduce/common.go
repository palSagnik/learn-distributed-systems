package mapreduce

import (
	"hash/fnv"
	"os"
	"path/filepath"
)

func hash(s string) uint32 {
	h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}

func discoverFiles(inputDir string) ([]MapTask, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, err
	}

	var tasks []MapTask
	taskID := 0
	for _, entry := range entries {
		
		// skip if isDir
		// TODO: implement recursive task assignment for directories also
		if entry.IsDir() {
			continue
		}

		// if file, assign it as a task
		path := filepath.Join(inputDir, entry.Name())
		tasks = append(tasks, MapTask{
			TaskID: taskID,
			Filename: path,
		})
		taskID++
	}

	return tasks, nil
} 