package invertedindex

import (
	"log"
	"mapreduce/internal/mapreduce"
	"os"
	"regexp"
	"strings"
)

func ProcessingTask(task *mapreduce.MapTask) mapreduce.MapResult {
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		log.Fatalf("Failed to read file: %s", task.Filename)
	}
	
	var pairs []mapreduce.KeyValue
	wordRegex := regexp.MustCompile(`[a-zA-Z]+|\d{4}`)
	words := wordRegex.FindAllString(string(content), -1)
	for _, word := range words {
		word = strings.ToLower(word)
		
		// emit
		pairs = append(pairs, mapreduce.KeyValue{Key: word, Value: task.Filename})
	}
	
	result := mapreduce.MapResult{
		TaskID: task.TaskID,
		Pairs:  pairs,
	}

	return result
} 