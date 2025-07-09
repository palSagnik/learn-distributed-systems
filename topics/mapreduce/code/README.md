# MapReduce Framework

This project implements a distributed MapReduce framework in Go with an inverted index job as an example.

## Directory Structure

```
code/
├── cmd/
│   ├── master/           ← entrypoint for master process
│   │   └── main.go
│   └── worker/           ← entrypoint for worker process
│       └── main.go
├── internal/
│   ├── mapreduce/        ← core framework logic (generic MapReduce engine)
│   │   ├── master.go
│   │   ├── worker.go
│   │   ├── common.go
│   │   └── types.go      ← shared data structures
│   └── job/              ← job-specific logic (inverted index, word count, etc)
│       ├── invertedindex/
│       │   ├── map.go
│       │   └── reduce.go
├── input/                ← test data files
├── output/               ← final outputs (index.json, logs, etc)
├── go.mod
└── README.md
```

## Usage


### Run

```bash
# Start master (in one terminal)
go run ./cmd/master

# Start worker (in another terminal)
go run ./cmd/worker
```

## Architecture

- **cmd/**: Thin CLI wrappers that handle command-line arguments and configuration
- **internal/mapreduce/**: Core MapReduce framework with Master/Worker coordination
- **internal/job/**: Job-specific implementations (map and reduce functions)
- **input/**: Directory containing input files for processing
- **output/**: Directory where final results are written
