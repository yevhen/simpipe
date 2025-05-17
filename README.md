# simpipe

A simple, powerful pipeline library for Go that provides building blocks for concurrent data processing.

## Overview

Simpipe is a Go library that implements the pipeline pattern for high-performant data processing. It provides a set of composable components to create efficient data processing workflows with support for:

- Low-level concurrency primitives 
- Item batching with size and timeout-based flushing
- Processor chaining with conditional routing and filtering
- Support for concurrent processor execution

## Installation

```bash
go get github.com/yevhen/simpipe
```

## Core Components

The library is organized into two main packages:

1. `blocks` - Low-level building blocks for concurrent processing
2. `routing` - Higher-level pipeline components for message routing and orchestration

### Blocks Package

#### ActionBlock

Executes an action on each item received from an input channel with configurable parallelism.

```go
import "simpipe/blocks"

// Create and run an ActionBlock that processes items with parallelism of 2
in := make(chan string)
block := &blocks.ActionBlock[string]{
    Input:       in,
    Parallelism: 2,
    Action:      func(item string) { fmt.Println("Processing:", item) },
    Done:        func(item string) { fmt.Println("Done:", item) },
}
block.Run()

// Send items to the block
in <- "item1"
in <- "item2"
```

#### BatchActionBlock

Combines BatchBlock and ActionBlock to collect items into batches and then process those batches with configurable parallelism.

```go
import (
    "simpipe/blocks"
    "time"
)

// Create and run a BatchActionBlock
in := make(chan string)
block := &blocks.BatchActionBlock[string]{
    Input:        in,
    BatchSize:    10,
    FlushTimeout:  5 * time.Second,
    Parallelism:  2,
    Action:       func(batch []string) { fmt.Println("Processing batch of size:", len(batch)) },
    Done:         func(batch []string) { fmt.Println("Done batch") },	
}
block.Run()

// Send items to the block
in <- "item1"
in <- "item2"
// ...
```

### Routing Package

The routing package provides higher-level components for building complex processing pipelines.

#### Pipeline

A pipeline orchestrates the flow of messages through a series of processors.

```go
import (
    "simpipe/routing"
    "fmt"
)

type Item struct {
    Text string
}

// Create a new pipeline
var completed *Item
pipeline := routing.NewPipeline(func(message *Item) {
    completed = message
    fmt.Println("Pipeline completed with:", message.Text)
})

// Add processors to the pipeline
processor1 := routing.Action(1, func(message *Item) {
    message.Text += ".step1"
})

processor2 := routing.Action(1, func(message *Item) {
    message.Text += ".step2"
})

pipeline.AddProcessor(processor1)
pipeline.AddProcessor(processor2)

// Send a message through the pipeline
message := &Item{Text: "start"}
pipeline.Send(message)
```

#### Forking processing

Pipelines support parallel processing paths using forks:

```go
// Create a new pipeline
pipeline := routing.NewPipeline(func(message *Item) {
    fmt.Println("Pipeline completed with:", message.Text)
})

// Create special patch processors for tread-safe parallel execution
processorA := routing.Patch(1, func(message Item) func(*Item) {
    patchedText := message.Text + ".A"
    return func(patch *Item) {
        patch.Text = patchedText
    }
})

processorB := routing.Patch(1, func(message Item) func(*Item) {
    patchedText := message.Text + ".B"
    return func(patch *Item) {
        patch.Text = patchedText
    }
})

// Add a processor that will be called after the all forked processors are done
finalProcessor := routing.Action(1, func(message *Item) {
    message.Text += ".final"
})

// Add a fork to process in parallel
pipeline.AddFork(processorA, processorB)
pipeline.AddProcessor(finalProcessor)

// Send a message
pipeline.Send(&Item{Text: "start"})
// Result will be: "start.A.B.final" or "start.B.A.final"
```

## Complete Pipeline Example

Here's an example that demonstrates a complete data processing pipeline:

```go
package main

import (
    "fmt"
    "simpipe/routing"
    "sync"
    "time"
)

type LogEntry struct {
    Level   string
    Message string
}

func main() {
    var wg sync.WaitGroup
    
    // Create pipeline with completion handler
    pipeline := routing.NewPipeline(func(entry *LogEntry) {
        fmt.Printf("Completed processing: %s\n", entry.Message)
        wg.Done()
    })
    
    // Create processors
    errorProcessor := routing.Action(2, func(entry *LogEntry) {
        if entry.Level == "ERROR" {
            fmt.Printf("ERROR: %s\n", entry.Message)
            time.Sleep(100 * time.Millisecond) // Simulate processing
        }
    })
    
    warningProcessor := routing.Action(1, func(entry *LogEntry) {
        if entry.Level == "WARNING" {
            fmt.Printf("WARNING: %s\n", entry.Message)
            time.Sleep(50 * time.Millisecond) // Simulate processing
        }
    })
    
    // Add both processors as a fork for parallel processing
    pipeline.AddFork(errorProcessor, warningProcessor)
    
    // Add a final processor
    finalProcessor := routing.Action(1, func(entry *LogEntry) {
        entry.Message += " [PROCESSED]"
    })
    pipeline.AddProcessor(finalProcessor)
    
    // Process log entries
    logs := []LogEntry{
        {Level: "INFO", Message: "Application started"},
        {Level: "WARNING", Message: "Disk space low"},
        {Level: "ERROR", Message: "Connection failed"},
    }
    
    wg.Add(len(logs))
    for i := range logs {
        // Need to send a pointer to the log entry
        pipeline.Send(&logs[i])
    }
    
    // Wait for all processing to complete
    wg.Wait()
    
    // Print final results
    for _, log := range logs {
        fmt.Printf("%s: %s\n", log.Level, log.Message)
    }
}
```

## Design Principles

1. **Simplicity**: Each component has a single responsibility and is easy to understand
2. **Composability**: Components can be combined to create complex processing pipelines
3. **Type Safety**: Using Go generics for compile-time type checking
4. **Concurrency**: Built-in support for parallel processing with controllable parallelism
5. **Thread Safety**: Proper synchronization for message state changes in pipelines
6. **Efficiency**: Batching support for operations that benefit from processing multiple items at once

## License

MIT
