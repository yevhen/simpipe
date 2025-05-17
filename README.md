# simpipe

A simple, powerful pipeline library for Go that provides building blocks for concurrent data processing.

## Overview

Simpipe is a Go library that implements the pipeline pattern for parallel data processing. It provides a set of composable components to create efficient data processing workflows with support for:

- Parallel processing
- Item batching
- Time-based flushing
- Pipeline chaining and forking
- Thread-safe message passing
- Strong typing via Go generics

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
done := func(item string) { fmt.Println("Done:", item) }
action := func(item string) { fmt.Println("Processing:", item) }

block := &blocks.ActionBlock[string]{
    Input:       in,
    Done:        done,
    Parallelism: 2,
    Action:      action,
}
block.Run()

// Send items to the block
in <- "item1"
in <- "item2"
```

#### BatchBlock

Accumulates items from an input channel into batches based on either batch size or flush timeout.

```go
import (
    "simpipe/blocks"
    "time"
)

// Create and run a BatchBlock that collects items into batches of 10
// or flushes every 5 seconds, whichever comes first
in := make(chan string)
done := func(batch []string) { fmt.Println("Batch size:", len(batch)) }
batchSize := 10
flushTimeout := 5 * time.Second

block := &blocks.BatchBlock[string]{
    Input:        in,
    Done:         done,
    BatchSize:    batchSize,
    FlushTimeout: flushTimeout,
    buffer:       make([]string, 0, batchSize),
    timer:        time.NewTicker(flushTimeout),
}
block.Run()

// Send items to the block
in <- "item1"
in <- "item2"
// ...
```

The `BatchBlock` features:
- Collecting items into batches of specified size
- Time-based flushing of incomplete batches
- Automatic flushing when input channel is closed

#### BatchActionBlock

Combines BatchBlock and ActionBlock to collect items into batches and then process those batches with configurable parallelism.

```go
import (
    "simpipe/blocks"
    "time"
)

// Create and run a BatchActionBlock
in := make(chan string)
action := func(batch []string) { fmt.Println("Processing batch of size:", len(batch)) }
done := func(item string) { fmt.Println("Done processing:", item) }
batchSize := 10
flushTimeout := 5 * time.Second
parallelism := 2

block := &blocks.BatchActionBlock[string]{
    Input:        in,
    Done:         done,
    BatchSize:    batchSize,
    FlushTimeout: flushTimeout,
    Parallelism:  parallelism,
    Action:       action,
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
processor1 := routing.NewActionProcessor(1, func(message *Item) {
    message.Text += ".step1"
})

processor2 := routing.NewActionProcessor(1, func(message *Item) {
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

// Create processors for parallel execution
processorA := routing.Transform(1, func(message Item) func(*Item) {
    patchedText := message.Text + ".A"
    return func(patch *Item) {
        patch.Text = patchedText
    }
})

processorB := routing.Transform(1, func(message Item) func(*Item) {
    patchedText := message.Text + ".B"
    return func(patch *Item) {
        patch.Text = patchedText
    }
})

// Add a processor that will be called after the all forked processors are done
finalProcessor := routing.NewActionProcessor(1, func(message *Item) {
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
    errorProcessor := routing.NewActionProcessor(2, func(entry *LogEntry) {
        if entry.Level == "ERROR" {
            fmt.Printf("ERROR: %s\n", entry.Message)
            time.Sleep(100 * time.Millisecond) // Simulate processing
        }
    })
    
    warningProcessor := routing.NewActionProcessor(1, func(entry *LogEntry) {
        if entry.Level == "WARNING" {
            fmt.Printf("WARNING: %s\n", entry.Message)
            time.Sleep(50 * time.Millisecond) // Simulate processing
        }
    })
    
    // Add both processors as a fork for parallel processing
    pipeline.AddFork(errorProcessor, warningProcessor)
    
    // Add a final processor
    finalProcessor := routing.NewActionProcessor(1, func(entry *LogEntry) {
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
        // Need to send pointer to the log entry
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
