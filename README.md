# simpipe

A simple, powerful pipeline library for Go that provides building blocks for concurrent data processing.

## Overview

Simpipe is a Go library that implements the pipeline pattern for parallel data processing. It provides a set of composable components to create efficient data processing workflows with support for:

- Parallel processing
- Item batching
- Time-based flushing
- Pipeline chaining
- Filtering
- Strong typing via Go generics

## Design Principles

1. **Simplicity**: Each component has a single responsibility and is easy to understand
2. **Composability**: Components can be combined to create complex processing pipelines
3. **Type Safety**: Using Go generics for compile-time type checking
4. **Concurrency**: Built-in support for parallel processing with controllable parallelism
5. **Efficiency**: Batching support for operations that benefit from processing multiple items at once
   
## Installation

```bash
go get github.com/yourusername/simpipe
```

## Core Components

### ActionBlock

Executes an action on each item received from an input channel with configurable parallelism.

```go
// Create and run an ActionBlock that processes items with a parallelism of 2
in := make(chan string)
done := func(item string) { fmt.Println("Done processing:", item) }
action := func(item string) { fmt.Println("Processing:", item) }

RunActionBlock(in, done, 2, action)

// Send items to the block
in <- "item1"
in <- "item2"
```

### BatchBlock

Accumulates items from an input channel into batches based on either batch size or flush timeout.

```go
// Create and run a BatchBlock that collects items into batches of 10 
// or flushes every 5 seconds, whichever comes first
in := make(chan string)
batchSize := 10
flushTimeout := 5 * time.Second
done := func(batch []string) { fmt.Println("Batch size:", len(batch)) }

RunBatchBlock(in, batchSize, flushTimeout, done)

// Send items to the block
in <- "item1"
in <- "item2"
// ...
```

### BatchActionBlock

Combines BatchBlock and ActionBlock to collect items into batches and then process those batches with configurable parallelism.

```go
// Create and run a BatchActionBlock
in := make(chan string)
done := func(item string) { fmt.Println("Done processing:", item) }
batchSize := 10
flushTimeout := 5 * time.Second
parallelism := 2
action := func(batch []string) { fmt.Println("Processing batch of size:", len(batch)) }

RunBatchActionBlock(in, done, batchSize, flushTimeout, parallelism, action)

// Send items to the block
in <- "item1"
in <- "item2"
// ...
```

### Pipes

Implements a pipeline pattern where data flows through a series of processing steps with support for filtering and chaining.

## Complete Pipeline Example

Here's an example that demonstrates a complete data processing pipeline using the simpipe library:

```go
package main

import (
	"fmt"
	"simpipe"
	"time"
)

type LogEntry struct {
	Level   string
	Message string
}

func main() {
	// Create a pipeline for processing log entries
	
	// First stage: filter and process error logs
	errorFilter := func(entry LogEntry) bool {
		return entry.Level == "ERROR"
	}
	
	// Second stage: process warning logs
	warningPipe := createWarningPipe()
	
	nextStage := func(entry LogEntry) simpipe.Pipe[LogEntry] {
		if entry.Level == "WARNING" {
			return warningPipe
		}
		return nil
	}
	
	// Define action for processing error logs
	processError := func(entry LogEntry) {
		fmt.Printf("Processing ERROR: %s\n", entry.Message)
		time.Sleep(100 * time.Millisecond) // Simulate processing time
	}
	
	// Create and run the error log pipeline
	errorPipe := simpipe.CreateActionPipe[LogEntry](
		10,           // channel capacity
		3,            // parallelism
		processError, // action function
		errorFilter,  // filter function
		nextStage,    // next stage function
	)
	errorPipe.Run()
	
	// Feed log entries into the pipeline
	logs := []LogEntry{
		{Level: "INFO", Message: "Application started"},
		{Level: "WARNING", Message: "Disk space low"},
		{Level: "ERROR", Message: "Connection failed"},
		{Level: "INFO", Message: "User logged in"},
		{Level: "WARNING", Message: "Slow response time"},
		{Level: "ERROR", Message: "Database timeout"},
	}
	
	for _, log := range logs {
		errorPipe.Send(log)
	}
	
	// Wait to allow processing to complete
	time.Sleep(1 * time.Second)
	errorPipe.Close()
	warningPipe.Close()
}

func createWarningPipe() *simpipe.ActionPipe[LogEntry] {
	processWarning := func(entry LogEntry) {
		fmt.Printf("Processing WARNING: %s\n", entry.Message)
		time.Sleep(50 * time.Millisecond) // Simulate processing time
	}
	
	warningFilter := func(entry LogEntry) bool {
		return true // Process all warnings that reach this pipe
	}
	
	return simpipe.CreateActionPipe[LogEntry](
		5,              // channel capacity
		2,              // parallelism
		processWarning, // action function
		warningFilter,  // filter function
		nil,            // no next stage
	)
}
```

## Batch Processing Example

This example demonstrates how to use BatchActionPipe for processing items in batches:

```go
package main

import (
	"fmt"
	"simpipe"
	"time"
)

type DataPoint struct {
	ID    int
	Value float64
}

func main() {
	// Create a batch processing pipeline
	processBatch := func(batch []DataPoint) {
		fmt.Printf("Processing batch of %d items\n", len(batch))
		// Simulate batch processing
		time.Sleep(200 * time.Millisecond)
		
		for _, point := range batch {
			fmt.Printf("  - Processed point %d: %.2f\n", point.ID, point.Value)
		}
	}
	
	// Filter to process all items
	filter := func(point DataPoint) bool {
		return true
	}
	
	// Create and run the batch processing pipeline
	batchPipe := simpipe.CreateBatchActionPipe[DataPoint](
		20,          // channel capacity
		2,           // parallelism
		5,           // batch size
		processBatch, // batch action function
		filter,      // filter function
		nil,         // no next stage
	)
	batchPipe.Run()
	
	// Generate and send data points
	for i := 0; i < 20; i++ {
		point := DataPoint{
			ID:    i,
			Value: float64(i) * 1.5,
		}
		batchPipe.Send(point)
	}
	
	// Wait to allow processing to complete
	time.Sleep(2 * time.Second)
	batchPipe.Close()
}
```

## Advanced Usage: Multi-Stage Pipeline

Here's an example of a more complex pipeline with multiple stages and different processing patterns:

```go
package main

import (
	"fmt"
	"simpipe"
	"strings"
	"time"
)

type Document struct {
	ID      string
	Content string
	Tags    []string
}

func main() {
	// Create a document processing pipeline with multiple stages
	
	// Stage 3: Archive processor (final stage for all documents)
	archiveFilter := func(doc Document) bool {
		return true // Archive all documents
	}
	
	archiveAction := func(docs []Document) {
		fmt.Printf("Archiving batch of %d documents\n", len(docs))
		time.Sleep(100 * time.Millisecond)
	}
	
	archivePipe := simpipe.CreateBatchActionPipe[Document](
		30,            // capacity
		1,             // parallelism
		10,            // batch size
		archiveAction, // batch action
		archiveFilter, // filter
		nil,           // no next stage
	)
	archivePipe.Run()
	
	// Stage 2: Analytics processor
	analyticsFilter := func(doc Document) bool {
		return true // Process all documents for analytics
	}
	
	analyticsAction := func(doc Document) {
		fmt.Printf("Analyzing document: %s\n", doc.ID)
		time.Sleep(50 * time.Millisecond)
	}
	
	analyticsNext := func(doc Document) simpipe.Pipe[Document] {
		return archivePipe // Send to archive after analytics
	}
	
	analyticsPipe := simpipe.CreateActionPipe[Document](
		20,              // capacity
		3,               // parallelism
		analyticsAction, // action
		analyticsFilter, // filter
		analyticsNext,   // next stage
	)
	analyticsPipe.Run()
	
	// Stage 1: Content processor (entry point)
	contentFilter := func(doc Document) bool {
		// Only process documents with non-empty content
		return len(doc.Content) > 0
	}
	
	contentAction := func(doc Document) {
		fmt.Printf("Processing content for document: %s\n", doc.ID)
		// Simulate content processing
		doc.Content = strings.ToUpper(doc.Content)
		time.Sleep(100 * time.Millisecond)
	}
	
	contentNext := func(doc Document) simpipe.Pipe[Document] {
		return analyticsPipe // Send to analytics after content processing
	}
	
	contentPipe := simpipe.CreateActionPipe[Document](
		10,             // capacity
		2,              // parallelism
		contentAction,  // action
		contentFilter,  // filter
		contentNext,    // next stage
	)
	contentPipe.Run()
	
	// Feed documents into the pipeline
	docs := []Document{
		{ID: "doc1", Content: "Hello world", Tags: []string{"greeting"}},
		{ID: "doc2", Content: "Important notice", Tags: []string{"notice", "important"}},
		{ID: "doc3", Content: "", Tags: []string{"empty"}},
		{ID: "doc4", Content: "Technical documentation", Tags: []string{"technical", "docs"}},
	}
	
	for _, doc := range docs {
		contentPipe.Send(doc)
	}
	
	// Wait to allow processing to complete
	time.Sleep(2 * time.Second)
	
	// Close all pipes (in reverse order to avoid sending to closed channels)
	contentPipe.Close()
	analyticsPipe.Close()
	archivePipe.Close()
}
```

## License

MIT
