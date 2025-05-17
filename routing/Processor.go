package routing

import (
	"simpipe/blocks"
	"sync"
)

type Message[T any] struct {
	Mutex   *sync.Mutex
	Payload *T
	Ack     func(payload *T, mutex *sync.Mutex)
}

type Processor[T any] interface {
	Send(message Message[T])
}

type ProcessorCompletion[T any] struct {
	payload *T
	mutex   *sync.Mutex
}

type ActionProcessor[T any] struct {
	in    chan Message[T]
	block *blocks.ActionBlock[Message[T]]
}

func (p *ActionProcessor[T]) Send(message Message[T]) {
	p.in <- message
}

func NewActionProcessor[T any](parallelism int, action func(message *T)) *ActionProcessor[T] {
	return Transform[T](parallelism, func(message T) func(message *T) {
		return func(message *T) {
			action(message)
		}
	})
}

func Transform[T any](parallelism int, action func(message T) func(*T)) *ActionProcessor[T] {
	processor := &ActionProcessor[T]{}

	processor.in = make(chan Message[T])
	processor.block = &blocks.ActionBlock[Message[T]]{
		Input: processor.in,
		Done: func(message Message[T]) {
			message.Ack(message.Payload, message.Mutex)
		},
		Parallelism: parallelism,
		Action: func(message Message[T]) {
			patch := action(*message.Payload)
			message.Mutex.Lock()
			defer message.Mutex.Unlock()
			patch(message.Payload)
		},
	}

	processor.block.Run()

	return processor
}
