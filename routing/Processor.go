package routing

import "simpipe/blocks"

type Message[T any] struct {
	Payload *T
	Ack     func(processor *Processor[T], payload *T)
}

type Processor[T any] struct {
	in    chan Message[T]
	block *blocks.ActionBlock[Message[T]]
}

func (n *Processor[T]) Run() {
	n.block.Run()
}

func (n *Processor[T]) Send(message Message[T]) {
	n.in <- message
}

func (n *Processor[T]) Close() {
	close(n.in)
}

func NewActionProcessor[T any](parallelism int, action func(message *T)) *Processor[T] {
	processor := &Processor[T]{}

	processor.in = make(chan Message[T])
	processor.block = &blocks.ActionBlock[Message[T]]{
		Input: processor.in,
		Done: func(message Message[T]) {
			message.Ack(processor, message.Payload)
		},
		Parallelism: parallelism,
		Action:      func(message Message[T]) { action(message.Payload) },
	}

	processor.block.Run()

	return processor
}

type ProcessorCompletion[T any] struct {
	message   *T
	Processor *Processor[T]
}
