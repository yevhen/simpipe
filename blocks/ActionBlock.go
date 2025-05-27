package blocks

// ActionBlockOption defines a configuration option for an ActionBlock
type ActionBlockOption[T any] func(*ActionBlock[T])

// WithActionParallelism returns an option that sets the parallelism level for the ActionBlock
func WithActionParallelism[T any](parallelism int) ActionBlockOption[T] {
	return func(block *ActionBlock[T]) {
		block.Parallelism = parallelism
	}
}

// WithActionDoneCallback returns an option that sets the callback function called after an item is processed
func WithActionDoneCallback[T any](done func(item T)) ActionBlockOption[T] {
	return func(block *ActionBlock[T]) {
		block.Done = done
	}
}

// ActionBlock processes items from an input channel with a specified action
type ActionBlock[T any] struct {
	Input       <-chan T
	Done        func(item T)
	Parallelism int
	Action      func(item T)
}

// NewActionBlock creates a new ActionBlock with the given input channel, action function, and options
func NewActionBlock[T any](input <-chan T, action func(item T), options ...ActionBlockOption[T]) *ActionBlock[T] {
	block := &ActionBlock[T]{
		Input:       input,
		Action:      action,
		Parallelism: 1,
		Done:        func(item T) {},
	}

	for _, option := range options {
		option(block)
	}

	return block
}

func (block *ActionBlock[T]) Run() {
	for i := 0; i < block.Parallelism; i++ {
		go block.process()
	}
}

func (block *ActionBlock[T]) process() {
	for item := range block.Input {
		block.Action(item)
		block.Done(item)
	}
}
