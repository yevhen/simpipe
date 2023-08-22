package simpipe

type ActionBlock[T any] struct {
	Input       <-chan T
	Done        func(item T)
	Parallelism int
	Action      func(item T)
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

func CreateActionBlock[T any](in chan T, done func(item T), parallelism int, action func(item T)) ActionBlock[T] {
	block := ActionBlock[T]{
		Input:       in,
		Done:        done,
		Parallelism: parallelism,
		Action:      action,
	}
	return block
}

func RunActionBlock[T any](in chan T, done func(item T), parallelism int, action func(item T)) {
	block := CreateActionBlock(in, done, parallelism, action)
	block.Run()
}
