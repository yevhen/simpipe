package blocks

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
