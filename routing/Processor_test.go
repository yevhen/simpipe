package routing

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type MockMessage[T any] struct {
	payload *T
	waiter  *sync.WaitGroup
}

func (dm *MockMessage[T]) Payload() *T {
	return dm.payload
}

func (dm *MockMessage[T]) Apply(action func(T) func(*T)) {
	patch := action(*dm.payload)
	patch(dm.payload)
}

func (dm *MockMessage[T]) Done() {
	dm.waiter.Done()
}

func TestProcessorFilterSkipsActionAndSignalDone(t *testing.T) {
	payload := &Item{"test"}

	waiter := &sync.WaitGroup{}
	waiter.Add(1)

	filter := func(payload *Item) bool {
		return false
	}

	processor := ActionWithFilter(1, filter, func(payload *Item) {
		payload.Text += ".processed"
	})

	message := &MockMessage[Item]{
		payload: payload,
		waiter:  waiter,
	}

	processor.Send(message)
	waiter.Wait()

	assert.Equal(t, "test", payload.Text)
}
