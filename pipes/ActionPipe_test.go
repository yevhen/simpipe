package pipes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestActionPipeBlockIntegration_Action(t *testing.T) {
	sent := make(chan string)
	action := func(item string) {
		sent <- item
	}

	pipe := CreateActionPipe(2, 2, action, func(item string) bool { return true }, func(item string) Pipe[string] { return nil })
	pipe.Run()

	pipe.Send("foo")
	pipe.Close()

	assert.Equal(t, "foo", <-sent)
}

func TestActionPipeBlockIntegration_Filter(t *testing.T) {
	sent := make(chan string)
	action := func(item string) {
		sent <- item
	}

	filter := func(item string) bool {
		return item == "foo"
	}

	pipe := CreateActionPipe(2, 2, action, filter, func(item string) Pipe[string] { return nil })
	pipe.Run()

	pipe.Send("foo")
	pipe.Send("bar")
	pipe.Close()

	assert.Equal(t, "foo", <-sent)
	assert.Equal(t, 0, len(sent))
}

func TestActionPipeBlockIntegration_Next(t *testing.T) {
	action := func(item string) {}

	var nextReceived = make([]string, 0)
	next := func(item string) Pipe[string] {
		nextReceived = append(nextReceived, item)
		return nil
	}

	pipe := CreateActionPipe(2, 2, action, func(item string) bool { return true }, next)
	pipe.Run()

	pipe.Send("foo")
	pipe.Send("bar")
	pipe.Close()

	assert.ElementsMatch(t, []string{"foo", "bar"}, nextReceived)
}

func TestActionPipeBlockIntegration_FilteredItemsArePassedToNext(t *testing.T) {
	action := func(item string) {}

	var nextReceived = make([]string, 0)
	next := func(item string) Pipe[string] {
		nextReceived = append(nextReceived, item)
		return nil
	}

	pipe := CreateActionPipe(2, 2, action, func(item string) bool { return item == "foo" }, next)
	pipe.Run()

	pipe.Send("foo")
	pipe.Send("bar")
	pipe.Close()

	assert.ElementsMatch(t, []string{"foo", "bar"}, nextReceived)
}
