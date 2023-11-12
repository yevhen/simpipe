package pipes

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestActionPipeBlockIntegration(t *testing.T) {
	sent := make(chan string)
	action := func(item string) {
		sent <- item
	}

	filter := func(item string) bool {
		return item == "foo"
	}

	var nextReceived = make([]string, 0)
	next := func(item string) Pipe[string] {
		nextReceived = append(nextReceived, item)
		return nil
	}

	pipe := CreateActionPipe(2, 2, action, filter, next)
	pipe.Run()

	pipe.Send("foo")
	pipe.Send("bar")
	pipe.Close()

	assert.Equal(t, "foo", <-sent)
	assert.ElementsMatch(t, []string{"foo", "bar"}, nextReceived)
}
