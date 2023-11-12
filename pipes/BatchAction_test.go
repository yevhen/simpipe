package pipes

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBatchPipeBlockIntegration(t *testing.T) {
	sent := make(chan []string)
	action := func(items []string) {
		sent <- items
	}

	filter := func(item string) bool {
		return item == "foo"
	}

	var nextReceived = make([]string, 0)
	next := func(item string) Pipe[string] {
		nextReceived = append(nextReceived, item)
		return nil
	}

	pipe := CreateBatchActionPipe(4, 2, 2, action, filter, next)
	pipe.Run()

	pipe.Send("foo")
	pipe.Send("bar")
	pipe.Send("foo")
	pipe.Close()

	assert.ElementsMatch(t, []string{"foo", "foo"}, <-sent)
	assert.ElementsMatch(t, []string{"foo", "foo", "bar"}, nextReceived)
}
