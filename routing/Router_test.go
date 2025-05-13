package routing

import (
	"github.com/stretchr/testify/assert"
	"simpipe/blocks"
	"testing"
)

type Item struct {
	Text string
}

type Node[T any] struct {
	in    chan *T
	block *blocks.ActionBlock[*T]
}

func (n *Node[T]) Run() {
	n.block.Run()
}

func (n *Node[T]) Send(item *T) {
	n.in <- item
}

func (n *Node[T]) Close() {
	close(n.in)
}

type Router[T any] struct {
	nodes      []*Node[T]
	completion func(item *T)
	state      map[*T]*RoutingSlipState[T]
}

func CreateRouter[T any](completion func(item *T)) *Router[T] {
	return &Router[T]{
		completion: completion,
		state:      make(map[*T]*RoutingSlipState[T]),
	}
}

func (r *Router[T]) AddNode(parallelism int, action func(item *T)) *Node[T] {
	node := &Node[T]{}

	node.in = make(chan *T)
	node.block = &blocks.ActionBlock[*T]{
		Input: node.in,
		Done: func(item *T) {
			r.done(node, item)
		},
		Parallelism: parallelism,
		Action:      action,
	}

	r.nodes = append(r.nodes, node)

	return node
}

func (r *Router[T]) done(node *Node[T], item *T) {
	state := r.state[item]
	state.done(node)

	next := state.advance()
	r.state[item] = next

	if next == nil {
		r.completion(item)
		return
	}

	next.send(item)
}

func (r *Router[T]) Run() {
	for _, node := range r.nodes {
		node.Run()
	}
}

func (r *Router[T]) Send(item *T, slip *RoutingSlip[T]) {
	next := slip.head
	r.state[item] = &RoutingSlipState[T]{
		node: next,
	}
	next.Send(item)
}

func (r *Router[T]) Close() {
	for _, node := range r.nodes {
		node.Close()
	}
}

type RoutingSlipNode[T any] struct {
	node *Node[T]
	next *RoutingSlipNode[T]
}

type RoutingSlipState[T any] struct {
	node *RoutingSlipNode[T]
}

func (t *RoutingSlipState[T]) done(node *Node[T]) {
	// do nothing for now
}

func (t *RoutingSlipState[T]) advance() *RoutingSlipState[T] {
	if t.node.next == nil {
		return nil
	}

	return &RoutingSlipState[T]{
		node: t.node.next,
	}
}

func (t *RoutingSlipState[T]) send(item *T) {
	t.node.Send(item)
}

func (rsn *RoutingSlipNode[T]) Send(item *T) {
	rsn.node.Send(item)
}

type RoutingSlip[T any] struct {
	head *RoutingSlipNode[T]
}

func CreateRoutingSlip[T any]() *RoutingSlip[T] {
	return &RoutingSlip[T]{}
}

func (s *RoutingSlip[T]) Add(node *Node[T]) *RoutingSlipNode[T] {
	rsn := &RoutingSlipNode[T]{
		node: node,
	}

	if s.head != nil {
		s.head.next = rsn
	}

	if s.head == nil {
		s.head = rsn
	}

	return rsn
}

func TestSingleNodeSlip(t *testing.T) {
	item := &Item{"foo"}

	var completed *Item
	router := CreateRouter(func(item *Item) {
		completed = item
	})

	node := router.AddNode(1, func(item *Item) {
		item.Text = "processed"
	})
	router.Run()

	slip := &RoutingSlip[Item]{}
	slip.Add(node)

	router.Send(item, slip)
	router.Close()

	assert.Equal(t, "processed", item.Text)
	assert.Equal(t, item, completed)
}

func TestMultiNodeSlip(t *testing.T) {
	item := &Item{"foo"}

	var completed *Item
	var completedText string
	router := CreateRouter(func(item *Item) {
		completed = item
		completedText = item.Text
	})

	nodeA := router.AddNode(1, func(item *Item) {
		item.Text += ".A"
	})
	nodeB := router.AddNode(1, func(item *Item) {
		item.Text += ".B"
	})

	router.Run()

	slip := CreateRoutingSlip[Item]()
	slip.Add(nodeA)
	slip.Add(nodeB)

	router.Send(item, slip)
	router.Close()

	assert.Equal(t, "foo.A.B", item.Text)
	assert.Equal(t, "foo.A.B", completedText, "Should complete only at the final stage")
	assert.Equal(t, item, completed)
}
