package routing

import (
	"github.com/stretchr/testify/assert"
	"simpipe/blocks"
	"sync"
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

type NodeAck[T any] struct {
	Item *T
	Node *Node[T]
}

type Router[T any] struct {
	nodes      []*Node[T]
	completion func(item *T)
	state      map[*T]*RoutingSlipState[T]
	ack        chan NodeAck[T]
}

func CreateRouter[T any](completion func(item *T)) *Router[T] {
	return &Router[T]{
		completion: completion,
		state:      make(map[*T]*RoutingSlipState[T]),
		ack:        make(chan NodeAck[T]),
	}
}

func (r *Router[T]) AddNode(parallelism int, action func(item *T)) *Node[T] {
	node := &Node[T]{}

	node.in = make(chan *T)
	node.block = &blocks.ActionBlock[*T]{
		Input: node.in,
		Done: func(item *T) {
			r.ack <- NodeAck[T]{
				Item: item,
				Node: node,
			}
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

	r.advanceNext(state, item)
}

func (r *Router[T]) Run() {
	go r.processNodeAck()
	r.runNodes()
}

func (r *Router[T]) runNodes() {
	for _, node := range r.nodes {
		node.Run()
	}
}

func (r *Router[T]) processNodeAck() {
	for {
		ack := <-r.ack
		r.done(ack.Node, ack.Item)
	}
}

func (r *Router[T]) Send(item *T, slip *RoutingSlip[T]) {
	state := slip.start()

	r.advanceNext(state, item)
}

func (r *Router[T]) advanceNext(state *RoutingSlipState[T], item *T) {
	next := state.advance()
	r.state[item] = next

	if next == nil {
		r.completion(item)
		return
	}

	next.send(item)
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

func (rsn *RoutingSlipNode[T]) Send(item *T) {
	rsn.node.Send(item)
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

func (s *RoutingSlip[T]) start() *RoutingSlipState[T] {
	return &RoutingSlipState[T]{
		node: &RoutingSlipNode[T]{
			node: nil,
			next: s.head,
		},
	}
}

func TestSingleNodeSlip(t *testing.T) {
	item := &Item{"foo"}
	var waiter sync.WaitGroup

	var completed *Item
	router := CreateRouter(func(item *Item) {
		completed = item
		waiter.Done()
	})

	node := router.AddNode(1, func(item *Item) {
		item.Text = "processed"
	})
	router.Run()

	slip := CreateRoutingSlip[Item]()
	slip.Add(node)

	waiter.Add(1)
	router.Send(item, slip)
	waiter.Wait()

	assert.Equal(t, "processed", item.Text)
	assert.Equal(t, item, completed)
}

func TestMultiNodeSlip(t *testing.T) {
	item := &Item{"foo"}
	var waiter sync.WaitGroup

	var completedText string
	router := CreateRouter(func(item *Item) {
		completedText = item.Text
		waiter.Done()
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

	waiter.Add(1)
	router.Send(item, slip)
	waiter.Wait()

	assert.Equal(t, "foo.A.B", item.Text)
	assert.Equal(t, "foo.A.B", completedText, "Should complete only at the final stage")
}
