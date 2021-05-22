package simpleflow

import (
	"context"
	"log"
)

type NodeFunc func(context.Context, RunningNode) error

type RunningNode interface {
	Get(key string) interface{}
	Put(key string, d interface{})
}

type node struct {
	flow    *Flow
	state   string
	fn      NodeFunc
	deps    []string
	outputs map[string]interface{}
	err     error
}

var _ RunningNode = &node{}

const (
	nodePending   = "pending"
	nodeRunning   = "running"
	nodeCompleted = "completed"
)

func (n *node) completed() bool {
	return n.state == nodeCompleted
}

func (n *node) Get(key string) interface{} {
	d, _ := n.flow.state[key]
	return d
}

func (n *node) Put(key string, d interface{}) {
	if n.state == nodeCompleted {
		panic("can not Put to a completed node")
	}
	n.outputs[key] = d
}

type Flow struct {
	state map[string]interface{}
	nodes map[string]*node
}

func New() *Flow {
	return &Flow{
		nodes: map[string]*node{},
		state: map[string]interface{}{},
	}
}

func (fl *Flow) Node(key string, deps []string, fn NodeFunc) {
	n := &node{
		flow:    fl,
		state:   nodePending,
		fn:      fn,
		deps:    deps,
		outputs: map[string]interface{}{},
		err:     nil,
	}
	fl.nodes[key] = n
}

func (fl *Flow) Start(ctx context.Context, args map[string]interface{}) error {
	for argKey, arg := range args {
		fl.state[argKey] = arg
	}

	// find the funcs with no depend to execute
	keys := fl.executableKeys()
	log.Printf("start keys=%v", keys)
	for _, key := range keys {
		n := fl.nodes[key]
		if len(n.deps) > 0 {
			continue
		}

		fl.executeFunc(ctx, key)
	}
	return nil
}

func (fl *Flow) Get(key string) interface{} {
	d, _ := fl.state[key]
	return d
}

func (fl *Flow) executeFunc(ctx context.Context, key string) {
	n, exists := fl.nodes[key]
	if !exists || n.completed() {
		return
	}

	log.Printf("execute %s", key)
	n.state = nodeRunning
	n.err = n.fn(ctx, n)
	n.state = nodeCompleted
	for outputKey, output := range n.outputs {
		fl.state[outputKey] = output
	}
}

func (fl *Flow) EOF() bool {
	return len(fl.executableKeys()) == 0
}

func (fl *Flow) Step(ctx context.Context) {
	keys := fl.executableKeys()

	log.Printf("step keys=%v", keys)
	for _, key := range keys {
		fl.executeFunc(ctx, key)
	}
}

func (fl *Flow) executableKeys() []string {
	// find the nodes not completed yet
	resultKeys := []string{}
	for key, n := range fl.nodes {
		if n.completed() {
			continue
		}

		depsOK := true
		if len(n.deps) > 0 {
			for _, dep := range n.deps {
				dn, exists := fl.nodes[dep]
				if !exists || !dn.completed() {
					depsOK = false
				}
			}
		}

		if depsOK {
			resultKeys = append(resultKeys, key)
		}
	}

	return resultKeys
}
