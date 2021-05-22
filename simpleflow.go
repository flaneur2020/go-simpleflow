package simpleflow

import (
	"context"
	"sync"
)

type NodeFunc func(context.Context, RunningNode) error

type RunningNode interface {
	Key() string
	Input(key string) interface{}
	Output(key string, d interface{})
}

type node struct {
	flow    *Flow
	key     string
	fn      NodeFunc
	state   string
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

func (n *node) success() bool {
	return n.completed() && n.err == nil
}

func (n *node) Key() string {
	return n.key
}

func (n *node) Input(key string) interface{} {
	n.flow.mu.Lock()
	defer n.flow.mu.Unlock()

	d, _ := n.flow.data[key]
	return d
}

func (n *node) Output(key string, d interface{}) {
	n.flow.mu.Lock()
	defer n.flow.mu.Unlock()

	if n.state == nodeCompleted {
		panic("can not Put to a completed node")
	}
	n.outputs[key] = d
}

type Flow struct {
	data  map[string]interface{}
	nodes map[string]*node
	mu    sync.Mutex
}

func New() *Flow {
	return &Flow{
		nodes: map[string]*node{},
		data:  map[string]interface{}{},
	}
}

func (fl *Flow) Node(key string, deps []string, fn NodeFunc) {
	n := &node{
		key:     key,
		flow:    fl,
		state:   nodePending,
		fn:      fn,
		deps:    deps,
		outputs: map[string]interface{}{},
		err:     nil,
	}
	fl.nodes[key] = n
}

func (fl *Flow) Start(ctx context.Context, args map[string]interface{}) []string {
	for argKey, arg := range args {
		fl.data[argKey] = arg
	}

	// find the funcs with no depend to execute
	executedKeys := []string{}
	for _, key := range fl.executableKeys() {
		n := fl.nodes[key]
		if len(n.deps) > 0 {
			continue
		}

		fl.executeFunc(ctx, key)
		executedKeys = append(executedKeys, key)
	}
	return executedKeys
}

func (fl *Flow) Get(key string) interface{} {
	d, _ := fl.data[key]
	return d
}

func (fl *Flow) executeFunc(ctx context.Context, key string) {
	n, exists := fl.nodes[key]
	if !exists || n.completed() {
		return
	}

	n.state = nodeRunning
	n.err = n.fn(ctx, n)
	n.state = nodeCompleted
	for outputKey, output := range n.outputs {
		fl.data[outputKey] = output
	}
}

func (fl *Flow) EOF() bool {
	return len(fl.executableKeys()) == 0
}

func (fl *Flow) Step(ctx context.Context) []string {
	keys := fl.executableKeys()

	for _, key := range keys {
		fl.executeFunc(ctx, key)
	}
	return keys
}

func (fl *Flow) executableKeys() []string {
	resultKeys := []string{}
	for key, n := range fl.nodes {
		// skip the completed nodes
		if n.completed() {
			continue
		}

		// select the nodes whose all the deps has got successful
		depsOK := true
		if len(n.deps) > 0 {
			for _, dep := range n.deps {
				dn, exists := fl.nodes[dep]
				if !exists || !dn.success() {
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
