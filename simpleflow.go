package simpleflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

type NodeFunc func(context.Context, FlowNode) error

type FlowNode interface {
	Key() string

	// Input reads the data stored inside the Flow store. The
	// data has to be JSON serializable.
	Input(key string, v interface{}) error

	// Output stores a JSON serializable object into the Node's
	// output buffer with a unique key, the depending funcs can
	// get these values by the Input() method.
	Output(key string, d interface{})
}

type node struct {
	flow    *Flow
	key     string
	fn      NodeFunc
	state   string
	deps    []string
	outputs map[string][]byte
	err     error
}

var _ FlowNode = &node{}

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

func (n *node) Input(key string, v interface{}) error {
	n.flow.mu.Lock()
	defer n.flow.mu.Unlock()

	buf, exists := n.flow.data[key]
	if !exists {
		return errors.New("input not found")
	}
	return json.Unmarshal(buf, v)
}

func (n *node) Output(key string, d interface{}) {
	n.flow.mu.Lock()
	defer n.flow.mu.Unlock()

	if n.state == nodeCompleted {
		panic("can not Put to a completed node")
	}

	buf, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	n.outputs[key] = buf
}

type Flow struct {
	data  map[string][]byte
	nodes map[string]*node
	mu    sync.Mutex
}

func New() *Flow {
	return &Flow{
		nodes: map[string]*node{},
		data:  map[string][]byte{},
	}
}

func (fl *Flow) Node(key string, deps []string, fn NodeFunc) {
	n := &node{
		key:     key,
		flow:    fl,
		state:   nodePending,
		fn:      fn,
		deps:    deps,
		outputs: map[string][]byte{},
		err:     nil,
	}
	fl.nodes[key] = n
}

func (fl *Flow) Start(ctx context.Context, args map[string]interface{}) []string {
	var err error

	for argKey, arg := range args {
		fl.data[argKey], err = json.Marshal(arg)
		if err != nil {
			panic(err)
		}
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

func (fl *Flow) Data(key string, v interface{}) error {
	buf, exists := fl.data[key]
	if !exists {
		return errors.New("data not found")
	}

	return json.Unmarshal(buf, v)
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
