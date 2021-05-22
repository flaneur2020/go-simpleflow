package simpleflow

import "context"

type NodeFunc func(context.Context, NodeInput, NodeOutput) error

type NodeInput interface {
	Get(key string) interface{}
}

type NodeOutput interface {
	Put(key string, d interface{})
}

type NodeResult struct {
	outputs map[string]interface{}
	err     error
}

var n NodeOutput = &NodeResult{}

func (r *NodeResult) Put(key string, d interface{}) {
	r.outputs[key] = d
}

type Flow struct {
	funcs   map[string]NodeFunc
	depends map[string][]string
	state   map[string]interface{}
	results map[string]NodeResult
}

func New() *Flow {
	return &Flow{
		funcs:   map[string]NodeFunc{},
		depends: map[string][]string{},
		state:   map[string]interface{}{},
		results: map[string]NodeResult{},
	}
}

func (fl *Flow) Node(key string, depends []string, nf NodeFunc) {
	fl.funcs[key] = nf
	fl.depends[key] = depends
}

func (fl *Flow) Start(ctx context.Context, args map[string]interface{}) error {
	for argKey, arg := range args {
		fl.state[argKey] = arg
	}
	// find the funcs with no depend to execute
	keys := fl.executableKeys()
	for _, key := range keys {
		fl.executeFunc(ctx, key)
	}
	return nil
}

func (fl *Flow) Get(key string) interface{} {
	d, _ := fl.state[key]
	return d
}

func (fl *Flow) executeFunc(ctx context.Context, key string) {
	result := NodeResult{
		outputs: map[string]interface{}{},
		err:     nil,
	}
	f := fl.funcs[key]
	result.err = f(ctx, fl, &result)
	fl.results[key] = result
	for outputKey, output := range result.outputs {
		fl.state[outputKey] = output
	}
}

func (fl *Flow) EOF() bool {
	return len(fl.executableKeys()) == 0
}

func (fl *Flow) Step(ctx context.Context) {
	keys := fl.executableKeys()

	for _, key := range keys {
		fl.executeFunc(ctx, key)
	}
}

func (fl *Flow) executableKeys() []string {
	// find the nodes not executed yet
	pendingKeys := []string{}
	for key := range fl.funcs {
		if _, hasResult := fl.results[key]; !hasResult {
			pendingKeys = append(pendingKeys, key)
		}
	}

	// find the nodes whose depends has already matched
	resultKeys := []string{}
	for _, key := range pendingKeys {
		deps, _ := fl.depends[key]
		if deps == nil {
			resultKeys = append(resultKeys, key)
			continue
		}

		depsOK := true
		for _, dep := range deps {
			if result, hasResult := fl.results[dep]; !hasResult || result.err != nil {
				depsOK = false
			}
		}

		if depsOK {
			resultKeys = append(resultKeys, key)
		}
	}

	return resultKeys
}
