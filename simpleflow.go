package simpleflow

import "context"

type NodeFunc func(context.Context, map[string][]byte) ([]byte, error)

type NodeResult struct {
	r   []byte
	err error
}

type Flow struct {
	funcs   map[string]NodeFunc
	depends map[string][]string
	results map[string]NodeResult
	args    map[string]byte
}

func (fl *Flow) Node(key string, depends []string, nf NodeFunc) {
	fl.funcs[key] = nf
	fl.depends[key] = depends
}

func (fl *Flow) Start(ctx context.Context, args map[string][]byte) error {
	// find the funcs with no depend to execute
	keys := []string{}
	for key := range fl.funcs {
		deps, _ := fl.depends[key]
		if deps == nil || len(deps) == 0 {
			keys = append(keys, key)
		}
	}

	for _, key := range keys {
		f := fl.funcs[key]
		r, err := f(ctx, args)
		fl.results[key] = NodeResult{r, err}
	}

	return nil
}

func (fl *Flow) EOF() bool {
	return len(fl.executableKeys()) == 0
}

func (fl *Flow) Step(ctx context.Context) {
	keys := fl.executableKeys()

	for _, key := range keys {
		args := map[string][]byte{}
		if deps, _ := fl.depends[key]; deps != nil {
			for _, dep := range deps {
				args[dep] = fl.results[dep].r
			}
		}

		f := fl.funcs[key]
		r, err := f(ctx, args)
		fl.results[key] = NodeResult{r, err}
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
