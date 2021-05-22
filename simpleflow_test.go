package simpleflow

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_executableKeys(t *testing.T) {
	fl := New()
	fl.Node("find-artifact", []string{}, func(ctx context.Context, in NodeInput, out NodeOutput) error {
		out.Put("artifact-id", "build-233")
		return nil
	})

	fl.Node("dispatch-artifact", []string{"find-artifact"}, func(ctx context.Context, in NodeInput, out NodeOutput) error {
		artifactID := in.Get("artifact-id")
		out.Put("artifact-host", fmt.Sprintf("%s:host1", artifactID))
		return nil
	})
	ctx := context.TODO()
	assert.Equal(t, len(fl.executableKeys()), 1)
	fl.Start(ctx, map[string]interface{}{"deploy-id": 1})
	fl.Step(ctx)
	assert.Equal(t, len(fl.results), 2)
	assert.Equal(t, fl.state["artifact-host"], "build-233:host1")
}
