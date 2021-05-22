package simpleflow

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_executableKeys(t *testing.T) {
	fl := New()
	fl.Node("find-artifact", []string{}, func(ctx context.Context, n RunningNode) error {
		n.Put("artifact-id", "build-233")
		return nil
	})

	fl.Node("dispatch-artifact", []string{"find-artifact"}, func(ctx context.Context, n RunningNode) error {
		artifactID := n.Get("artifact-id")
		n.Put("artifact-host", fmt.Sprintf("%s:host1", artifactID))
		return nil
	})
	ctx := context.TODO()
	assert.Equal(t, len(fl.executableKeys()), 1)
	fl.Start(ctx, map[string]interface{}{"deploy-id": 1})
	fl.Step(ctx)
	assert.Equal(t, "build-233:host1", fl.nodes["dispatch-artifact"].outputs["artifact-host"])
	assert.Equal(t, "build-233:host1", fl.state["artifact-host"])
}
