package simpleflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_flow_Step(t *testing.T) {
	fl := New()
	fl.Node("n1", []string{}, func(ctx context.Context, n FlowNode) error {
		n.Output("n1-executed", true)
		return nil
	})
	fl.Node("n2", []string{"n1"}, func(ctx context.Context, n FlowNode) error {
		n.Output("n2-executed", true)
		return nil
	})
	fl.Node("n3", []string{"n1"}, func(ctx context.Context, n FlowNode) error {
		n.Output("n3-executed", true)
		return nil
	})
	fl.Node("n4", []string{"n2", "n3"}, func(ctx context.Context, n FlowNode) error {
		n.Output("n4-executed", true)
		return nil
	})
	ctx := context.TODO()
	gotKeys := fl.Start(ctx, map[string]interface{}{})
	assert.Equal(t, []string{"n1"}, gotKeys)
	gotKeys = fl.Step(ctx)
	assert.Contains(t, gotKeys, "n2")
	assert.Contains(t, gotKeys, "n3")
	gotKeys = fl.Step(ctx)
	assert.Equal(t, []string{"n4"}, gotKeys)
}

func Test_flow_Step_with_persist(t *testing.T) {
	newFlow := func() *Flow {
		fl := New()
		fl.Node("n1", []string{}, func(ctx context.Context, n FlowNode) error {
			n.Output("n1-executed", true)
			return nil
		})
		fl.Node("n2", []string{"n1"}, func(ctx context.Context, n FlowNode) error {
			n.Output("n2-executed", true)
			return errors.New("oh no")
		})
		fl.Node("n3", []string{"n1"}, func(ctx context.Context, n FlowNode) error {
			n.Output("n3-executed", true)
			return nil
		})
		fl.Node("n4", []string{"n2", "n3"}, func(ctx context.Context, n FlowNode) error {
			n.Output("n4-executed", true)
			return nil
		})
		return fl
	}

	var (
		buf = bytes.NewBuffer([]byte{})
		ctx = context.TODO()
	)

	fl1 := newFlow()
	gotKeys := fl1.Start(ctx, map[string]interface{}{})

	fl1.EncodeTo(buf)
	fl2 := newFlow()
	fl2.Decode(buf)

	assert.Equal(t, []string{"n1"}, gotKeys)
	gotKeys = fl2.Step(ctx)
	assert.Contains(t, gotKeys, "n2")
	assert.Contains(t, gotKeys, "n3")

	buf = bytes.NewBuffer([]byte{})
	fl2.EncodeTo(buf)
	fl3 := newFlow()
	fl3.Decode(buf)

	gotKeys = fl3.Step(ctx)
	assert.Equal(t, []string{}, gotKeys)
}

func Test_flow_Step2(t *testing.T) {
	fl := New()
	fl.Node("n1", []string{}, func(ctx context.Context, n FlowNode) error {
		n.Output("n1-executed", true)
		return nil
	})
	fl.Node("n2", []string{"n1"}, func(ctx context.Context, n FlowNode) error {
		n.Output("n2-executed", true)
		return errors.New("failed")
	})
	fl.Node("n3", []string{"n1"}, func(ctx context.Context, n FlowNode) error {
		n.Output("n3-executed", true)
		return nil
	})
	fl.Node("n4", []string{"n2", "n3"}, func(ctx context.Context, n FlowNode) error {
		n.Output("n4-executed", true)
		return nil
	})
	ctx := context.TODO()
	gotKeys := fl.Start(ctx, map[string]interface{}{})
	assert.Equal(t, []string{"n1"}, gotKeys)
	gotKeys = fl.Step(ctx)
	assert.Contains(t, gotKeys, "n2")
	assert.Contains(t, gotKeys, "n3")
	gotKeys = fl.Step(ctx)
	assert.Equal(t, []string{}, gotKeys)
}

func Test_executableKeys(t *testing.T) {
	fl := New()
	fl.Node("find-artifact", []string{}, func(ctx context.Context, n FlowNode) error {
		n.Output("artifact-id", "build-233")
		return nil
	})

	fl.Node("dispatch-artifact", []string{"find-artifact"}, func(ctx context.Context, n FlowNode) error {
		var artifactID string
		n.Input("artifact-id", &artifactID)
		n.Output("artifact-host", fmt.Sprintf("%s:host1", artifactID))
		return nil
	})
	ctx := context.TODO()
	assert.Equal(t, len(fl.executableKeys()), 1)
	fl.Start(ctx, map[string]interface{}{"deploy-id": 1})
	fl.Step(ctx)

	var got string
	json.Unmarshal(fl.nodes["dispatch-artifact"].outputs["artifact-host"], &got)
	assert.Equal(t, "build-233:host1", got)
	fl.Data("artifact-host", &got)
	assert.Equal(t, "build-233:host1", got)
}
