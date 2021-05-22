package simpleflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_executableKeys(t *testing.T) {
	fl := New()
	fl.Node("find-artifact", []string{}, func(context.Context, map[string][]byte) ([]byte, error) {
		return []byte("1"), nil
	})
	fl.Node("dispatch-artifact", []string{"find-artifact"}, func(ctx context.Context, args map[string][]byte) ([]byte, error) {
		return []byte("2"), nil
	})
	ctx := context.TODO()
	fl.Start(ctx, map[string][]byte{})
	fl.Step(ctx)
	assert.Equal(t, len(fl.results), 2)
	assert.Equal(t, fl.results["find-artifact"].r, []byte("1"))
}
