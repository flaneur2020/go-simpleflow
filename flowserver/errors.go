package flowserver

import (
	"errors"
	"fmt"
)

var (
	errFlowNotFound = errors.New("flow not found")
)

type ServerError struct {
	kind string
	err  error
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("%s: %s", e.kind, e.err)
}

func newUnexpectedError(err error) *ServerError {
	return &ServerError{kind: "unexpected", err: err}
}
