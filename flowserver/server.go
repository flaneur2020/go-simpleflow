package flowserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flaneur2020/go-simpleflow/simpleflow"
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

type Server struct {
	storage        Storage
	buildFlowFuncs map[string]func() *simpleflow.Flow
	quitc          chan struct{}
}

type JobID string

type Storage interface {
	PopJob(limit int) []JobID

	Enqueue(buf []byte) (JobID, error)

	Update(jobID JobID, state string, buf []byte) error

	Load(jobID JobID) (string, []byte, error)
}

func New(storage Storage) *Server {
	return &Server{
		storage:        storage,
		buildFlowFuncs: map[string]func() *simpleflow.Flow{},
		quitc:          make(chan struct{}),
	}
}

func (s *Server) Register(key string, buildFlowFunc func() *simpleflow.Flow) {
	s.buildFlowFuncs[key] = buildFlowFunc
}

func (s *Server) Serve() {
}

func (s *Server) startFlow(ctx context.Context, key string, args map[string]interface{}) (JobID, error) {
	fn, exists := s.buildFlowFuncs[key]
	if !exists {
		return "", errFlowNotFound
	}

	flow := fn()
	flow.Prepare(args)
	buf, err := flow.Encode()
	if err != nil {
		return "", err
	}
	return s.storage.Enqueue(buf)
}

func (s *Server) executeJobs() {
	for {
		jobIDs := s.storage.PopJob(1)
		if len(jobIDs) == 0 {
			time.Sleep(3)
			continue
		}

		jobID := jobIDs[0]
		go s.executeOneJob(jobID)
	}
}

func (s *Server) executeOneJob(jobID JobID) error {
	flow, err := s.restoreFlow(jobID)
	if err != nil {
		return err
	}

	for !flow.EOF() {
		flow.Step(context.TODO())

		err = s.saveFlow(jobID, flow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) restoreFlow(jobID JobID) (*simpleflow.Flow, error) {
	flowKey, flowBuf, err := s.storage.Load(jobID)
	if err != nil {
		return nil, newUnexpectedError(err)
	}

	fn, exists := s.buildFlowFuncs[flowKey]
	if !exists {
		return nil, newUnexpectedError(errFlowNotFound)
	}

	flow := fn()
	err = flow.Decode(bytes.NewReader(flowBuf))
	if err != nil {
		return nil, newUnexpectedError(err)
	}

	return flow, nil
}

func (s *Server) saveFlow(jobID JobID, flow *simpleflow.Flow) error {
	flowBuf, err := flow.Encode()
	if err != nil {
		return newUnexpectedError(err)
	}

	err = s.storage.Update(jobID, flow.State(), flowBuf)
	if err != nil {
		return newUnexpectedError(err)
	}

	return nil
}
