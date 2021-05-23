package simpleflow

import (
	"encoding/json"
	"io"
)

type persistFlow struct {
	Data  map[string][]byte       `json:"data"`
	Nodes map[string]*persistNode `json:"nodes"`
}

type persistNode struct {
	State   string            `json:"state"`
	Outputs map[string][]byte `json:"outputs"`
	Err     string            `json:"err"`
}

func (n *persistFlow) Decode(buf io.Reader) error {
	dec := json.NewDecoder(buf)
	return dec.Decode(n)
}

func (n *persistFlow) Encode(buf io.Writer) error {
	enc := json.NewEncoder(buf)
	return enc.Encode(n)
}
