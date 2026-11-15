package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Operation string

const (
	OpSet            Operation = "SET"
	OpPut            Operation = "PUT"
	OpDelete         Operation = "DELETE"
	OpCompareAndSwap Operation = "COMPARE_AND_SWAP"
)

type Command struct {
	Op        Operation `json:"op"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	Expected  string    `json:"expected,omitempty"`
	ClientID  string    `json:"client_id,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
}

func (c Command) Validate() error {
	if c.Key == "" {
		return errors.New("key cannot be empty")
	}

	if (c.ClientID == "") != (c.RequestID == "") {
		return errors.New("client_id and request_id must be supplied together")
	}

	switch c.Op {
	case OpSet, OpPut, OpDelete, OpCompareAndSwap:
		return nil
	default:
		return fmt.Errorf("unsupported operation: %q", c.Op)
	}
}

func EncodeCommand(c Command) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

func DecodeCommand(data []byte) (Command, error) {
	var c Command

	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}

	if err := c.Validate(); err != nil {
		return Command{}, err
	}

	return c, nil
}
