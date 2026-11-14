package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Operation string

const (
	OpSet    Operation = "SET"
	OpDelete Operation = "DELETE"
)

type Command struct {
	Op    Operation `json:"op"`
	Key   string    `json:"key"`
	Value string    `json:"value,omitempty"`
}

func (c Command) Validate() error {
	if c.Key == "" {
		return errors.New("key cannot be empty")
	}

	switch c.Op {
	case OpSet, OpDelete:
		return nil
	default:
		return fmt.Errorf("unsupported operation: %q", c.Op)
	}
}

// EncodeCommand serializes a command for the Raft log.
func EncodeCommand(c Command) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

// DecodeCommand parses and validates a command.
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
