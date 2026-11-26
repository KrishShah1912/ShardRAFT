package history

import (
	"reflect"

	"github.com/anishathalye/porcupine"
)

type requestIdentity struct {
	ClientID  string
	RequestID string
}

type cachedMutation struct {
	Input  Input
	Output Output
}

type keyState struct {
	Found bool
	Value string
	Dedup map[requestIdentity]cachedMutation
}

func singleKeyModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return keyState{
				Dedup: make(map[requestIdentity]cachedMutation),
			}
		},

		Step: func(
			state interface{},
			input interface{},
			output interface{},
		) (bool, interface{}) {
			current := state.(keyState)
			in := input.(Input)
			out := output.(Output)

			if out.Outcome != Success {
				return false, current
			}

			if in.Kind == Get {
				expected := Output{
					Outcome: Success,
					Found:   current.Found,
					Value:   current.Value,
				}

				return out == expected, current
			}

			identity := requestIdentity{
				ClientID:  in.ClientID,
				RequestID: in.RequestID,
			}

			if cached, exists := current.Dedup[identity]; exists {
				return cached.Input == in &&
					cached.Output == out, current
			}

			next := keyState{
				Found: current.Found,
				Value: current.Value,
				Dedup: make(
					map[requestIdentity]cachedMutation,
					len(current.Dedup)+1,
				),
			}

			for id, cached := range current.Dedup {
				next.Dedup[id] = cached
			}

			expected := Output{
				Outcome: Success,
			}

			switch in.Kind {
			case Put:
				next.Found = true
				next.Value = in.Value

				expected.Applied = true
				expected.Value = in.Value

			case Delete:
				expected.Applied = current.Found

				next.Found = false
				next.Value = ""

			case CAS:
				if current.Found &&
					current.Value == in.Expected {

					next.Found = true
					next.Value = in.Value

					expected.Applied = true
					expected.Value = in.Value
				}

			default:
				return false, current
			}

			if out != expected {
				return false, current
			}

			next.Dedup[identity] = cachedMutation{
				Input:  in,
				Output: out,
			}

			return true, next
		},

		Equal: func(a, b interface{}) bool {
			return reflect.DeepEqual(a, b)
		},
	}
}
