package kvstore

import "errors"

// ErrRequestConflict indicates that an existing client
// request ID was reused with different command arguments.
var ErrRequestConflict = errors.New(
	"request ID reused with a different command",
)

// CheckRequest checks whether a request has already
// been applied and whether its arguments match.
//
// An identical retry returns the original result.
// A conflicting retry returns ErrRequestConflict.
func (s *Store) CheckRequest(
	command Command,
) (RequestResult, bool, error) {
	if err := command.Validate(); err != nil {
		return RequestResult{}, false, err
	}

	if command.ClientID == "" {
		return RequestResult{}, false, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key := RequestKey{
		ClientID:  command.ClientID,
		RequestID: command.RequestID,
	}

	record, exists := s.dedup[key]
	if !exists {
		return RequestResult{}, false, nil
	}

	if record.Command != command {
		return RequestResult{}, true, ErrRequestConflict
	}

	return record.Result, true, nil
}
