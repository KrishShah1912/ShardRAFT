package kvstore

// RequestResult records the deterministic outcome of
// a replicated write operation.
type RequestResult struct {
	Applied bool   `json:"applied"`
	Value   string `json:"value,omitempty"`
}

// RequestKey uniquely identifies a client request.
type RequestKey struct {
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

// RequestRecord stores the original command and result.
// Keeping the command allows us to detect accidental
// reuse of the same request ID with different arguments.
type RequestRecord struct {
	Command Command       `json:"command"`
	Result  RequestResult `json:"result"`
}
