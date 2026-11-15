package transport

// Errors returns the channel used to report HTTP delivery
// failures after retry attempts are exhausted.
func (t *HTTPTransport) Errors() <-chan error {
	return t.errors
}
