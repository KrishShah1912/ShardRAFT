package transport

import (
	"context"
	"fmt"
	"time"
)

const (
	maxDeliveryAttempts = 4
	initialRetryDelay   = 50 * time.Millisecond
	maxRetryDelay       = 400 * time.Millisecond
)

// deliverWithRetry retries temporary delivery failures with
// bounded exponential backoff.
//
// Delivery remains best-effort: after the final failure,
// the caller receives an error.
func (t *HTTPTransport) deliverWithRetry(
	ctx context.Context,
	item outgoing,
) error {
	delay := initialRetryDelay
	var lastErr error

	for attempt := 1; attempt <= maxDeliveryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := t.deliver(ctx, item); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt == maxDeliveryAttempts {
			break
		}

		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()

		case <-timer.C:
		}

		delay *= 2
		if delay > maxRetryDelay {
			delay = maxRetryDelay
		}
	}

	return fmt.Errorf(
		"Raft message delivery failed after %d attempts: %w",
		maxDeliveryAttempts,
		lastErr,
	)
}
