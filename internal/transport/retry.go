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

// deliverWithRetry preserves the original transport-internal API.
//
// The destination peer worker is resolved from the outgoing
// message so callers and existing tests do not need to know
// about telemetry bookkeeping.
func (t *HTTPTransport) deliverWithRetry(
	ctx context.Context,
	item outgoing,
) error {
	t.mu.Lock()
	worker := t.peers[item.message.To]
	t.mu.Unlock()

	if worker == nil {
		return fmt.Errorf(
			"%w: %d",
			ErrPeer,
			item.message.To,
		)
	}

	return t.deliverWithRetryForPeer(
		ctx,
		worker,
		item,
	)
}

func (t *HTTPTransport) deliverWithRetryForPeer(
	ctx context.Context,
	worker *peerWorker,
	item outgoing,
) error {
	delay := initialRetryDelay
	var lastErr error

	for attempt := 1; attempt <= maxDeliveryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := t.deliver(
			ctx,
			worker,
			item,
			attempt,
		)

		if err == nil {
			return nil
		}

		lastErr = err

		if attempt == maxDeliveryAttempts {
			break
		}

		worker.mu.Lock()
		worker.retries++
		worker.mu.Unlock()

		t.emit(EventRetry, map[string]any{
			"peer_id":      worker.id,
			"address":      worker.address,
			"message_type": item.message.Type,
			"request_id":   item.message.RequestID,
			"attempt":      attempt,
			"next_attempt": attempt + 1,
			"delay_ms":     delay.Milliseconds(),
			"error":        err.Error(),
		})

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

	worker.mu.Lock()
	worker.failures++
	worker.lastFailureAt = time.Now()

	if lastErr != nil {
		worker.lastError = lastErr.Error()
	}

	worker.mu.Unlock()

	errorMessage := "unknown transport failure"

	if lastErr != nil {
		errorMessage = lastErr.Error()
	}

	t.emit(EventFailure, map[string]any{
		"peer_id":      worker.id,
		"address":      worker.address,
		"message_type": item.message.Type,
		"request_id":   item.message.RequestID,
		"attempts":     maxDeliveryAttempts,
		"error":        errorMessage,
	})

	return fmt.Errorf(
		"Raft message delivery failed after %d attempts: %w",
		maxDeliveryAttempts,
		lastErr,
	)
}
