package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/KrishShah1912/shardraft/internal/observability"
)

func registerEventHandlers(
	mux *http.ServeMux,
	bus *observability.Bus,
) {
	mux.HandleFunc("GET /api/v1/events/history", func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		after := uint64(0)

		if value := r.URL.Query().Get("after"); value != "" {
			parsed, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				http.Error(
					w,
					"invalid after event ID",
					http.StatusBadRequest,
				)
				return
			}

			after = parsed
		}

		events := bus.EventsAfter(after)

		writeJSON(w, http.StatusOK, map[string]any{
			"events":          events,
			"latest_event_id": bus.LatestEventID(),
		})
	})

	mux.HandleFunc("GET /api/v1/events", func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(
				w,
				"streaming unsupported",
				http.StatusInternalServerError,
			)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		lastEventID := uint64(0)

		if value := r.Header.Get("Last-Event-ID"); value != "" {
			parsed, err := strconv.ParseUint(value, 10, 64)
			if err == nil {
				lastEventID = parsed
			}
		}

		if value := r.URL.Query().Get("after"); value != "" {
			parsed, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				http.Error(
					w,
					"invalid after event ID",
					http.StatusBadRequest,
				)
				return
			}

			lastEventID = parsed
		}

		for _, event := range bus.EventsAfter(lastEventID) {
			if err := writeSSEEvent(w, event); err != nil {
				return
			}
		}

		flusher.Flush()

		sub := bus.Subscribe(
			observability.DefaultSubscriberSize,
		)
		defer bus.Unsubscribe(sub.ID)

		for {
			select {
			case <-r.Context().Done():
				return

			case event, ok := <-sub.Events:
				if !ok {
					return
				}

				if event.ID <= lastEventID {
					continue
				}

				if err := writeSSEEvent(w, event); err != nil {
					return
				}

				lastEventID = event.ID
				flusher.Flush()
			}
		}
	})
}

func writeSSEEvent(
	w http.ResponseWriter,
	event observability.Event,
) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"id: %d\n",
		event.ID,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"event: %s\n",
		event.Type,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"data: %s\n\n",
		payload,
	); err != nil {
		return err
	}

	return nil
}
