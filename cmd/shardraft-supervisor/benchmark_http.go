package main

import (
	"encoding/json"
	"net/http"
)

func registerBenchmarkHandlers(
	mux *http.ServeMux,
	manager *benchmarkManager,
) {
	mux.HandleFunc(
		"POST /api/v1/benchmarks",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			var request benchmarkRequest

			decoder :=
				json.NewDecoder(
					r.Body,
				)

			decoder.DisallowUnknownFields()

			if err :=
				decoder.Decode(
					&request,
				); err != nil {

				http.Error(
					w,
					"invalid benchmark request",
					http.StatusBadRequest,
				)

				return
			}

			job, err :=
				manager.start(
					request,
				)

			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusConflict,
				)

				return
			}

			writeJSON(
				w,
				http.StatusAccepted,
				job,
			)
		},
	)

	mux.HandleFunc(
		"GET /api/v1/benchmarks/current",
		func(
			w http.ResponseWriter,
			_ *http.Request,
		) {
			job :=
				manager.currentJob()

			if job == nil {
				writeJSON(
					w,
					http.StatusOK,
					map[string]any{
						"state": benchmarkIdle,
					},
				)

				return
			}

			writeJSON(
				w,
				http.StatusOK,
				job,
			)
		},
	)

	mux.HandleFunc(
		"GET /api/v1/benchmarks/history",
		func(
			w http.ResponseWriter,
			_ *http.Request,
		) {
			writeJSON(
				w,
				http.StatusOK,
				map[string]any{
					"runs": manager.historySnapshot(),
				},
			)
		},
	)
}
