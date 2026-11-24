package main

import (
	"net/http"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

const (
	routingCodeNotLeader   = "not_leader"
	routingCodeUnavailable = "leader_unavailable"
	routingCodeUnknown     = "outcome_unknown"
)

type routingErrorResponse struct {
	Error    string      `json:"error"`
	Code     string      `json:"code"`
	LeaderID raft.NodeID `json:"leader_id"`
	Term     uint64      `json:"term"`
}

func writeRoutingError(
	w http.ResponseWriter,
	httpStatus int,
	code string,
	message string,
	status raft.Status,
) {
	writeJSON(w, httpStatus, routingErrorResponse{
		Error:    message,
		Code:     code,
		LeaderID: status.LeaderID,
		Term:     status.Term,
	})
}
