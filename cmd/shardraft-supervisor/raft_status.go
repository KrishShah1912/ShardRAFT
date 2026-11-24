package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type supervisorRaftStatus struct {
	ID int `json:"id"`

	Role int    `json:"role"`
	Term uint64 `json:"term"`

	LeaderID int `json:"leader_id"`

	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
}

func (
	s *supervisor,
) raftStatus(
	id int,
) (supervisorRaftStatus, error) {
	if id < 1 || id > 3 {
		return supervisorRaftStatus{},
			fmt.Errorf(
				"invalid node id %d",
				id,
			)
	}

	url :=
		fmt.Sprintf(
			"http://127.0.0.1:%d/api/v1/admin/raft",
			8080+id,
		)

	response, err :=
		s.httpClient.Get(url)

	if err != nil {
		return supervisorRaftStatus{},
			err
	}

	defer response.Body.Close()

	if response.StatusCode !=
		http.StatusOK {

		return supervisorRaftStatus{},
			fmt.Errorf(
				"node %d raft status returned HTTP %d",
				id,
				response.StatusCode,
			)
	}

	var status supervisorRaftStatus

	if err :=
		json.NewDecoder(
			response.Body,
		).Decode(
			&status,
		); err != nil {

		return supervisorRaftStatus{},
			err
	}

	return status, nil
}

func (
	s *supervisor,
) findLeader() (
	supervisorRaftStatus,
	error,
) {
	for id := 1; id <= 3; id++ {
		status, err :=
			s.raftStatus(id)

		if err != nil {
			continue
		}

		/*
		 * Existing Raft API uses:
		 * 0 follower
		 * 1 candidate
		 * 2 leader
		 */
		if status.Role == 2 {
			return status, nil
		}
	}

	return supervisorRaftStatus{},
		fmt.Errorf(
			"no reachable Raft leader",
		)
}
