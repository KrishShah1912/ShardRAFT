package main

import "testing"

func TestSupervisorRaftStatusFields(
	t *testing.T,
) {
	status := supervisorRaftStatus{
		ID:           1,
		Role:         2,
		Term:         7,
		LeaderID:     1,
		CommitIndex:  100,
		AppliedIndex: 100,
	}

	if status.ID != 1 {
		t.Fatal(
			"unexpected node ID",
		)
	}

	if status.Role != 2 {
		t.Fatal(
			"unexpected role",
		)
	}

	if status.Term != 7 {
		t.Fatal(
			"unexpected term",
		)
	}
}
