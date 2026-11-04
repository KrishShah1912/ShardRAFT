package raft

import "testing"

func findAppendRequest(t *testing.T, node *Raft, peer NodeID, lastIndex uint64) Message {
	t.Helper()

	for _, msg := range node.Ready().Messages {
		if msg.Type != MsgAppend || msg.To != peer {
			continue
		}

		endIndex := msg.Index + uint64(len(msg.Entries))

		if endIndex == lastIndex {
			if msg.RequestID == 0 {
				t.Fatal("AppendEntries request has no RequestID")
			}
			return msg
		}
	}

	t.Fatalf("no AppendEntries request found for peer %d ending at index %d", peer, lastIndex)
	return Message{}
}

func TestLeaderProposesCommand(t *testing.T) {
	node := NewRaft(1, nil, 5)

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if node.Status().Role != Leader {
		t.Fatal("expected node to become leader")
	}

	command := []byte("SET x=10")

	if err := node.Propose(command); err != nil {
		t.Fatalf("proposal failed: %v", err)
	}

	if len(node.log) == 0 {
		t.Fatal("expected a log entry")
	}

	last := node.log[len(node.log)-1]

	if string(last.Data) != string(command) {
		t.Fatalf("expected %q, got %q", command, last.Data)
	}

	if last.Term != node.hard.Term {
		t.Fatal("entry term must match leader term")
	}

	if last.Type != EntryNormal {
		t.Fatal("expected normal command entry")
	}
}

func TestLeaderSendsAppendEntries(t *testing.T) {
	node := NewRaft(1, []NodeID{2, 3}, 5)

	// Start an election.
	for i := 0; i < 5; i++ {
		node.Tick()
	}

	// Node 2 grants its vote, giving Node 1 a majority.
	err := node.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != Leader {
		t.Fatal("expected node 1 to become leader")
	}

	// Clear messages generated during the election.
	node.Advance()

	// Propose a command.
	if err := node.Propose([]byte("SET x=10")); err != nil {
		t.Fatal(err)
	}

	ready := node.Ready()

	count := 0

	for _, msg := range ready.Messages {
		if msg.Type != MsgAppend {
			continue
		}

		count++

		if msg.From != 1 {
			t.Fatalf("unexpected sender: %d", msg.From)
		}

		if msg.Index != 0 || msg.LogTerm != 0 {
			t.Fatalf("unexpected previous log position: %+v", msg)
		}

		if len(msg.Entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(msg.Entries))
		}

		if msg.Entries[0].Type != EntryNoop {
			t.Fatal("expected first entry to be leader no-op")
		}

		if msg.Entries[1].Type != EntryNormal {
			t.Fatal("expected second entry to be a normal command")
		}

		if msg.Entries[0].Index != 1 || msg.Entries[1].Index != 2 {
			t.Fatal("unexpected entry indexes")
		}

		if string(msg.Entries[1].Data) != "SET x=10" {
			t.Fatalf("unexpected command: %s", msg.Entries[1].Data)
		}
	}

	if count != 2 {
		t.Fatalf("expected 2 AppendEntries messages, got %d", count)
	}
}

func TestFollowerAcceptsAppendEntries(t *testing.T) {
	follower := NewRaft(2, []NodeID{1, 3}, 8)

	err := follower.Step(Message{
		Type:    MsgAppend,
		From:    1,
		To:      2,
		Term:    1,
		Index:   0,
		LogTerm: 0,
		Entries: []Entry{
			{
				Index: 1,
				Term:  1,
				Type:  EntryNormal,
				Data:  []byte("SET x=10"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(follower.log) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(follower.log))
	}

	if string(follower.log[0].Data) != "SET x=10" {
		t.Fatal("follower stored incorrect command")
	}

	ready := follower.Ready()

	if len(ready.Messages) != 1 ||
		ready.Messages[0].Reject {
		t.Fatal("expected successful AppendEntries response")
	}
}

func TestDuplicateAppendPreservesSuffix(t *testing.T) {
	follower := NewRaft(2, []NodeID{1, 3}, 8)

	follower.log = []Entry{
		{Index: 1, Term: 1, Data: []byte("A")},
		{Index: 2, Term: 1, Data: []byte("B")},
		{Index: 3, Term: 2, Data: []byte("C")},
	}

	follower.hard.Term = 2

	// A delayed message repeats an existing entry.
	err := follower.Step(Message{
		Type:    MsgAppend,
		From:    1,
		To:      2,
		Term:    2,
		Index:   1,
		LogTerm: 1,
		Entries: []Entry{
			{Index: 2, Term: 1, Data: []byte("B")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(follower.log) != 3 {
		t.Fatalf(
			"duplicate append erased suffix: got %d entries",
			len(follower.log),
		)
	}

	if string(follower.log[2].Data) != "C" {
		t.Fatal("valid suffix was modified")
	}
}

func TestLeaderTracksFollowerAcknowledgment(t *testing.T) {
	node := NewRaft(1, []NodeID{2, 3}, 5)

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := node.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != Leader {
		t.Fatal("expected node 1 to become leader")
	}

	node.Advance()

	if err := node.Propose([]byte("SET x=10")); err != nil {
		t.Fatal(err)
	}

	// The leader has a no-op at index 1
	// and the proposed command at index 2.
	request := findAppendRequest(t, node, 2, 2)

	if err := node.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      1,
		Index:     2,
		RequestID: request.RequestID,
	}); err != nil {
		t.Fatal(err)
	}

	if node.matchIndex[2] != 2 {
		t.Fatalf(
			"expected matchIndex[2]=2, got %d",
			node.matchIndex[2],
		)
	}

	if node.nextIndex[2] != 3 {
		t.Fatalf(
			"expected nextIndex[2]=3, got %d",
			node.nextIndex[2],
		)
	}

	if node.matchIndex[3] != 0 {
		t.Fatal("node 3 should not have acknowledged the entry")
	}
}

func TestLeaderCommitsAfterMajority(t *testing.T) {
	node := NewRaft(1, []NodeID{2, 3}, 5)

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := node.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != Leader {
		t.Fatal("expected node 1 to become leader")
	}

	node.Advance()

	if err := node.Propose([]byte("SET x=10")); err != nil {
		t.Fatal(err)
	}

	// Leader alone is not a majority.
	if node.Status().Commit != 0 {
		t.Fatal("entry committed before majority")
	}

	// Node 2 acknowledges both entries.
	request := findAppendRequest(t, node, 2, 2)

	if err := node.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      1,
		Index:     2,
		RequestID: request.RequestID,
	}); err != nil {
		t.Fatal(err)
	}

	// The no-op and command should both be committed.
	if node.Status().Commit != 2 {
		t.Fatalf(
			"expected commit index 2, got %d",
			node.Status().Commit,
		)
	}
}

func TestLeaderDoesNotDirectlyCommitOldTermEntry(t *testing.T) {
	node := NewRaft(1, []NodeID{2, 3}, 5)

	// An entry exists from an earlier term.
	node.log = []Entry{
		{
			Index: 1,
			Term:  1,
			Type:  EntryNormal,
			Data:  []byte("SET x=10"),
		},
	}

	node.hard.Term = 1

	// Start an election for term 2.
	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := node.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: 2,
	}); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != Leader {
		t.Fatal("expected leader")
	}

	// The new leader has appended a term-2 no-op
	// at index 2.
	if len(node.log) != 2 ||
		node.log[1].Type != EntryNoop {
		t.Fatal("expected leader no-op at index 2")
	}

	// Simulate follower 2 having replicated
	// only the old-term entry.
	node.matchIndex[2] = 1
	node.nextIndex[2] = 2

	node.maybeCommit()

	// An old-term entry cannot be committed directly.
	if node.Status().Commit != 0 {
		t.Fatalf(
			"old-term entry committed incorrectly: %d",
			node.Status().Commit,
		)
	}

	node.Advance()

	// Propose a new entry in term 2 at index 3.
	if err := node.Propose([]byte("SET y=20")); err != nil {
		t.Fatal(err)
	}

	// Acknowledge the actual request covering index 3.
	request := findAppendRequest(t, node, 2, 3)

	if err := node.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      2,
		Index:     3,
		RequestID: request.RequestID,
	}); err != nil {
		t.Fatal(err)
	}

	// Committing a current-term entry also commits
	// the preceding old-term entry.
	if node.Status().Commit != 3 {
		t.Fatalf(
			"expected commit index 3, got %d",
			node.Status().Commit,
		)
	}
}

func TestLeaderRetriesRejectedAppend(t *testing.T) {
	node := NewRaft(1, []NodeID{2, 3}, 5)

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := node.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != Leader {
		t.Fatal("expected leader")
	}

	node.Advance()

	if err := node.Propose([]byte("SET x=10")); err != nil {
		t.Fatal(err)
	}

	// Simulate the leader trying to replicate
	// starting after index 1.
	node.nextIndex[2] = 2

	// Clear earlier messages and send a fresh request.
	node.Advance()
	node.sendAppend(2)

	request := findAppendRequest(t, node, 2, 2)

	if request.Index != 1 {
		t.Fatalf(
			"expected previous log index 1, got %d",
			request.Index,
		)
	}

	// Follower rejects because it does not
	// have the required previous log entry.
	if err := node.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      1,
		Index:     request.Index,
		Reject:    true,
		RequestID: request.RequestID,
	}); err != nil {
		t.Fatal(err)
	}

	// Leader should move nextIndex backward.
	if node.nextIndex[2] != 1 {
		t.Fatalf(
			"expected nextIndex[2]=1, got %d",
			node.nextIndex[2],
		)
	}

	// Leader should send a retry starting at index 1.
	foundRetry := false

	for _, msg := range node.Ready().Messages {
		if msg.Type == MsgAppend &&
			msg.To == 2 &&
			msg.RequestID != request.RequestID &&
			msg.Index == 0 {
			foundRetry = true
		}
	}

	if !foundRetry {
		t.Fatal("expected AppendEntries retry")
	}
}

func TestLaggingFollowerCatchesUp(t *testing.T) {
	leader := NewRaft(1, []NodeID{2, 3}, 5)
	follower := NewRaft(2, []NodeID{1, 3}, 8)

	// Leader has three entries from term 1.
	leader.hard.Term = 1
	leader.log = []Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("A")},
		{Index: 2, Term: 1, Type: EntryNormal, Data: []byte("B")},
		{Index: 3, Term: 1, Type: EntryNormal, Data: []byte("C")},
	}

	// Follower has only the first entry.
	follower.hard.Term = 1
	follower.log = []Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("A")},
	}

	// Becoming leader appends a no-op at index 4.
	leader.becomeLeader()

	if len(leader.log) != 4 {
		t.Fatalf("expected leader to have 4 entries, got %d", len(leader.log))
	}

	if leader.nextIndex[2] != 4 {
		t.Fatalf("expected nextIndex[2]=4, got %d", leader.nextIndex[2])
	}

	// Deliver AppendEntries requests and responses,
	// including automatic retries generated by the leader.
	for attempt := 0; attempt < 20; attempt++ {
		// Collect outgoing messages from the leader.
		messages := leader.Ready().Messages
		leader.Advance()

		if len(messages) == 0 {
			// If no messages are queued, explicitly send
			// an AppendEntries request to follower 2.
			leader.sendAppend(2)
			messages = leader.Ready().Messages
			leader.Advance()
		}

		// Deliver AppendEntries to follower 2.
		for _, msg := range messages {
			if msg.Type != MsgAppend || msg.To != 2 {
				continue
			}

			if err := follower.Step(msg); err != nil {
				t.Fatal(err)
			}
		}

		// Collect follower responses.
		responses := follower.Ready().Messages

		for _, response := range responses {
			t.Logf(
				"Follower response: requestID=%d reject=%v index=%d",
				response.RequestID,
				response.Reject,
				response.Index,
			)
		}

		follower.Advance()

		// Deliver responses back to the leader.
		for _, response := range responses {
			if response.Type != MsgAppendResp {
				continue
			}

			if err := leader.Step(response); err != nil {
				t.Fatal(err)
			}
		}

		// Stop when the follower catches up.
		if len(follower.log) == 4 {
			break
		}
	}

	if len(follower.log) != 4 {
		t.Fatalf(
			"expected follower to catch up to 4 entries, got %d",
			len(follower.log),
		)
	}

	// Verify every replicated entry matches the leader.
	for i := range leader.log {
		if follower.log[i].Index != leader.log[i].Index ||
			follower.log[i].Term != leader.log[i].Term ||
			follower.log[i].Type != leader.log[i].Type ||
			string(follower.log[i].Data) != string(leader.log[i].Data) {
			t.Fatalf("log mismatch at index %d", i+1)
		}
	}

	if follower.log[3].Type != EntryNoop {
		t.Fatal("expected no-op entry at index 4")
	}

	// Verify the leader tracked follower progress.
	if leader.matchIndex[2] != 4 {
		t.Fatalf(
			"expected matchIndex[2]=4, got %d",
			leader.matchIndex[2],
		)
	}
}

func TestMalformedAppendDoesNotModifyLog(t *testing.T) {
	follower := NewRaft(2, []NodeID{1, 3}, 8)

	follower.hard.Term = 1
	follower.log = []Entry{
		{Index: 1, Term: 1, Data: []byte("A")},
		{Index: 2, Term: 1, Data: []byte("B")},
	}

	// Incoming request contains an invalid index.
	err := follower.Step(Message{
		Type:    MsgAppend,
		From:    1,
		To:      2,
		Term:    1,
		Index:   1,
		LogTerm: 1,
		Entries: []Entry{
			{Index: 2, Term: 2, Data: []byte("X")},
			{Index: 99, Term: 2, Data: []byte("Y")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The malformed request must be rejected.
	responses := follower.Ready().Messages

	if len(responses) != 1 || !responses[0].Reject {
		t.Fatal("expected malformed AppendEntries to be rejected")
	}

	// The original log must remain unchanged.
	if len(follower.log) != 2 {
		t.Fatalf(
			"malformed request changed log length: %d",
			len(follower.log),
		)
	}

	if string(follower.log[1].Data) != "B" {
		t.Fatal("malformed request modified existing entry")
	}
}

func TestFollowerRejectsCommittedEntryOverwrite(t *testing.T) {
	follower := NewRaft(2, []NodeID{1, 3}, 8)

	follower.hard.Term = 2
	follower.hard.Commit = 2

	follower.log = []Entry{
		{Index: 1, Term: 1, Data: []byte("A")},
		{Index: 2, Term: 1, Data: []byte("B")},
		{Index: 3, Term: 2, Data: []byte("C")},
	}

	// A conflicting request tries to overwrite
	// the committed entry at index 2.
	err := follower.Step(Message{
		Type:    MsgAppend,
		From:    1,
		To:      2,
		Term:    2,
		Index:   1,
		LogTerm: 1,
		Entries: []Entry{
			{Index: 2, Term: 2, Data: []byte("X")},
			{Index: 3, Term: 2, Data: []byte("Y")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The follower must reject the conflicting request.
	responses := follower.Ready().Messages

	if len(responses) != 1 || !responses[0].Reject {
		t.Fatal("expected rejection of committed entry overwrite")
	}

	// The log must remain unchanged.
	if len(follower.log) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(follower.log))
	}

	if string(follower.log[1].Data) != "B" {
		t.Fatal("committed entry at index 2 was modified")
	}

	if string(follower.log[2].Data) != "C" {
		t.Fatal("entry at index 3 was unexpectedly modified")
	}

	if follower.hard.Commit != 2 {
		t.Fatal("commit index changed unexpectedly")
	}
}

func TestDelayedRejectionDoesNotMoveProgressBackward(t *testing.T) {
	leader := NewRaft(1, []NodeID{2, 3}, 5)

	leader.hard.Term = 1
	leader.log = []Entry{
		{Index: 1, Term: 1, Data: []byte("A")},
		{Index: 2, Term: 1, Data: []byte("B")},
		{Index: 3, Term: 1, Data: []byte("C")},
	}

	// Becoming leader adds a no-op at index 4.
	leader.becomeLeader()
	leader.Advance()

	// First, send a request starting at index 4.
	leader.nextIndex[2] = 4
	leader.sendAppend(2)

	oldRequest := findAppendRequest(t, leader, 2, 4)

	if oldRequest.Index != 3 {
		t.Fatalf(
			"expected old request prevIndex 3, got %d",
			oldRequest.Index,
		)
	}

	// Send a newer request containing all entries.
	leader.Advance()
	leader.nextIndex[2] = 1
	leader.sendAppend(2)

	newRequest := findAppendRequest(t, leader, 2, 4)

	if newRequest.Index != 0 {
		t.Fatalf(
			"expected new request prevIndex 0, got %d",
			newRequest.Index,
		)
	}

	// The newer request succeeds.
	if err := leader.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      1,
		Index:     4,
		RequestID: newRequest.RequestID,
	}); err != nil {
		t.Fatal(err)
	}

	if leader.matchIndex[2] != 4 {
		t.Fatalf(
			"expected matchIndex 4, got %d",
			leader.matchIndex[2],
		)
	}

	if leader.nextIndex[2] != 5 {
		t.Fatalf(
			"expected nextIndex 5, got %d",
			leader.nextIndex[2],
		)
	}

	// The older request is rejected after the
	// newer request has already succeeded.
	if err := leader.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      1,
		Index:     oldRequest.Index,
		Reject:    true,
		RequestID: oldRequest.RequestID,
	}); err != nil {
		t.Fatal(err)
	}

	// A delayed rejection must not undo progress.
	if leader.matchIndex[2] != 4 {
		t.Fatalf(
			"delayed rejection changed matchIndex to %d",
			leader.matchIndex[2],
		)
	}

	if leader.nextIndex[2] != 5 {
		t.Fatalf(
			"delayed rejection moved nextIndex backward: %d",
			leader.nextIndex[2],
		)
	}
}

func TestLeaderCommitsInheritedEntryThroughNoop(t *testing.T) {
	node := NewRaft(1, []NodeID{2, 3}, 5)

	// An uncommitted entry exists from term 1.
	node.hard.Term = 1
	node.log = []Entry{
		{
			Index: 1,
			Term:  1,
			Type:  EntryNormal,
			Data:  []byte("SET x=10"),
		},
	}

	// Start an election for term 2.
	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := node.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: 2,
	}); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != Leader {
		t.Fatal("expected node to become leader")
	}

	// The leader should append a term-2 no-op.
	if len(node.log) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(node.log))
	}

	if node.log[1].Type != EntryNoop ||
		node.log[1].Term != 2 ||
		node.log[1].Index != 2 {
		t.Fatal("expected term-2 no-op at index 2")
	}

	// Election alone must not commit the entries.
	if node.Status().Commit != 0 {
		t.Fatal("entries committed before majority replication")
	}

	// Simulate follower 2 having replicated only
	// the inherited entry from term 1.
	node.matchIndex[2] = 1
	node.nextIndex[2] = 2

	// Check whether the old-term entry can be committed.
	node.maybeCommit()

	// The old-term entry must NOT be committed directly.
	if node.Status().Commit != 0 {
		t.Fatal("old-term entry committed directly")
	}

	// Clear previous outgoing messages.
	node.Advance()

	// Send AppendEntries containing the term-2 no-op.
	node.sendAppend(2)

	// Find the actual request sent to follower 2.
	request := findAppendRequest(t, node, 2, 2)

	// Simulate follower 2 acknowledging the term-2 no-op.
	if err := node.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      2,
		Index:     2,
		RequestID: request.RequestID,
	}); err != nil {
		t.Fatal(err)
	}

	// Committing the term-2 no-op also commits
	// the inherited term-1 entry.
	if node.Status().Commit != 2 {
		t.Fatalf(
			"expected commit index 2, got %d",
			node.Status().Commit,
		)
	}
}

func TestFollowerCommitIndexNeverDecreases(t *testing.T) {
	follower := NewRaft(2, []NodeID{1, 3}, 8)

	follower.hard.Term = 2
	follower.hard.Commit = 3
	follower.volatile.CommitIndex = 3

	follower.log = []Entry{
		{Index: 1, Term: 1, Data: []byte("A")},
		{Index: 2, Term: 1, Data: []byte("B")},
		{Index: 3, Term: 2, Data: []byte("C")},
		{Index: 4, Term: 2, Data: []byte("D")},
	}

	// An older, shorter AppendEntries message arrives.
	// Its previous index is 1, and it repeats entry 2.
	err := follower.Step(Message{
		Type:    MsgAppend,
		From:    1,
		To:      2,
		Term:    2,
		Index:   1,
		LogTerm: 1,
		Commit:  4,
		Entries: []Entry{
			{Index: 2, Term: 1, Data: []byte("B")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if follower.hard.Commit != 3 {
		t.Fatalf(
			"commit index decreased from 3 to %d",
			follower.hard.Commit,
		)
	}

	if follower.volatile.CommitIndex != 3 {
		t.Fatalf(
			"volatile commit index decreased to %d",
			follower.volatile.CommitIndex,
		)
	}

	if len(follower.log) != 4 {
		t.Fatal("delayed AppendEntries modified the log")
	}
}

func TestFollowerLearnsCommitThroughHeartbeat(t *testing.T) {
	leader := NewRaft(1, []NodeID{2, 3}, 5)
	follower := NewRaft(2, []NodeID{1, 3}, 8)

	// Both nodes have the same log.
	leader.hard.Term = 1
	follower.hard.Term = 1

	leader.log = []Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("A")},
	}
	follower.log = []Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("A")},
	}

	leader.becomeLeader()
	leader.Advance()

	// Simulate the leader having committed the first entry.
	leader.hard.Commit = 1
	leader.volatile.CommitIndex = 1

	// Leader has confirmed follower 2 stores entry 1.
	leader.matchIndex[2] = 1

	// Trigger the leader's periodic heartbeat.
	leader.Tick()
	leader.Tick()

	// Just to check the logs leader.matchIndex[2] = 1
	// for _, msg := range leader.Ready().Messages {
	// 	t.Logf(
	// 		"heartbeat: type=%v to=%d term=%d index=%d logTerm=%d commit=%d",
	// 		msg.Type, msg.To, msg.Term, msg.Index, msg.LogTerm, msg.Commit,
	// 	)
	// }

	// t.Logf(
	// 	"follower: term=%d logLength=%d termAt1=%d commit=%d",
	// 	follower.hard.Term,
	// 	len(follower.log),
	// 	follower.termAt(1),
	// 	follower.hard.Commit,
	// )

	found := false

	for _, msg := range leader.Ready().Messages {
		if msg.Type != MsgHeartbeat || msg.To != 2 {
			continue
		}

		found = true

		if err := follower.Step(msg); err != nil {
			t.Fatal(err)
		}
	}

	if !found {
		t.Fatal("expected heartbeat to follower 2")
	}

	if follower.hard.Commit != 1 {
		t.Fatalf(
			"expected follower commit index 1, got %d",
			follower.hard.Commit,
		)
	}
}

func TestHeartbeatDoesNotCommitConflictingLog(t *testing.T) {
	follower := NewRaft(2, []NodeID{1, 3}, 8)

	follower.hard.Term = 2

	// Follower has an old, uncommitted entry.
	follower.log = []Entry{
		{
			Index: 1,
			Term:  1,
			Type:  EntryNormal,
			Data:  []byte("SET x=99"),
		},
	}

	// The leader has committed through index 1,
	// but its log differs from the follower's log.
	err := follower.Step(Message{
		Type:    MsgHeartbeat,
		From:    1,
		To:      2,
		Term:    2,
		Index:   1,
		LogTerm: 2,
		Commit:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A heartbeat must not commit an entry unless
	// the corresponding log prefix is verified.
	if follower.hard.Commit != 0 {
		t.Fatalf(
			"heartbeat committed conflicting entry: commit=%d",
			follower.hard.Commit,
		)
	}

	if follower.volatile.CommitIndex != 0 {
		t.Fatalf(
			"volatile commit index advanced incorrectly: %d",
			follower.volatile.CommitIndex,
		)
	}

	if follower.log[0].Term != 1 {
		t.Fatal("heartbeat unexpectedly modified the log")
	}
}

func TestUnsentAppendResponseDoesNotAdvanceMatchIndex(t *testing.T) {
	leader := NewRaft(1, []NodeID{2, 3}, 5)

	leader.hard.Term = 1
	leader.log = []Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("A")},
	}

	// Becoming leader appends a term-1 no-op at index 2.
	leader.becomeLeader()
	leader.Advance()

	// Propose another entry at index 3.
	if err := leader.Propose([]byte("B")); err != nil {
		t.Fatal(err)
	}

	// Discard outgoing messages without delivering them.
	// Follower 2 has not acknowledged any entry.
	leader.Advance()

	// Pretend follower 2 acknowledges index 3.
	// This response was never actually produced by
	// a follower, so it must not be trusted.
	err := leader.Step(Message{
		Type:  MsgAppendResp,
		From:  2,
		To:    1,
		Term:  1,
		Index: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if leader.matchIndex[2] != 0 {
		t.Fatalf(
			"unsolicited response advanced matchIndex to %d",
			leader.matchIndex[2],
		)
	}

	if leader.hard.Commit != 0 {
		t.Fatalf(
			"unsolicited response advanced commit index to %d",
			leader.hard.Commit,
		)
	}
}
