package main

import (
	"context"
	"fmt"
	"time"

	"github.com/KrishShah1912/shardraft/internal/benchmark"
)

const (
	leaderElectionTimeout = 15 * time.Second

	nodeRecoveryTimeout = 15 * time.Second
)

func (
	manager *benchmarkManager,
) runLeaderFailureExperiment(
	ctx context.Context,
	id string,
	cfg benchmark.Config,
	request benchmarkRequest,
) {
	leaderBefore, err :=
		manager.supervisor.findLeader()

	if err != nil {
		manager.finishExperimentFailure(
			id,
			fmt.Errorf(
				"initial leader discovery: %w",
				err,
			),
		)

		return
	}

	experiment :=
		benchmarkExperiment{
			Mode: "leader-failure",

			LeaderBefore: leaderBefore.ID,

			TermBefore: leaderBefore.Term,

			FailedNodeID: leaderBefore.ID,
		}

	manager.setExperiment(
		id,
		experiment,
	)

	type benchmarkOutcome struct {
		result benchmark.Result
		err    error
	}

	outcome :=
		make(
			chan benchmarkOutcome,
			1,
		)

	go func() {
		result, err :=
			benchmark.Run(
				ctx,
				cfg,
			)

		outcome <- benchmarkOutcome{
			result: result,

			err: err,
		}
	}()

	delay :=
		time.Duration(
			request.FailureDelayMS,
		) * time.Millisecond

	timer :=
		time.NewTimer(delay)

	defer timer.Stop()

	select {
	case <-ctx.Done():
		manager.finishExperimentFailure(
			id,
			ctx.Err(),
		)

		return

	case result :=
		<-outcome:

		/*
		 * Benchmark completed before we
		 * could inject the failure.
		 */
		if result.err != nil {
			manager.finishExperimentFailure(
				id,
				result.err,
			)
		} else {
			manager.finishExperimentFailure(
				id,
				fmt.Errorf(
					"benchmark completed before failure injection",
				),
			)
		}

		return

	case <-timer.C:
	}

	failureStarted :=
		time.Now()

	manager.updateExperiment(
		id,
		func(
			value *benchmarkExperiment,
		) {
			value.FailureStartedAt =
				failureStarted
		},
	)

	if err :=
		manager.supervisor.stop(
			leaderBefore.ID,
		); err != nil {

		manager.finishExperimentFailure(
			id,
			fmt.Errorf(
				"stop leader node %d: %w",
				leaderBefore.ID,
				err,
			),
		)

		return
	}

	leaderAfter, electedAt, err :=
		manager.waitForReplacementLeader(
			ctx,
			leaderBefore.ID,
			leaderBefore.Term,
		)

	if err != nil {
		/*
		 * Always attempt to restore the
		 * failed node before returning.
		 */
		_ =
			manager.supervisor.start(
				leaderBefore.ID,
			)

		manager.finishExperimentFailure(
			id,
			err,
		)

		return
	}

	manager.updateExperiment(
		id,
		func(
			value *benchmarkExperiment,
		) {
			value.LeaderAfter =
				leaderAfter.ID

			value.TermAfter =
				leaderAfter.Term

			value.LeaderElectedAt =
				electedAt

			value.FailoverMS =
				float64(
					electedAt.Sub(
						failureStarted,
					),
				) /
					float64(
						time.Millisecond,
					)
		},
	)

	/*
	 * Let the workload continue against
	 * the surviving quorum while the old
	 * leader remains offline.
	 */
	select {
	case <-ctx.Done():
	case result :=
		<-outcome:

		if err :=
			manager.supervisor.start(
				leaderBefore.ID,
			); err == nil {

			restartedAt :=
				time.Now()

			manager.updateExperiment(
				id,
				func(
					value *benchmarkExperiment,
				) {
					value.NodeRestartedAt =
						restartedAt
				},
			)

			recovered :=
				manager.waitForNodeRecovery(
					context.Background(),
					leaderBefore.ID,
				)

			manager.updateExperiment(
				id,
				func(
					value *benchmarkExperiment,
				) {
					value.Recovered =
						recovered
				},
			)
		}

		manager.finishExperiment(
			id,
			result.result,
			result.err,
		)

		return
	}

	_ =
		manager.supervisor.start(
			leaderBefore.ID,
		)

	manager.finishExperimentFailure(
		id,
		ctx.Err(),
	)
}

func (
	manager *benchmarkManager,
) waitForReplacementLeader(
	ctx context.Context,
	failedNodeID int,
	oldTerm uint64,
) (
	supervisorRaftStatus,
	time.Time,
	error,
) {
	deadline :=
		time.Now().Add(
			leaderElectionTimeout,
		)

	for time.Now().Before(
		deadline,
	) {
		select {
		case <-ctx.Done():
			return supervisorRaftStatus{},
				time.Time{},
				ctx.Err()

		default:
		}

		leader, err :=
			manager.supervisor.findLeader()

		if err == nil &&
			leader.ID !=
				failedNodeID &&
			leader.Term >
				oldTerm {

			return leader,
				time.Now(),
				nil
		}

		time.Sleep(
			100 *
				time.Millisecond,
		)
	}

	return supervisorRaftStatus{},
		time.Time{},
		fmt.Errorf(
			"replacement leader not elected within %s",
			leaderElectionTimeout,
		)
}

func (
	manager *benchmarkManager,
) waitForNodeRecovery(
	ctx context.Context,
	nodeID int,
) bool {
	deadline :=
		time.Now().Add(
			nodeRecoveryTimeout,
		)

	for time.Now().Before(
		deadline,
	) {
		select {
		case <-ctx.Done():
			return false

		default:
		}

		status :=
			manager.supervisor.status(
				nodeID,
			)

		if status.Running &&
			status.Reachable {

			return true
		}

		time.Sleep(
			100 *
				time.Millisecond,
		)
	}

	return false
}
