// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package admission

import "testing"

func TestFairAdmissionLetsSingleQueueBorrowWholeBudget(t *testing.T) {
	decision := DecideFair(99, 99, 1, 100, 0)
	if !decision.Allowed {
		t.Fatalf("single active queue should borrow the full budget: %+v", decision)
	}
	if decision.Snapshot.QueueBacklog != 100 || decision.Snapshot.UpstreamBacklog != 100 {
		t.Fatalf("expected projected counts to include the admitted request: %+v", decision.Snapshot)
	}
}

func TestFairAdmissionRejectsOnlyAfterGlobalBudgetIsFull(t *testing.T) {
	decision := DecideFair(100, 100, 1, 100, 0.99)
	if decision.Allowed || decision.Reason != "upstream_queue" {
		t.Fatalf("request beyond the global budget should be rejected: %+v", decision)
	}
}

func TestFairAdmissionProtectsSmallQueueFromNoisyNeighbor(t *testing.T) {
	const maxBacklog = 100

	noisy := DecideFair(89, 94, 2, maxBacklog, 0)
	if noisy.Allowed || noisy.Reason != "queue_fair_share" {
		t.Fatalf("expected above-share queue to be probabilistically rejected under pressure: %+v", noisy)
	}
	if noisy.Snapshot.RejectionProbability <= 0 {
		t.Fatalf("expected a positive rejection probability: %+v", noisy.Snapshot)
	}

	quiet := DecideFair(5, 94, 2, maxBacklog, 0)
	if !quiet.Allowed {
		t.Fatalf("below-share queue should remain admitted: %+v", quiet)
	}
}

func TestFairAdmissionDoesNotRejectBeforePressureThreshold(t *testing.T) {
	decision := DecideFair(60, 60, 2, 100, 0)
	if !decision.Allowed || decision.Snapshot.AdmissionPressure != 0 {
		t.Fatalf("unused capacity should remain borrowable below 70%% pressure: %+v", decision)
	}
}

func TestFairAdmissionBecomesLessAggressiveAsCompetitorsLeave(t *testing.T) {
	withCompetitors := DecideFair(79, 89, 4, 100, 0.99)
	withOneCompetitor := DecideFair(79, 89, 2, 100, 0.99)
	if withCompetitors.Snapshot.RejectionProbability <= withOneCompetitor.Snapshot.RejectionProbability {
		t.Fatalf("expected more active queues to increase pressure on the oversized queue: four=%f two=%f",
			withCompetitors.Snapshot.RejectionProbability,
			withOneCompetitor.Snapshot.RejectionProbability)
	}
}
