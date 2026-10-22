// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "github.com/rjpruitt16/aquifer/internal/admission"

// Admission control, fair admission and the inbound rate controller live in
// internal/admission. These names keep the API embedding applications
// already use.
type (
	AdmissionLimits        = admission.Limits
	AdmissionDecision      = admission.Decision
	AdmissionRejectedError = admission.RejectedError
	AdmissionController    = admission.Controller
	QueueSnapshot          = admission.QueueSnapshot
	FairAdmissionDecision  = admission.FairDecision
	InboundRateController  = admission.InboundRateController
	InboundRateSnapshot    = admission.InboundRateSnapshot
)

func LoadAdmissionLimits() AdmissionLimits { return admission.LoadLimits() }

func NewAdmissionController(limits AdmissionLimits, dbPath string) *AdmissionController {
	return admission.NewController(limits, dbPath)
}

func NewInboundRateController() *InboundRateController { return admission.NewInboundRateController() }
