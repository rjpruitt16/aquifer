// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "github.com/rjpruitt16/aquifer/internal/region"

// Region discovery and the redirect helpers live in internal/region. These
// names keep the API embedding applications already use.
type (
	RegionAdapter          = region.Adapter
	NoopRegionAdapter      = region.Noop
	FlyRegionAdapter       = region.FlyAdapter
	RedirectExhaustedError = region.ExhaustedError
)

func NewFlyRegionAdapter() *FlyRegionAdapter { return region.NewFlyAdapter() }
