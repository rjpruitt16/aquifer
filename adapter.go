// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "context"

type FrameworkAdapter interface {
	Name() string
	Start(ctx context.Context, aquifer *Aquifer) error
}
