package aquifer

import "context"

type FrameworkAdapter interface {
	Name() string
	Start(ctx context.Context, aquifer *Aquifer) error
}
