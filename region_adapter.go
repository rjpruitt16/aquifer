package aquifer

// RegionAdapter reports which regions Aquifer is currently deployed to and
// reachable in, backing /proxy's cross-region redirect (see proxy.go's
// AttemptDirect). Same shape as MetricsAdapter/JobStore: a small interface,
// a no-op default, the deployer plugs in a real implementation for their
// platform. Off unless explicitly configured -- matches every other
// opt-in feature in this codebase.
type RegionAdapter interface {
	// LiveRegions returns the currently known-live regions, excluding
	// SelfRegion. Safe to call frequently; implementations should return a
	// cached snapshot, not block on a live check.
	LiveRegions() []string
	// SelfRegion returns this instance's own region identifier, or "" if
	// unknown/not applicable.
	SelfRegion() string
}

// NoopRegionAdapter is the default: no known regions, cross-region redirect
// never triggers. This is what "the feature is off" looks like -- AttemptDirect
// checks LiveRegions() being empty as its gate for even considering redirect,
// so a deployment that never configures a real RegionAdapter sees zero
// behavior change from this feature existing in the codebase.
type NoopRegionAdapter struct{}

func (NoopRegionAdapter) LiveRegions() []string { return nil }
func (NoopRegionAdapter) SelfRegion() string     { return "" }

func ensureRegionAdapter(adapter RegionAdapter) RegionAdapter {
	if adapter == nil {
		return NoopRegionAdapter{}
	}
	return adapter
}
