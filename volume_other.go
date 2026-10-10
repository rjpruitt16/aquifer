//go:build !unix

package aquifer

// volumeAvailableBytes isn't implemented off Unix; the DB ceiling falls back
// to defaultDBMaxBytes there.
func volumeAvailableBytes(string) (int64, bool) { return 0, false }
