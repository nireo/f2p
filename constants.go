package f2p

import "errors"

var (
	errNodeNotInitialized      = errors.New("node is missing routing information")
	errTransportNotInitialized = errors.New("node transport is not initialized")
	errContentNameRequired     = errors.New("content name is required")
	errNilContentReader        = errors.New("content reader is nil")
	errChunkNotFound           = errors.New("chunk not found")
	errManifestNotFound        = errors.New("content manifest not found")
	errInvalidManifest         = errors.New("invalid content manifest")
)

const (
	parameterK = 20
	alpha      = 3

	defaultChunkSize       = 256 * 1024 // 256kb
	contentManifestVersion = 1
)
