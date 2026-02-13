package streaming

// This file contains test helpers that must be accessible from external
// packages (e.g., internal/source tests). Go _test.go files are only visible
// within their own package's test scope, so these live in a regular file.
// The cost is negligible: one small function in an internal package.

import pb "github.com/arsac/qb-sync/proto"

// AddTestState adds a minimal torrent state for testing from external packages.
// This allows tests in other packages (e.g., source) to set up PieceMonitor state
// without needing a full PieceSource and polling cycle.
//
// DO NOT USE IN PRODUCTION CODE.
func (t *PieceMonitor) AddTestState(hash string, numPieces int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.torrents[hash] = &torrentState{
		meta: &TorrentMetadata{
			InitTorrentRequest: &pb.InitTorrentRequest{
				TorrentHash: hash,
				NumPieces:   int32(numPieces),
			},
		},
		streamed: make([]bool, numPieces),
		failed:   make([]bool, numPieces),
	}
}
