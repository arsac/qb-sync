// Package grpcutil provides shared gRPC transport constants that must be
// identical between the destination server and the streaming client.
package grpcutil

const (
	// MaxPieceDataSize is the maximum torrent piece payload in bytes.
	// Torrent piece sizes range from 256 KB to 32 MB; 32 MB is the practical maximum
	// supported by libtorrent/qBittorrent.
	MaxPieceDataSize = 32 * 1024 * 1024 // 32 MB

	// MaxGRPCMessageSize is the maximum gRPC message size for piece transfers.
	// Must exceed MaxPieceDataSize by enough to cover WritePieceRequest proto overhead
	// (torrent_hash, piece_index, offset, size, piece_hash fields add ~200 bytes).
	MaxGRPCMessageSize = MaxPieceDataSize + 4*1024 // 32 MB + 4 KB proto overhead

	// InitialStreamWindowSize is the HTTP/2 per-stream flow control window.
	// The default 64 KB window throttles bulk piece transfers.
	InitialStreamWindowSize = 16 * 1024 * 1024 // 16 MB

	// InitialConnWindowSize is the HTTP/2 connection-level flow control window.
	InitialConnWindowSize = 64 * 1024 * 1024 // 64 MB

	// BytesPerMB is the number of bytes in a megabyte (binary, 1 MiB).
	// Shared across packages for unit conversions (buffer sizes, throughput logging).
	BytesPerMB = 1024 * 1024

	// TransportBufferSize is the gRPC read/write buffer size on both client and
	// server. Default (32 KiB) fragments large pieces into many syscalls; 1 MiB
	// matches typical piece sizes and reduces syscall and TCP send-coalescing
	// overhead for bulk streaming.
	TransportBufferSize = 1 * 1024 * 1024 // 1 MiB
)
