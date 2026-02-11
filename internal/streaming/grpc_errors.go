package streaming

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcStatusProvider is implemented by errors that wrap a gRPC status
// (e.g., [fmt.Errorf]("...: %%w", statusErr)). status.FromError only checks
// the outermost error, so we fall back to [errors.As] for wrapped errors.
type grpcStatusProvider interface {
	GRPCStatus() *status.Status
}

// IsTransientError returns true if the error is a transient gRPC error that may
// succeed on retry (e.g., network issues, server overload).
func IsTransientError(err error) bool {
	//nolint:exhaustive // Only specific transient codes are relevant
	switch GRPCErrorCode(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// GRPCErrorCode extracts the gRPC status code from an error, if present.
// Returns codes.Unknown if the error is not a gRPC status error.
func GRPCErrorCode(err error) codes.Code {
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	// Fall back to unwrapping wrapped gRPC errors
	var provider grpcStatusProvider
	if errors.As(err, &provider) {
		return provider.GRPCStatus().Code()
	}
	return codes.Unknown
}

// pieceKey creates a unique key for tracking in-flight pieces.
func pieceKey(hash string, index int32) string {
	return fmt.Sprintf("%s:%d", hash, index)
}

// ParsePieceKey extracts the torrent hash and piece index from a piece key.
func ParsePieceKey(key string) (string, int32, bool) {
	// Format is "hash:index"
	lastColon := strings.LastIndexByte(key, ':')
	if lastColon == -1 {
		return "", 0, false
	}

	idx, err := strconv.ParseInt(key[lastColon+1:], 10, 32)
	if err != nil {
		return "", 0, false
	}

	return key[:lastColon], int32(idx), true
}
