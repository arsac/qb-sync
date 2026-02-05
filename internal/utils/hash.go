package utils

import (
	"crypto/sha1" //nolint:gosec // SHA1 is required by BitTorrent protocol for piece verification
	"encoding/hex"
	"fmt"
)

// VerifyPieceHash verifies that data matches the expected SHA1 hash.
// Returns nil if the hash matches or if expectedHash is empty (no verification needed).
// Returns an error describing the mismatch if the hashes don't match.
func VerifyPieceHash(data []byte, expectedHash string) error {
	if expectedHash == "" {
		return nil
	}

	hash := sha1.Sum(data) //nolint:gosec // Required by BitTorrent protocol
	actual := hex.EncodeToString(hash[:])
	if actual != expectedHash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", expectedHash, actual)
	}
	return nil
}

// ComputeSHA1 computes the SHA1 hash of data and returns it as a hex string.
func ComputeSHA1(data []byte) string {
	hash := sha1.Sum(data) //nolint:gosec // Required by BitTorrent protocol
	return hex.EncodeToString(hash[:])
}
