package destination

import "github.com/bits-and-blooms/bitset"

// boolSliceToBitSet converts a []bool to a *bitset.BitSet.
// Used by buildWrittenBitmap to merge calculatePiecesCovered results via InPlaceUnion.
func boolSliceToBitSet(bs []bool) *bitset.BitSet {
	b := bitset.New(uint(len(bs)))
	for i, v := range bs {
		if v {
			b.Set(uint(i))
		}
	}
	return b
}

// ensureBitSetLength extends a bitset to at least n bits.
// After UnmarshalBinary the bitset may be shorter than expected
// if the persisted state was written with fewer pieces.
func ensureBitSetLength(bs *bitset.BitSet, n uint) *bitset.BitSet {
	if bs.Len() >= n {
		return bs
	}
	grown := bitset.New(n)
	grown.InPlaceUnion(bs)
	return grown
}
