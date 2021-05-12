package big32

import (
	"encoding/hex"
	"math/big"
)

type Big32 struct {
	Bytes [32]byte
}

var Zero *Big32 = &Big32{}
var One *Big32 = nil

func init() {
	// Initialize zero.
	for i := 0; i < len(Zero.Bytes); i++ {
		Zero.Bytes[i] = 0
	}
	// Initialize one.
	One = FromBig(big.NewInt(1))
}

func FromSlice(source []byte) *Big32 {
	b := &Big32{}
	copy(b.Bytes[:], source[:32])
	return b
}

func FromBytes(source *[32]byte) *Big32 {
	b := &Big32{}
	copy(b.Bytes[:], source[:])
	return b
}

func FromHexString(hash string) *Big32 {
	data, _ := hex.DecodeString(hash)
	return FromSlice(data)
}

func FromBig(b *big.Int) *Big32 {
	if b.Cmp(big.NewInt(0)) == 0 {
		return Zero
	} else {
		buffer := make([]byte, 32)
		bbytes := b.Bytes()
		copy(buffer[32-len(bbytes):32], bbytes)
		return FromSlice(buffer)
	}
}

func (b *Big32) ToBig() *big.Int {
	x := big.NewInt(0)
	x.SetBytes(b.Bytes[:])
	return x
}

func (b *Big32) Hex() string {
	return hex.EncodeToString(b.Bytes[:])
}

func (b *Big32) IsZero() bool {
	// Check bytes one by one.
	for i := 0; i < 32; i++ {
		if b.Bytes[i] != 0 {
			return false // This byte is not a zero.
		}
	}
	// All bytes match.
	return true
}

func (b *Big32) IsOne() bool {
	return b.Equals(One)
}

func (b *Big32) Equals(other *Big32) bool {
	// Compare bytes one by one.
	for i := 0; i < 32; i++ {
		if b.Bytes[i] != other.Bytes[i] {
			// Some byte does not match.
			return false
		}
	}
	// All bytes match.
	return true
}
