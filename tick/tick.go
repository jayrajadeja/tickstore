// Package tick defines the fixed-width binary tick record persisted by the
// store and streamed over the ingest pipe.
package tick

import (
	"encoding/binary"
	"errors"
)

// Side is the aggressor side of a trade.
type Side uint8

const (
	Buy  Side = 0
	Sell Side = 1
)

// RecordSize is the fixed on-disk / on-wire size of one encoded Tick, in bytes.
const RecordSize = 25

// Tick is a single execution stored by the tick store. TS is a logical
// (non-decreasing) sequence timestamp; Price is in integer ticks.
type Tick struct {
	TS    int64
	Price int64
	Qty   uint64
	Side  Side
}

// Sentinel errors.
var (
	ErrShortBuffer = errors.New("tick: buffer smaller than RecordSize")
	ErrBadSize     = errors.New("tick: buffer length must equal RecordSize")
)

// EncodeInto writes t into buf (len must be >= RecordSize), little-endian.
func (t Tick) EncodeInto(buf []byte) error {
	if len(buf) < RecordSize {
		return ErrShortBuffer
	}
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t.TS))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(t.Price))
	binary.LittleEndian.PutUint64(buf[16:24], t.Qty)
	buf[24] = byte(t.Side)
	return nil
}

// Decode reads exactly RecordSize bytes from buf into a Tick.
func Decode(buf []byte) (Tick, error) {
	if len(buf) != RecordSize {
		return Tick{}, ErrBadSize
	}
	return Tick{
		TS:    int64(binary.LittleEndian.Uint64(buf[0:8])),
		Price: int64(binary.LittleEndian.Uint64(buf[8:16])),
		Qty:   binary.LittleEndian.Uint64(buf[16:24]),
		Side:  Side(buf[24]),
	}, nil
}
