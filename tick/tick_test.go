package tick

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := Tick{TS: 42, Price: -1500, Qty: 9, Side: Sell}
	var buf [RecordSize]byte
	if err := in.EncodeInto(buf[:]); err != nil {
		t.Fatalf("EncodeInto: %v", err)
	}
	got, err := Decode(buf[:])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != in {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
}

func TestEncodeIntoShortBuffer(t *testing.T) {
	small := make([]byte, RecordSize-1)
	if err := (Tick{}).EncodeInto(small); err != ErrShortBuffer {
		t.Fatalf("err = %v, want ErrShortBuffer", err)
	}
}

func TestDecodeWrongSize(t *testing.T) {
	if _, err := Decode(make([]byte, RecordSize+1)); err != ErrBadSize {
		t.Fatalf("err = %v, want ErrBadSize", err)
	}
}

func TestSideBytes(t *testing.T) {
	var buf [RecordSize]byte
	_ = Tick{Side: Buy}.EncodeInto(buf[:])
	if buf[24] != 0 {
		t.Fatalf("Buy side byte = %d, want 0", buf[24])
	}
	_ = Tick{Side: Sell}.EncodeInto(buf[:])
	if buf[24] != 1 {
		t.Fatalf("Sell side byte = %d, want 1", buf[24])
	}
	if !bytes.Equal(buf[:24], make([]byte, 24)) {
		t.Fatalf("unexpected non-zero payload for zero tick")
	}
}
