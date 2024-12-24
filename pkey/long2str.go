package pkey

// This file is generated using prim2pkey.tmpl. NEVER EDIT.

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"
)

func (w *StringKeyWriter) WriteLong(key int64) IO[string] {
	var bufh [16]byte
	var buf [8]byte
	var bufs strings.Builder
	return func(_ context.Context) (string, error) {
		bufs.Reset()
		binary.BigEndian.PutUint64(buf[:], uint64(key))
		hex.Encode(bufh[:], buf[:])
		_, _ = bufs.Write(bufh[:]) // error is always nil or OOM
		return bufs.String(), nil
	}
}
