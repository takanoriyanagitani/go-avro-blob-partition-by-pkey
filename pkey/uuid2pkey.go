package pkey

// This file is generated using prim2pkey.tmpl. NEVER EDIT.

import (
	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"
)

func UuidToKey(key [16]byte) PrimaryKey {
	return func(wtr PrimaryKeyWriter) IO[string] {
		return wtr.WriteUuid(key)
	}
}
