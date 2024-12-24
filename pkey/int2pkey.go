package pkey

// This file is generated using prim2pkey.tmpl. NEVER EDIT.

import (
	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"
)

func IntToKey(key int32) PrimaryKey {
	return func(wtr PrimaryKeyWriter) IO[string] {
		return wtr.WriteInt(key)
	}
}
