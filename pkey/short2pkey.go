package pkey

// This file is generated using prim2pkey.tmpl. NEVER EDIT.

import (
	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"
)

func ShortToKey(key int16) PrimaryKey {
	return func(wtr PrimaryKeyWriter) IO[string] {
		return wtr.WriteShort(key)
	}
}
