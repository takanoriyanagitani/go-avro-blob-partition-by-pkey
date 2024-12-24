package pkey

// This file is generated using prim2pkey.tmpl. NEVER EDIT.

import "time"

import (
	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"
)

func TimeToKey(key time.Time) PrimaryKey {
	return func(wtr PrimaryKeyWriter) IO[string] {
		return wtr.WriteTime(key)
	}
}
