package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	bp "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey"
	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"

	pk "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/pkey"

	dh "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var blobSizeMax IO[int] = Bind(
	EnvValByKey("ENV_BLOB_SIZE_MAX"),
	Lift(strconv.Atoi),
).Or(Of(bp.BlobSizeMaxDefault))

var decodeConfig IO[bp.DecodeConfig] = Bind(
	blobSizeMax,
	Lift(func(i int) (bp.DecodeConfig, error) {
		return bp.DecodeConfig{BlobSizeMax: i}, nil
	}),
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return func(filename string) IO[string] {
		return func(_ context.Context) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()

			lmtd := &io.LimitedReader{
				R: f,
				N: limit,
			}

			var buf strings.Builder
			_, e = io.Copy(&buf, lmtd)
			return buf.String(), e
		}
	}
}

const SchemaFileSizeLimitDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeLimitDefault),
)

var stdin2avro2maps IO[iter.Seq2[map[string]any, error]] = Bind(
	decodeConfig,
	Lift(func(c bp.DecodeConfig) (iter.Seq2[map[string]any, error], error) {
		return dh.StdinToMaps(c), nil
	}),
)

var codec IO[bp.Codec] = Bind(
	EnvValByKey("ENV_CODEC_NAME"),
	Lift(func(codec string) (bp.Codec, error) {
		return bp.CodecFromString(codec), nil
	}),
).Or(Of(bp.CodecNull))

var encodeConfig IO[bp.EncodeConfig] = Bind(
	codec,
	Lift(func(c bp.Codec) (bp.EncodeConfig, error) {
		return bp.EncodeConfig{
			BlockLength: bp.BlockLengthDefault,
			Codec:       c,
		}, nil
	}),
)

var ecfg IO[eh.Config] = Bind(
	encodeConfig,
	func(c bp.EncodeConfig) IO[eh.Config] {
		return Bind(
			schemaContent,
			Lift(func(schema string) (eh.Config, error) {
				return eh.Config{
					Schema:       schema,
					EncodeConfig: c,
				}, nil
			}),
		)
	},
)

var fsyncType IO[eh.FsyncType] = Bind(
	EnvValByKey("ENV_FSYNC_TYPE").Or(Of("fsync")),
	Lift(eh.StringToFsyncType),
)

var dirname IO[eh.Dirname] = Bind(
	EnvValByKey("ENV_SAVE_DIRNAME_ROOT"),
	Lift(func(dname string) (eh.Dirname, error) {
		return eh.Dirname(dname), nil
	}),
)

var fscfg IO[eh.FsConfig] = Bind(
	ecfg,
	func(c eh.Config) IO[eh.FsConfig] {
		return Bind(
			fsyncType,
			func(ft eh.FsyncType) IO[eh.FsConfig] {
				return Bind(
					dirname,
					Lift(func(dn eh.Dirname) (eh.FsConfig, error) {
						return eh.FsConfig{
							Config:    c,
							FsyncType: ft,
							Dirname:   dn,
						}, nil
					}),
				)
			},
		)
	},
)

var saver IO[pk.RecordSaver] = Bind(
	fscfg,
	Lift(func(fc eh.FsConfig) (pk.RecordSaver, error) {
		return fc.SaverFromDirnameDefault(), nil
	}),
)

var primaryKeyName IO[string] = EnvValByKey("ENV_PKEY_NAME")

var map2pkey IO[pk.MapToPrimaryKey] = Bind(
	primaryKeyName,
	Lift(func(pkey string) (pk.MapToPrimaryKey, error) {
		return pk.MapToKeyNew(pkey), nil
	}),
)

var pkWriter pk.PrimaryKeyWriter = &pk.StringKeyWriterDefault

var stdin2avro2maps2partitioned IO[Void] = Bind(
	stdin2avro2maps,
	func(m iter.Seq2[map[string]any, error]) IO[Void] {
		return Bind(
			map2pkey,
			func(mp pk.MapToPrimaryKey) IO[Void] {
				return Bind(
					saver,
					func(rs pk.RecordSaver) IO[Void] {
						return rs.SaveAll(
							m,
							mp,
							pkWriter,
						)
					},
				)
			},
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2avro2maps2partitioned(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
