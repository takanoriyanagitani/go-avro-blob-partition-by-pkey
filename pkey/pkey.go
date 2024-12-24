package pkey

import (
	"context"
	"encoding/hex"
	"errors"
	"iter"
	"strings"
	"time"

	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"
)

var (
	ErrInvalidKey error = errors.New("invalid key")
)

type PrimaryKeyWriter interface {
	WriteShort(int16) IO[string]
	WriteInt(int32) IO[string]
	WriteLong(int64) IO[string]
	WriteTime(time.Time) IO[string]
	WriteUuid([16]byte) IO[string]
}

//go:generate go run internal/gen/primitive2pkey/main.go Short int16
//go:generate go run internal/gen/primitive2pkey/main.go Int int32
//go:generate go run internal/gen/primitive2pkey/main.go Long int64
//go:generate go run internal/gen/primitive2pkey/main.go Time time.Time
//go:generate go run internal/gen/primitive2pkey/main.go Uuid [16]byte
//go:generate gofmt -s -w .
type PrimaryKey func(PrimaryKeyWriter) IO[string]

type RecordSaver func(
	PrimaryKey,
	PrimaryKeyWriter,
	map[string]any,
) IO[Void]

type MapToPrimaryKey func(map[string]any) PrimaryKey

func (s RecordSaver) SaveAll(
	m iter.Seq2[map[string]any, error],
	map2pk MapToPrimaryKey,
	wtr PrimaryKeyWriter,
) IO[Void] {
	return func(ctx context.Context) (Void, error) {
		for row, e := range m {
			select {
			case <-ctx.Done():
				return Empty, ctx.Err()
			default:
			}

			if nil != e {
				return Empty, e
			}

			var pk PrimaryKey = map2pk(row)

			_, e = s(pk, wtr, row)(ctx)
			if nil != e {
				return Empty, e
			}
		}
		return Empty, nil
	}
}

func MapToKeyNew(keyname string) MapToPrimaryKey {
	return func(m map[string]any) PrimaryKey {
		val, found := m[keyname]
		if !found {
			return InvalidKey
		}

		return AnyToKey(val)
	}
}

func PrimaryKeyInvalid(_ PrimaryKeyWriter) IO[string] {
	return Err[string](ErrInvalidKey)
}

var InvalidKey PrimaryKey = PrimaryKeyInvalid

func AnyToKey(key any) PrimaryKey {
	switch t := key.(type) {

	case int16:
		return ShortToKey(t)
	case int32:
		return IntToKey(t)
	case int64:
		return LongToKey(t)

	case time.Time:
		return TimeToKey(t)

	case [16]byte:
		return UuidToKey(t)
	case []byte:
		var buf [16]byte
		switch 16 == len(t) {
		case true:
			copy(buf[:], t)
			return UuidToKey(buf)
		default:
			return InvalidKey
		}

	default:
		return InvalidKey

	}
}

//go:generate go run internal/gen/strkeywriter/main.go Short int16  4 Uint16
//go:generate go run internal/gen/strkeywriter/main.go Int   int32  8 Uint32
//go:generate go run internal/gen/strkeywriter/main.go Long  int64 16 Uint64
//go:generate gofmt -s -w .
type StringKeyWriter struct {
	TimeLayout string
}

var StringKeyWriterDefault StringKeyWriter = StringKeyWriter{
	TimeLayout: time.DateOnly,
}

func (w *StringKeyWriter) WriteTime(key time.Time) IO[string] {
	return func(_ context.Context) (string, error) {
		return key.Format(w.TimeLayout), nil
	}
}

func (w *StringKeyWriter) WriteUuid(key [16]byte) IO[string] {
	var bufh [32]byte
	var bufs strings.Builder
	return func(_ context.Context) (string, error) {
		bufs.Reset()
		hex.Encode(bufh[:], key[:])
		_, _ = bufs.Write(bufh[:]) // error is always nil or OOM
		return bufs.String(), nil
	}
}

func (w *StringKeyWriter) AsWriter() PrimaryKeyWriter { return w }
