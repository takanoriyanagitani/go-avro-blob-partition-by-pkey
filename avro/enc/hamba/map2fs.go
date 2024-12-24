package enc

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	bp "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey"
	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"

	pk "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/pkey"
)

func MapToWriterHamba(
	m map[string]any,
	w io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, e := ho.NewEncoderWithSchema(s, w, opts...)
	if nil != e {
		return e
	}
	defer enc.Close()

	e = enc.Encode(m)

	return errors.Join(e, enc.Flush())
}

func CodecConv(c bp.Codec) ho.CodecName {
	switch c {
	case bp.CodecNull:
		return ho.Null
	case bp.CodecDeflate:
		return ho.Deflate
	case bp.CodecSnappy:
		return ho.Snappy
	case bp.CodecZstd:
		return ho.ZStandard
	default:
		return ho.Null
	}
}

func ConfigToOpts(cfg bp.EncodeConfig) []ho.EncoderFunc {
	var blockLen int = cfg.BlockLength
	var c bp.Codec = cfg.Codec
	var converted ho.CodecName = CodecConv(c)
	return []ho.EncoderFunc{
		ho.WithBlockLength(blockLen),
		ho.WithCodec(converted),
	}
}

func MapToWriter(
	m map[string]any,
	w io.Writer,
	schema string,
	cfg bp.EncodeConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}
	var opts []ho.EncoderFunc = ConfigToOpts(cfg)
	return MapToWriterHamba(
		m,
		w,
		parsed,
		opts...,
	)
}

func MapToFileLike(
	m map[string]any,
	f io.WriteCloser,
	finalize func() error,
	schema string,
	cfg bp.EncodeConfig,
) error {
	return errors.Join(
		MapToWriter(m, f, schema, cfg),
		finalize(),
		f.Close(),
	)
}

func MapToFs(
	m map[string]any,
	filename string,
	sync func(*os.File) error,
	schema string,
	cfg bp.EncodeConfig,
) error {
	f, e := os.Create(filename)
	if nil != e {
		return e
	}
	defer f.Close()
	return MapToFileLike(
		m,
		f,
		func() error { return sync(f) },
		schema,
		cfg,
	)
}

type Config struct {
	Schema string
	bp.EncodeConfig
}

type FsyncType string

const (
	FsyncSync FsyncType = "fsync"
	FsyncFast FsyncType = "fast"
)

func StringToFsyncType(s string) (FsyncType, error) {
	switch s {
	case "fast":
		return FsyncFast, nil
	default:
		return FsyncSync, nil
	}
}

func (f FsyncType) ToFsync() func(*os.File) error {
	switch f {
	case FsyncFast:
		return func(_ *os.File) error { return nil }
	default:
		return func(f *os.File) error { return f.Sync() }
	}
}

type FsConfig struct {
	Config
	FsyncType
	Dirname
}

func (f FsConfig) WriteMap(
	m map[string]any,
	filename string,
) error {
	return MapToFs(
		m,
		filename,
		f.FsyncType.ToFsync(),
		f.Config.Schema,
		f.Config.EncodeConfig,
	)
}

type KeyToFilename func(pk.PrimaryKey, pk.PrimaryKeyWriter) IO[string]

func (f FsConfig) ToSaver(
	pk2filename KeyToFilename,
) pk.RecordSaver {
	return func(
		pk pk.PrimaryKey,
		pw pk.PrimaryKeyWriter,
		m map[string]any,
	) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			filename, e := pk2filename(pk, pw)(ctx)
			if nil != e {
				return Empty, e
			}

			return Empty, f.WriteMap(
				m,
				filename,
			)
		}
	}
}

func (f FsConfig) SaverFromDirnameDefault() pk.RecordSaver {
	var key2filename = f.Dirname.ToKeyToFilenameDefault()
	return f.ToSaver(key2filename)
}

type BasenameToPath func(string) IO[string]

func (d BasenameToPath) ToKeyToFilename() KeyToFilename {
	return func(
		pk pk.PrimaryKey,
		pw pk.PrimaryKeyWriter,
	) IO[string] {
		return Bind(
			pk(pw),
			d,
		)
	}
}

type Dirname string

type JoinPath func(parent string) func(withExt string) IO[string]

func JoinPathStd(parent string) func(withExt string) IO[string] {
	return func(withExt string) IO[string] {
		return OfFn(func() string {
			return filepath.Join(parent, withExt)
		})
	}
}

var JoinPathDefault JoinPath = JoinPathStd

type BasenameWithExt func(basename string) IO[string]

func (d Dirname) ToBasenameToPath(
	joinPath JoinPath,
	basenameWithExt BasenameWithExt,
) BasenameToPath {
	return func(basename string) IO[string] {
		return Bind(
			basenameWithExt(basename),
			joinPath(string(d)),
		)
	}
}

func (d Dirname) ToBasenameToPathDefault() BasenameToPath {
	return d.ToBasenameToPath(
		JoinPathDefault,
		ExtDefault.ToBasenameWithExt(),
	)
}

func (d Dirname) ToKeyToFilenameDefault() KeyToFilename {
	return d.ToBasenameToPathDefault().ToKeyToFilename()
}

type Ext string

func (e Ext) ToBasenameWithExt() BasenameWithExt {
	var buf strings.Builder
	return func(basename string) IO[string] {
		return OfFn(func() string {
			buf.Reset()
			_, _ = buf.WriteString(basename)  // error is always nil or OOM
			_, _ = buf.WriteString(".")       // error is always nil or OOM
			_, _ = buf.WriteString(string(e)) // error is always nil or OOM
			return buf.String()
		})
	}
}

var ExtDefault Ext = "avro"
