package pkey2avros

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct {
	BlobSizeMax int
}

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

func CodecFromString(s string) Codec {
	switch s {
	case "deflate":
		return CodecDeflate
	case "snappy":
		return CodecSnappy
	case "zstandard":
		return CodecZstd
	case "bzip2":
		return CodecBzip2
	case "xz":
		return CodecXz
	default:
		return CodecNull
	}
}

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
