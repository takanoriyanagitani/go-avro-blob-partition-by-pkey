package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"text/template"

	. "github.com/takanoriyanagitani/go-avro-blob-partition-by-pkey/util"
)

var argLen int = len(os.Args)

var ArgByIndex func(int) IO[string] = Lift(
	func(index int) (string, error) {
		switch index < argLen {
		case true:
			return os.Args[index], nil
		default:
			return "", fmt.Errorf("invalid arg. ix: %v", index)
		}
	},
)

var tmpl *template.Template = template.Must(template.ParseFiles(
	"./internal/gen/strkeywriter/strkeywtr.tmpl",
))

type Config struct {
	TypeHint  string
	Primitive string
	PrimSize  int64
	HalfSize  int64
	EncodeFn  string
	CastName  string
	Filename  string
}

func (c Config) ExecuteTemplate(t *template.Template) error {
	return TemplateToFilename(t, c)
}

func (c Config) ToExecuteTemplate() IO[Void] {
	return func(_ context.Context) (Void, error) {
		return Empty, c.ExecuteTemplate(tmpl)
	}
}

var config IO[Config] = Bind(
	// arguments sample: Short int16 4 Uint16
	All(
		ArgByIndex(1), // e.g, Short
		ArgByIndex(2), // e.g, int16
		ArgByIndex(3), // e.g, 4
		ArgByIndex(4), // e.g, Uint16
	),
	Lift(func(s []string) (Config, error) {
		primSize, e := strconv.ParseInt(s[2], 10, 64)
		return Config{
			TypeHint:  s[0],
			Primitive: s[1],
			PrimSize:  primSize,
			HalfSize:  primSize >> 1,
			EncodeFn:  s[3],
			CastName:  strings.ToLower(s[3]),
			Filename:  strings.ToLower(s[0]) + "2str.go",
		}, e
	}),
)

var executeTemplate IO[Void] = Bind(
	config,
	func(c Config) IO[Void] { return c.ToExecuteTemplate() },
)

func TemplateToWriter(
	t *template.Template,
	w io.Writer,
	c Config,
) error {
	var bw *bufio.Writer = bufio.NewWriter(w)
	defer bw.Flush()
	return t.Execute(bw, c)
}

func TemplateToFileLike(
	t *template.Template,
	w io.WriteCloser,
	c Config,
) error {
	defer w.Close()
	return TemplateToWriter(
		t,
		w,
		c,
	)
}

func TemplateToFilename(
	t *template.Template,
	c Config,
) error {
	f, e := os.Create(c.Filename)
	if nil != e {
		return e
	}
	return TemplateToFileLike(
		t,
		f,
		c,
	)
}

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return executeTemplate(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		panic(e)
	}
}
