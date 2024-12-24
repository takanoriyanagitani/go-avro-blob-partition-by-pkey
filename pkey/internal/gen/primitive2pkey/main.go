package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
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
	"./internal/gen/primitive2pkey/prim2pkey.tmpl",
))

type Config struct {
	TypeHint  string
	Primitive string
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
	All(
		ArgByIndex(1),
		ArgByIndex(2),
	),
	Lift(func(s []string) (Config, error) {
		return Config{
			TypeHint:  s[0],
			Primitive: s[1],
			Filename:  strings.ToLower(s[0]) + "2pkey.go",
		}, nil
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
