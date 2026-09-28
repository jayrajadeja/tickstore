// Command tickstore ingests a lob trade stream into per-symbol logs and queries
// them. It is the only package in the project that performs I/O.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jayrajadeja/tickstore/store"
	"github.com/jayrajadeja/tickstore/tick"
)

const (
	minInt64 = -1 << 63
	maxInt64 = 1<<63 - 1
)

func sideString(s tick.Side) string {
	if s == tick.Sell {
		return "sell"
	}
	return "buy"
}

func formatTick(t tick.Tick) string {
	return fmt.Sprintf("ts=%d price=%d qty=%d side=%s", t.TS, t.Price, t.Qty, sideString(t.Side))
}

// runIngest reads RecordSize-byte records from in until EOF and appends them to
// <dir>/<symbol>.log. A partial trailing record is an error (whole records read
// so far are still flushed).
func runIngest(dir, symbol string, in io.Reader) error {
	s := store.New(dir)
	a, err := s.OpenAppender(symbol)
	if err != nil {
		return err
	}
	r := bufio.NewReader(in)
	buf := make([]byte, tick.RecordSize)
	for {
		_, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			a.Close()
			return errors.New("truncated stream: partial final record")
		}
		if err != nil {
			a.Close()
			return err
		}
		t, derr := tick.Decode(buf)
		if derr != nil {
			a.Close()
			return derr
		}
		if aerr := a.Append(t); aerr != nil {
			a.Close()
			return aerr
		}
	}
	return a.Close()
}

// writeTicks renders ticks as text, one per line.
func writeTicks(out io.Writer, ticks []tick.Tick) error {
	w := bufio.NewWriter(out)
	for _, t := range ticks {
		if _, err := fmt.Fprintln(w, formatTick(t)); err != nil {
			return err
		}
	}
	return w.Flush()
}

// runQuery prints ticks as text: last N when last > 0, else the range [from,to].
func runQuery(dir, symbol string, from, to int64, last int, out io.Writer) error {
	s := store.New(dir)
	var (
		ticks []tick.Tick
		err   error
	)
	if last > 0 {
		ticks, err = s.Last(symbol, last)
	} else {
		ticks, err = s.Range(symbol, from, to)
	}
	if err != nil {
		return err
	}
	return writeTicks(out, ticks)
}

// runDump prints every record in the log as text.
func runDump(dir, symbol string, out io.Writer) error {
	ticks, err := store.New(dir).Range(symbol, minInt64, maxInt64)
	if err != nil {
		return err
	}
	return writeTicks(out, ticks)
}

// runReindex rebuilds the sparse index for symbol from its log.
func runReindex(dir, symbol string, out io.Writer) error {
	n, err := store.New(dir).Reindex(symbol)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "reindexed %s: %d index entries\n", symbol, n)
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tickstore <ingest|query|dump|reindex> [flags]")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "ingest":
		fs := flag.NewFlagSet("ingest", flag.ExitOnError)
		dir := fs.String("dir", "data", "data directory")
		symbol := fs.String("symbol", "", "symbol to ingest into")
		fs.Parse(args)
		if *symbol == "" {
			err = errors.New("ingest: --symbol is required")
		} else {
			err = runIngest(*dir, *symbol, os.Stdin)
		}
	case "query":
		fs := flag.NewFlagSet("query", flag.ExitOnError)
		dir := fs.String("dir", "data", "data directory")
		symbol := fs.String("symbol", "", "symbol to query")
		from := fs.Int64("from", minInt64, "range start (inclusive, logical TS)")
		to := fs.Int64("to", maxInt64, "range end (inclusive, logical TS)")
		last := fs.Int("last", 0, "return the last N ticks (exclusive with --from/--to)")
		fs.Parse(args)
		switch {
		case *symbol == "":
			err = errors.New("query: --symbol is required")
		case *last < 0:
			err = errors.New("query: --last must be non-negative")
		case *last > 0 && (*from != minInt64 || *to != maxInt64):
			err = errors.New("query: --last is exclusive with --from/--to")
		default:
			err = runQuery(*dir, *symbol, *from, *to, *last, os.Stdout)
		}
	case "dump":
		fs := flag.NewFlagSet("dump", flag.ExitOnError)
		dir := fs.String("dir", "data", "data directory")
		symbol := fs.String("symbol", "", "symbol to dump")
		fs.Parse(args)
		if *symbol == "" {
			err = errors.New("dump: --symbol is required")
		} else {
			err = runDump(*dir, *symbol, os.Stdout)
		}
	case "reindex":
		fs := flag.NewFlagSet("reindex", flag.ExitOnError)
		dir := fs.String("dir", "data", "data directory")
		symbol := fs.String("symbol", "", "symbol to reindex")
		fs.Parse(args)
		if *symbol == "" {
			err = errors.New("reindex: --symbol is required")
		} else {
			err = runReindex(*dir, *symbol, os.Stdout)
		}
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
