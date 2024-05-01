package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/middle-management/migratex"
	_ "modernc.org/sqlite"
)

func getenv(e string, d string) string {
	if v, ok := os.LookupEnv(e); ok {
		return v
	}
	return d
}

func main() {
	flagSchema := flag.String("schema", getenv("SCHEMA", "/dev/stdin"), "path to schema ($SCHEMA)")
	flagAutoApply := flag.Bool("auto-apply", false, "apply plan without asking")
	flagAllowDeletions := flag.Bool("allow-deletions", false, "unless set deletions are not allowed")
	flag.Parse()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		select {
		case <-signalChan:
			cancel()
		case <-ctx.Done():
		}
	}()

	if flag.NArg() == 0 {
		log.Fatal("no db in args")
	}

	db, err := sql.Open("sqlite", flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Define the new schema as SQL text
	f, err := os.Open(*flagSchema)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	schema, err := io.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}

	// create migration plan
	ops, err := migratex.Plan(ctx, db, string(schema), *flagAllowDeletions)
	if err != nil {
		log.Fatal(err)
	}

	if len(ops) == 0 {
		return
	}

	// present plan
	fmt.Println("Plan: ")
	for i, op := range ops {
		fmt.Printf(" %d: %s\n", i+1, op.Normalized())
	}

	// get plan confirmed
	if !*flagAutoApply {
		tty, err := os.Open("/dev/tty") // TODO this would not support windows...
		if err != nil {
			log.Fatalf("can't open /dev/tty: %s", err)
		}
		scanner := bufio.NewScanner(tty)
		for {
			fmt.Print(`Type "y" to apply plan: `)
			scanner.Scan()
			text := scanner.Text()
			if text != "y" {
				log.Fatal("aborting plan")
			} else {
				break
			}
		}
	} else {
		log.Println("will apply plan automatically")
	}

	// apply plan
	err = migratex.Apply(ctx, db, ops)
	if err != nil {
		log.Fatal(err)
	}
}
