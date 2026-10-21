// puddle: watch Kafka and Redpanda topics in your terminal, with Debezium
// change events shown field by field.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/Dem1241/puddle/internal/broker"
	"github.com/Dem1241/puddle/internal/demo"
	"github.com/Dem1241/puddle/internal/ui"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "puddle:", err)
		os.Exit(1)
	}
}

func run() error {
	brokers := flag.String("brokers", defaultBrokers(), "comma-separated broker addresses (env PUDDLE_BROKERS or KAFKA_BROKERS)")
	flag.StringVar(brokers, "b", *brokers, "shorthand for -brokers")
	demoMode := flag.Bool("demo", false, "start a built-in cluster with a live stream of example changes")
	last := flag.Int64("last", 200, "older messages to load per partition when opening a topic")
	showVersion := flag.Bool("version", false, "print the version")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, "usage: puddle [flags] [topic]\n\nWatch Kafka and Redpanda topics, with Debezium changes shown field by field.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *showVersion {
		fmt.Println("puddle", version)
		return nil
	}
	if flag.NArg() > 1 {
		flag.Usage()
		os.Exit(2)
	}

	seeds, label := strings.Split(*brokers, ","), *brokers
	if *demoMode {
		d, err := demo.Start(700 * time.Millisecond)
		if err != nil {
			return fmt.Errorf("starting the demo cluster: %w", err)
		}
		defer d.Close()
		seeds, label = d.Addrs(), "demo cluster"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := broker.Dial(ctx, seeds)
	if err != nil {
		return fmt.Errorf("%w\n(no broker? try: puddle --demo)", err)
	}
	defer client.Close()

	_, err = tea.NewProgram(ui.New(client, label, *last, flag.Arg(0))).Run()
	return err
}

func defaultBrokers() string {
	for _, env := range []string{"PUDDLE_BROKERS", "KAFKA_BROKERS"} {
		if v := os.Getenv(env); v != "" {
			return v
		}
	}
	return "localhost:9092"
}
