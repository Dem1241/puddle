package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/Dem1241/puddle/internal/broker"
	"github.com/Dem1241/puddle/internal/cdc"
	"github.com/Dem1241/puddle/internal/demo"
)

func TestDemoCluster(t *testing.T) {
	d, err := demo.Start(20 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := broker.Dial(ctx, d.Addrs())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	topics, err := c.Topics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]broker.Topic{}
	for _, tp := range topics {
		byName[tp.Name] = tp
	}
	for _, name := range []string{demo.Customers, demo.Products, demo.Orders, demo.Logs} {
		if byName[name].Partitions != 3 {
			t.Errorf("%s: %+v", name, byName[name])
		}
	}
	if byName[demo.Customers].Messages < 8 || byName[demo.Products].Messages < 8 {
		t.Errorf("snapshot missing: %+v", topics)
	}

	ch, err := c.Tail(ctx, demo.Customers, 100)
	if err != nil {
		t.Fatal(err)
	}
	var snap int
	for snap < 8 {
		select {
		case m := <-ch:
			if !m.IsCDC || m.Event.Source != "shop.public.customers" {
				t.Fatalf("not decoded: %s", m.Value)
			}
			if m.Event.Op == cdc.OpRead {
				snap++
				if m.Event.After[0].Name != "id" {
					t.Errorf("columns out of order: %v", m.Event.After)
				}
			}
		case <-ctx.Done():
			t.Fatalf("only %d snapshot rows arrived", snap)
		}
	}

	if _, err := c.Tail(ctx, "nope", 10); err == nil {
		t.Error("tailing a missing topic should fail")
	}
}

func TestDialFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := broker.Dial(ctx, []string{"127.0.0.1:1"}); err == nil {
		t.Error("dial to a closed port should fail")
	}
}
