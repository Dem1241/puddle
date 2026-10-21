// Package demo runs an in-memory Kafka cluster (franz-go's kfake) and fills
// it with Debezium-style change events from a pretend shop database, so
// puddle can be tried without a real broker.
package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	Customers = "shop.public.customers"
	Products  = "shop.public.products"
	Orders    = "shop.public.orders"
	Logs      = "app.logs"
)

// Cluster is a running demo cluster.
type Cluster struct {
	fake   *kfake.Cluster
	cancel context.CancelFunc
	done   chan struct{}
}

// Start launches the cluster, writes an initial snapshot and keeps producing
// changes until Close. Pass every > 0 to set the average time between changes.
func Start(every time.Duration) (*Cluster, error) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, Customers, Products, Orders, Logs))
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...))
	if err != nil {
		fake.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{fake, cancel, make(chan struct{})}
	s := newShop()
	if err := s.produce(ctx, cl, s.snapshot()...); err != nil {
		cl.Close()
		c.Close()
		return nil, err
	}
	go func() {
		defer close(c.done)
		defer cl.Close()
		for {
			wait := time.Duration(float64(every) * (0.3 + s.rng.Float64()*1.4))
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			// A failed write only happens while shutting down; the next one retries.
			_ = s.produce(ctx, cl, s.change()...)
		}
	}()
	return c, nil
}

// Addrs are the broker addresses to connect to.
func (c *Cluster) Addrs() []string { return c.fake.ListenAddrs() }

func (c *Cluster) Close() {
	c.cancel()
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
	}
	c.fake.Close()
}

// shop is the pretend database: rows by id, so updates and deletes refer to
// rows that exist.
type shop struct {
	rng       *rand.Rand
	now       time.Time
	customers map[int]map[string]any
	products  map[int]map[string]any
	orders    map[int]map[string]any
	nextID    map[string]int
}

func newShop() *shop {
	return &shop{
		rng:       rand.New(rand.NewPCG(42, 7)),
		now:       time.Now(),
		customers: map[int]map[string]any{},
		products:  map[int]map[string]any{},
		orders:    map[int]map[string]any{},
		nextID:    map[string]int{Customers: 1001, Products: 101, Orders: 5001},
	}
}

var (
	firstNames = []string{"mia", "noah", "lena", "elias", "sofia", "luca", "emma", "finn", "hannah", "ben", "yuki", "amir"}
	lastNames  = []string{"schmidt", "weber", "fischer", "nakamura", "yilmaz", "novak", "berg", "silva", "kowalski", "meyer"}
	cities     = []string{"paderborn", "berlin", "hamburg", "köln", "münchen", "leipzig", "bielefeld", "dortmund"}
	plans      = []string{"free", "free", "pro", "team"}
	items      = []struct {
		name  string
		price float64
	}{{"rain jacket", 89.90}, {"umbrella", 24.50}, {"wool socks", 12.00}, {"thermos", 29.90}, {"headphones", 149.00}, {"desk lamp", 39.90}, {"notebook", 6.50}, {"mechanical keyboard", 119.00}}
	statuses = []string{"pending", "paid", "packed", "shipped", "delivered"}
)

type record struct {
	topic  string
	id     int
	op     string
	before map[string]any
	after  map[string]any
}

func (s *shop) id(topic string) int {
	id := s.nextID[topic]
	s.nextID[topic]++
	return id
}

func (s *shop) newCustomer() map[string]any {
	first, last := pick(s.rng, firstNames), pick(s.rng, lastNames)
	id := s.id(Customers)
	return map[string]any{
		"id": id, "first_name": first, "last_name": last,
		"email": fmt.Sprintf("%s.%s@example.com", first, last),
		"plan":  pick(s.rng, plans), "city": pick(s.rng, cities),
		"updated_at": s.now.UTC().Format(time.RFC3339),
	}
}

func (s *shop) snapshot() []record {
	var out []record
	for range 8 {
		c := s.newCustomer()
		s.customers[c["id"].(int)] = c
		out = append(out, record{Customers, c["id"].(int), "r", nil, c})
	}
	for _, it := range items {
		id := s.id(Products)
		p := map[string]any{"id": id, "name": it.name, "price": it.price, "stock": 20 + s.rng.IntN(80)}
		s.products[id] = p
		out = append(out, record{Products, id, "r", nil, p})
	}
	return out
}

// change makes one plausible thing happen in the shop.
func (s *shop) change() []record {
	s.now = time.Now()
	switch n := s.rng.IntN(100); {
	case n < 30: // an order comes in, and stock goes down
		cid := anyKey(s.rng, s.customers)
		pid := anyKey(s.rng, s.products)
		qty := 1 + s.rng.IntN(3)
		p := s.products[pid]
		id := s.id(Orders)
		o := map[string]any{
			"id": id, "customer_id": cid, "product_id": pid, "quantity": qty,
			"total": round2(p["price"].(float64) * float64(qty)), "status": "pending",
			"created_at": s.now.UTC().Format(time.RFC3339),
		}
		s.orders[id] = o
		before := clone(p)
		p["stock"] = max(0, p["stock"].(int)-qty)
		return []record{{Orders, id, "c", nil, o}, {Products, pid, "u", before, clone(p)}}
	case n < 60 && len(s.orders) > 0: // an order moves along
		id := anyKey(s.rng, s.orders)
		o := s.orders[id]
		i := index(statuses, o["status"].(string))
		if i == len(statuses)-1 {
			delete(s.orders, id)
			return []record{{Orders, id, "d", o, nil}}
		}
		before := clone(o)
		o["status"] = statuses[i+1]
		return []record{{Orders, id, "u", before, clone(o)}}
	case n < 75: // someone edits their profile
		id := anyKey(s.rng, s.customers)
		c := s.customers[id]
		before := clone(c)
		if s.rng.IntN(2) == 0 {
			c["plan"] = pickOther(s.rng, plans, c["plan"].(string))
		} else {
			c["city"] = pickOther(s.rng, cities, c["city"].(string))
		}
		c["updated_at"] = s.now.UTC().Format(time.RFC3339)
		return []record{{Customers, id, "u", before, clone(c)}}
	case n < 82: // a new customer signs up
		c := s.newCustomer()
		s.customers[c["id"].(int)] = c
		return []record{{Customers, c["id"].(int), "c", nil, c}}
	case n < 86 && len(s.customers) > 6: // and sometimes one leaves
		id := anyKey(s.rng, s.customers)
		c := s.customers[id]
		delete(s.customers, id)
		return []record{{Customers, id, "d", c, nil}}
	case n < 92: // restock
		id := anyKey(s.rng, s.products)
		p := s.products[id]
		before := clone(p)
		p["stock"] = p["stock"].(int) + 25
		return []record{{Products, id, "u", before, clone(p)}}
	default: // the app logs something: a plain, non-CDC message
		return []record{{topic: Logs}}
	}
}

func (s *shop) produce(ctx context.Context, cl *kgo.Client, recs ...record) error {
	var out []*kgo.Record
	for _, r := range recs {
		if r.topic == Logs {
			out = append(out, &kgo.Record{Topic: Logs, Value: s.logLine()})
			continue
		}
		key, _ := json.Marshal(map[string]int{"id": r.id})
		out = append(out, &kgo.Record{Topic: r.topic, Key: key, Value: s.envelope(r)})
	}
	return cl.ProduceSync(ctx, out...).FirstErr()
}

func (s *shop) envelope(r record) []byte {
	db, schema, table := "shop", "public", r.topic[len("shop.public."):]
	ms := s.now.UnixMilli()
	payload := map[string]any{
		"before": nullable(r.topic, r.before),
		"after":  nullable(r.topic, r.after),
		"source": map[string]any{
			"version": "3.3.1.Final", "connector": "postgresql", "name": "shop",
			"ts_ms": ms, "snapshot": r.op == "r", "db": db, "schema": schema, "table": table,
		},
		"op":    r.op,
		"ts_ms": ms + int64(s.rng.IntN(40)),
	}
	// Customers come with the schema wrapper, like the JSON converter's
	// default; the other tables without, like schemas.enable=false.
	var v any = payload
	if r.topic == Customers {
		v = map[string]any{"schema": map[string]any{"type": "struct", "name": "shop.public.customers.Envelope"}, "payload": payload}
	}
	b, _ := json.Marshal(orderedPayload(v))
	return b
}

func (s *shop) logLine() []byte {
	lines := []map[string]any{
		{"level": "info", "msg": "checkout completed", "duration_ms": 80 + s.rng.IntN(300)},
		{"level": "warn", "msg": "payment provider slow", "duration_ms": 1200 + s.rng.IntN(2000)},
		{"level": "info", "msg": "cache warmed", "entries": 200 + s.rng.IntN(800)},
		{"level": "error", "msg": "email bounced", "retry": true},
	}
	l := lines[s.rng.IntN(len(lines))]
	l["ts"] = s.now.UTC().Format(time.RFC3339Nano)
	l["service"] = "shop-api"
	b, _ := json.Marshal(l)
	return b
}

// orderedPayload makes encoding/json write before, after, source, op, ts_ms in
// Debezium's order instead of alphabetically.
func orderedPayload(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	if p, ok := m["payload"]; ok {
		return struct {
			Schema  any `json:"schema"`
			Payload any `json:"payload"`
		}{m["schema"], orderedPayload(p)}
	}
	return struct {
		Before any `json:"before"`
		After  any `json:"after"`
		Source any `json:"source"`
		Op     any `json:"op"`
		TsMs   any `json:"ts_ms"`
	}{m["before"], m["after"], m["source"], m["op"], m["ts_ms"]}
}

func nullable(topic string, m map[string]any) any {
	if m == nil {
		return nil
	}
	return row{columns[topic], m}
}

func clone(m map[string]any) map[string]any {
	c := make(map[string]any, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func pick[T any](rng *rand.Rand, xs []T) T { return xs[rng.IntN(len(xs))] }

func pickOther(rng *rand.Rand, xs []string, not string) string {
	for {
		if x := pick(rng, xs); x != not {
			return x
		}
	}
}

func anyKey(rng *rand.Rand, m map[int]map[string]any) int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// map order is random; sort so the seeded rng stays reproducible
	slices.Sort(keys)
	return keys[rng.IntN(len(keys))]
}

func index(xs []string, x string) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return 0
}

func round2(f float64) float64 { return float64(int(f*100+0.5)) / 100 }

// columns is each table's column order, which rows are written in (a map
// alone would come out alphabetically).
var columns = map[string][]string{
	Customers: {"id", "first_name", "last_name", "email", "plan", "city", "updated_at"},
	Products:  {"id", "name", "price", "stock"},
	Orders:    {"id", "customer_id", "product_id", "quantity", "total", "status", "created_at"},
}

type row struct {
	cols []string
	vals map[string]any
}

func (r row) MarshalJSON() ([]byte, error) {
	b := []byte{'{'}
	for i, c := range r.cols {
		if i > 0 {
			b = append(b, ',')
		}
		k, _ := json.Marshal(c)
		v, err := json.Marshal(r.vals[c])
		if err != nil {
			return nil, err
		}
		b = append(append(append(b, k...), ':'), v...)
	}
	return append(b, '}'), nil
}
