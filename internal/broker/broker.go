// Package broker talks to Kafka-compatible brokers (Kafka, Redpanda, ...)
// through franz-go: listing topics and tailing one of them.
package broker

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Dem1241/puddle/internal/cdc"
)

// Topic is a topic with its size.
type Topic struct {
	Name       string
	Partitions int
	Messages   int64 // records currently retained, summed over partitions
}

// Message is one consumed record, decoded as CDC when it is one.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
	Event     cdc.Event
	IsCDC     bool
}

// Client is a connection to one cluster.
type Client struct {
	seeds []string
	kgo   *kgo.Client
	adm   *kadm.Client
}

// Dial connects to the cluster and checks that a broker answers.
func Dial(ctx context.Context, seeds []string) (*Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return nil, err
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("no broker answered at %v: %w", seeds, err)
	}
	return &Client{seeds, cl, kadm.NewClient(cl)}, nil
}

func (c *Client) Close() { c.kgo.Close() }

// Topics lists the non-internal topics, sorted by name.
func (c *Client) Topics(ctx context.Context) ([]Topic, error) {
	details, err := c.adm.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	names := details.Names()
	starts, err := c.adm.ListStartOffsets(ctx, names...)
	if err != nil {
		return nil, err
	}
	ends, err := c.adm.ListEndOffsets(ctx, names...)
	if err != nil {
		return nil, err
	}

	topics := make([]Topic, 0, len(details))
	for _, d := range details.Sorted() {
		t := Topic{Name: d.Topic, Partitions: len(d.Partitions)}
		for p, end := range ends[d.Topic] {
			if start, ok := starts[d.Topic][p]; ok && end.Err == nil && start.Err == nil {
				t.Messages += end.Offset - start.Offset
			}
		}
		topics = append(topics, t)
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	return topics, nil
}

// Tail streams topic, starting with up to last records of every partition,
// then everything new. The channel closes when ctx is done.
func (c *Client) Tail(ctx context.Context, topic string, last int64) (<-chan Message, error) {
	starts, err := c.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	ends, err := c.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	offsets := map[int32]kgo.Offset{}
	for p, end := range ends[topic] {
		if end.Err != nil {
			return nil, end.Err
		}
		from := max(end.Offset-last, starts[topic][p].Offset)
		offsets[p] = kgo.NewOffset().At(from)
	}
	if len(offsets) == 0 {
		return nil, fmt.Errorf("topic %q not found", topic)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}),
	)
	if err != nil {
		return nil, err
	}

	ch := make(chan Message, 256)
	go func() {
		defer close(ch)
		defer cl.Close()
		for {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				r := iter.Next()
				m := Message{
					Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
					Key: r.Key, Value: r.Value, Time: r.Timestamp,
				}
				m.Event, m.IsCDC = cdc.Parse(r.Value)
				select {
				case ch <- m:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}
