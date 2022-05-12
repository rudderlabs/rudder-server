package testutil

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// NewWithDialer returns a client with a custom dialer
func NewWithDialer(dialer *kafka.Dialer, network, address string) *Client {
	return &Client{
		dialer:  dialer,
		network: network,
		address: address,
	}
}

// New returns a client with a default dialer
func New(network, address string) *Client {
	return NewWithDialer(&kafka.Dialer{
		DualStack: true,
		Timeout:   10 * time.Second,
	}, network, address)
}

type Client struct {
	dialer           *kafka.Dialer
	network, address string
}

func (c *Client) CreateTopic(ctx context.Context, topic string, numPartitions, replicationFactor int) error {
	conn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return fmt.Errorf("could not dial %s/%s: %w", c.network, c.address, err)
	}

	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = conn.Close() }()
	}()

	var (
		errors  = make(chan error, 1)
		brokers = make(chan kafka.Broker, 1)
	)

	go func() { // doing it asynchronously because conn.Controller() does not honour the context
		b, err := conn.Controller()
		if err != nil {
			errors <- fmt.Errorf("could not get controller: %w", err)
			return
		}
		if b.Host == "" {
			errors <- fmt.Errorf("create topic: empty host")
			return
		}
		brokers <- b
	}()

	var broker kafka.Broker
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errors:
		return err
	case broker = <-brokers:
	}

	controllerConn, err := kafka.DialContext(ctx, c.network, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		return fmt.Errorf("could not dial via controller: %w", err)
	}
	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = controllerConn.Close() }()
	}()

	go func() { // doing it asynchronously because controllerConn.CreateTopics() does not honour the context
		errors <- controllerConn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		})
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errors:
		return err
	}
}

type TopicPartition struct {
	Topic     string
	Partition int
}

func (c *Client) ListTopics(ctx context.Context) ([]TopicPartition, error) {
	conn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return nil, fmt.Errorf("could not dial %s/%s: %w", c.network, c.address, err)
	}

	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = conn.Close() }()
	}()

	var (
		done   = make(chan []kafka.Partition, 1)
		errors = make(chan error, 1)
	)
	go func() { // doing it asynchronously because conn.ReadPartitions() does not honour the context
		partitions, err := conn.ReadPartitions()
		if err != nil {
			errors <- err
		} else {
			done <- partitions
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err = <-errors:
		return nil, fmt.Errorf("could not read partitions: %w", err)
	case partitions := <-done:
		var topics []TopicPartition
		for i := range partitions {
			topics = append(topics, TopicPartition{
				Topic:     partitions[i].Topic,
				Partition: partitions[i].ID,
			})
		}
		return topics, nil
	}
}
