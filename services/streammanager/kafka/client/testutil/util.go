package testutil

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type Client struct {
	Dialer           *kafka.Dialer
	Network, Address string
}

func (c *Client) CreateTopic(ctx context.Context, topic string, numPartitions, replicationFactor int) error {
	conn, err := c.Dialer.DialContext(ctx, c.Network, c.Address)
	if err != nil {
		return fmt.Errorf("could not dial %s/%s: %w", c.Network, c.Address, err)
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

	controllerConn, err := kafka.DialContext(ctx, c.Network, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
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

func (c *Client) ListTopics(ctx context.Context) ([]string, error) {
	conn, err := c.Dialer.DialContext(ctx, c.Network, c.Address)
	if err != nil {
		return nil, fmt.Errorf("could not dial %s/%s: %w", c.Network, c.Address, err)
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
		var topics []string
		for i := range partitions {
			topics = append(topics, fmt.Sprintf("%s [partition %d]", partitions[i].Topic, partitions[i].ID))
		}
		return topics, nil
	}
}
