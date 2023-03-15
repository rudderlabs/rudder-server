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
func NewWithDialer(dialer *kafka.Dialer, network string, address ...string) *Client {
	return &Client{
		dialer:    dialer,
		network:   network,
		addresses: address,
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
	dialer    *kafka.Dialer
	network   string
	addresses []string
}

func (c *Client) ping(ctx context.Context) (*kafka.Conn, error) {
	var (
		err  error
		conn *kafka.Conn
	)
	for _, addr := range c.addresses {
		conn, err = c.dialer.DialContext(ctx, c.network, kafka.TCP(addr).String())
		if err == nil { // we can connect to at least one address, no need to check all of them
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf(
			"could not dial any of the addresses %s/%s: %w", c.network, kafka.TCP(c.addresses...).String(), err,
		)
	}
	return conn, nil
}

func (c *Client) CreateTopic(ctx context.Context, topic string, numPartitions, replicationFactor int) error {
	conn, err := c.ping(ctx)
	if err != nil {
		return fmt.Errorf("create topic: cannot ping: %w", err)
	}

	var (
		controllerHost string
		errors         = make(chan error, 1)
	)
	defer close(errors)
	go func() { // doing it asynchronously because conn.Controller() does not honour the context
		var b kafka.Broker
		b, err = conn.Controller()
		if err != nil {
			errors <- fmt.Errorf("create topic: could not get controller: %w", err)
			return
		}
		if b.Host == "" {
			errors <- fmt.Errorf("create topic: controller connection has empty broker host")
			return
		}
		controllerHost = net.JoinHostPort(b.Host, strconv.Itoa(b.Port))
		errors <- nil
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("create topic: %w", ctx.Err())
	case err := <-errors:
		if err != nil {
			return err
		}
	}

	controllerConn, err := c.dialer.DialContext(ctx, c.network, controllerHost)
	if err != nil {
		return fmt.Errorf("create topic: could not dial controller: %w", err)
	}
	defer func() {
		// close asynchronously, if we block we might not respect the context
		go func() { _ = controllerConn.Close() }()
	}()

	go func() { // doing it asynchronously because controllerConn.CreateTopics() does not honour the context
		errors <- conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		})
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errors:
		if err != nil {
			return fmt.Errorf("create topic: could not create topic: %w", err)
		}
		return nil
	}
}

type TopicPartition struct {
	Topic     string
	Partition int
}

func (c *Client) ListTopics(ctx context.Context) ([]TopicPartition, error) {
	conn, err := c.ping(ctx)
	if err != nil {
		return nil, err
	}

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
