package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// Usage example
// go run build/wait-for-go/wait-for.go -u -t 10 localhost:8125 && go run main.go
func canConnect(host, port, protocol string) bool {
	timeout := time.Second
	conn, err := net.DialTimeout(protocol, net.JoinHostPort(host, port), timeout)
	if err != nil {
		fmt.Println("Connecting error:", err)
	}
	// When using UDP do a quick check to see if something is listening on the
	// given port to return an error as soon as possible.

	if conn != nil {
		if protocol == "udp" {
			for i := 0; i < 2; i++ {
				_, err = conn.Write(nil)
				time.Sleep(1 * time.Second)
				fmt.Println("UDP error:", err)
				if err != nil {
					_ = conn.Close()
					return false
				}
			}
		}
		_ = conn.Close()
		fmt.Println("Opened", net.JoinHostPort(host, port))
		return true
	}
	return false
}

func main() {
	protocol := "tcp"
	udp := flag.Bool("u", false, "check for udp")
	timeout := flag.Int("t", 60, "Timeout in seconds")
	flag.Parse()
	if *udp {
		protocol = "udp"
	}
	hostport := flag.Args()

	hostportArray := strings.SplitN(hostport[0], ":", 2)
	host := hostportArray[0]
	port := hostportArray[1]
	for index := 0; index < *timeout; index++ {
		connected := canConnect(host, port, protocol)
		if connected {
			os.Exit(0)
		}
		time.Sleep(time.Second)
	}
	os.Exit(1)
}
