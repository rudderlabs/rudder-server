package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func raw_connect(host string, port string, protocol string) bool {
	timeout := time.Second
	conn, err := net.DialTimeout(protocol, net.JoinHostPort(host, port), timeout)
	if err != nil {
		fmt.Println("Connecting error:", err)
	}
	// When using UDP do a quick check to see if something is listening on the
	// given port to return an error as soon as possible.
	if protocol == "udp" {
		for i := 0; i < 2; i++ {
			_, err = conn.Write(nil)
			time.Sleep(1 * time.Second)
			fmt.Println("UDP error:", err)
			if err != nil {
				_ = conn.Close()
				conn = nil
				return false
			}
		}
	}

	if conn != nil {
		defer conn.Close()
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
	if *udp == true {
		protocol = "udp"
	}
	hostport := flag.Args()

	hostportArray := strings.SplitN(hostport[0], ":", 2)
	host := hostportArray[0]
	port := hostportArray[1]
	index := 0
	for index = 1; index < *timeout; index++ {
		connected := raw_connect(host, port, protocol)
		if connected {
			os.Exit(0)
		}
		time.Sleep(time.Duration(1 * time.Second))
	}
	os.Exit(1)
}
