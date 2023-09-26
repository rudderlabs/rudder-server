// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package zmssvctoken

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"
)

// getSalt returns a random salt
func getSalt() (string, error) {
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", buf), nil
}

// defaultHostname returns a default value for hostname
// or an empty string in case of errors
func defaultHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}

// defaultIP returns a default IP address
// or an empty string in case of errors
func defaultIP() string {
	hostname := defaultHostname()
	if hostname != "" {
		addrs, err := net.LookupHost(hostname)
		if err == nil {
			for _, addr := range addrs {
				ip := net.ParseIP(addr)
				if ip.To4() == nil {
					continue
				}
				if ip.IsLoopback() {
					continue
				}
				return ip.String()
			}
		}
	}
	return ""
}
