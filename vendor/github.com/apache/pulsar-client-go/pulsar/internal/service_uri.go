// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	BinaryService = "pulsar"
	HTTPService   = "http"
	HTTPSService  = "https"
	SSLService    = "ssl"
	BinaryPort    = 6650
	BinaryTLSPort = 6651
	HTTPPort      = 80
	HTTPSPort     = 443
)

type PulsarServiceURI struct {
	ServiceName  string
	ServiceInfos []string
	ServiceHosts []string
	servicePath  string
	URL          *url.URL
}

func NewPulsarServiceURIFromURIString(uri string) (*PulsarServiceURI, error) {
	u, err := fromString(uri)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return u, nil
}

func NewPulsarServiceURIFromURL(url *url.URL) (*PulsarServiceURI, error) {
	u, err := fromURL(url)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return u, nil
}

func fromString(uriStr string) (*PulsarServiceURI, error) {
	if uriStr == "" || len(uriStr) == 0 {
		return nil, errors.New("service uriStr string is null")
	}
	if strings.Contains(uriStr, "[") && strings.Contains(uriStr, "]") {
		// deal with ipv6 address
		hosts := strings.FieldsFunc(uriStr, splitURI)
		if len(hosts) > 1 {
			// deal with ipv6 address
			firstHost := hosts[0]
			lastHost := hosts[len(hosts)-1]
			hasPath := strings.Contains(lastHost, "/")
			path := ""
			if hasPath {
				idx := strings.Index(lastHost, "/")
				path = lastHost[idx:]
			}
			firstHost += path
			url, err := url.Parse(firstHost)
			if err != nil {
				return nil, err
			}
			serviceURI, err := fromURL(url)
			if err != nil {
				return nil, err
			}
			var mHosts []string
			var multiHosts []string
			mHosts = append(mHosts, serviceURI.ServiceHosts[0])
			mHosts = append(mHosts, hosts[1:]...)

			for _, v := range mHosts {
				h, err := validateHostName(serviceURI.ServiceName, serviceURI.ServiceInfos, v)
				if err == nil {
					multiHosts = append(multiHosts, h)
				} else {
					return nil, err
				}
			}

			return &PulsarServiceURI{
				serviceURI.ServiceName,
				serviceURI.ServiceInfos,
				multiHosts,
				serviceURI.servicePath,
				serviceURI.URL,
			}, nil
		}
	}

	url, err := url.Parse(uriStr)
	if err != nil {
		return nil, err
	}

	return fromURL(url)
}

func fromURL(url *url.URL) (*PulsarServiceURI, error) {
	if url == nil {
		return nil, errors.New("service url instance is null")
	}

	if url.Host == "" || len(url.Host) == 0 {
		return nil, errors.New("service host is null")
	}

	var serviceName string
	var serviceInfos []string
	scheme := url.Scheme
	if scheme != "" {
		scheme = strings.ToLower(scheme)
		schemeParts := strings.Split(scheme, "+")
		serviceName = schemeParts[0]
		serviceInfos = schemeParts[1:]
	}

	var serviceHosts []string
	hosts := strings.FieldsFunc(url.Host, splitURI)
	for _, v := range hosts {
		h, err := validateHostName(serviceName, serviceInfos, v)
		if err == nil {
			serviceHosts = append(serviceHosts, h)
		} else {
			return nil, err
		}
	}

	return &PulsarServiceURI{
		serviceName,
		serviceInfos,
		serviceHosts,
		url.Path,
		url,
	}, nil
}

func splitURI(r rune) bool {
	return r == ',' || r == ';'
}

func validateHostName(serviceName string, serviceInfos []string, hostname string) (string, error) {
	uri, err := url.Parse("dummyscheme://" + hostname)
	if err != nil {
		return "", err
	}
	host := uri.Hostname()
	if strings.Contains(hostname, "[") && strings.Contains(hostname, "]") {
		host = fmt.Sprintf("[%s]", host)
	}
	if host == "" || uri.Scheme == "" {
		return "", errors.New("Invalid hostname : " + hostname)
	}

	port := uri.Port()
	if uri.Port() == "" {
		p := getServicePort(serviceName, serviceInfos)
		if p == -1 {
			return "", fmt.Errorf("invalid port : %d", p)
		}
		port = fmt.Sprint(p)
	}
	result := host + ":" + port
	_, _, err = net.SplitHostPort(result)
	if err != nil {
		return "", err
	}
	return result, nil
}

func getServicePort(serviceName string, serviceInfos []string) int {
	switch strings.ToLower(serviceName) {
	case BinaryService:
		if len(serviceInfos) == 0 {
			return BinaryPort
		} else if len(serviceInfos) == 1 && strings.ToLower(serviceInfos[0]) == SSLService {
			return BinaryTLSPort
		}
	case HTTPService:
		return HTTPPort
	case HTTPSService:
		return HTTPSPort
	}
	return -1
}
