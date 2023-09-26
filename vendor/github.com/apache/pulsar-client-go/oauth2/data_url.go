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

package oauth2

import (
	"encoding/base64"
	"errors"
	"regexp"
)

var errDataURLInvalid = errors.New("invalid data URL")

// https://datatracker.ietf.org/doc/html/rfc2397
var dataURLRegex = regexp.MustCompile("^data:(?P<mimetype>[^;,]+)?(;(?P<charset>charset=[^;,]+))?" +
	"(;(?P<base64>base64))?,(?P<data>.+)")

type dataURL struct {
	url      string
	Mimetype string
	Data     []byte
}

func newDataURL(url string) (*dataURL, error) {
	if !dataURLRegex.Match([]byte(url)) {
		return nil, errDataURLInvalid
	}

	match := dataURLRegex.FindStringSubmatch(url)
	if len(match) != 7 {
		return nil, errDataURLInvalid
	}

	dataURL := &dataURL{
		url: url,
	}

	mimetype := match[dataURLRegex.SubexpIndex("mimetype")]
	if mimetype == "" {
		mimetype = "text/plain"
	}
	dataURL.Mimetype = mimetype

	data := match[dataURLRegex.SubexpIndex("data")]
	if match[dataURLRegex.SubexpIndex("base64")] == "" {
		dataURL.Data = []byte(data)
	} else {
		data, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, err
		}
		dataURL.Data = data
	}

	return dataURL, nil
}
