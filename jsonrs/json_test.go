package jsonrs_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/jsonrs"
)

func TestJSONCommonFunctionality(t *testing.T) {
	run := func(t *testing.T, name string) {
		t.Run(name, func(t *testing.T) {
			c := config.New()
			c.Set("Json.Library", name)
			j := jsonrs.New(c)

			t.Run("marshall", func(t *testing.T) {
				type test struct {
					A string `json:"a"`
				}
				data, err := j.Marshal(test{A: "a"})
				require.NoError(t, err)
				require.Equal(t, `{"a":"a"}`, string(data))

				t.Run("json.RawMessage", func(t *testing.T) {
					type test struct {
						A json.RawMessage `json:"a"`
					}
					data, err := j.Marshal(test{A: json.RawMessage(`{"a":"a"}`)})
					require.NoError(t, err)
					require.Equal(t, `{"a":{"a":"a"}}`, string(data))
				})
			})

			t.Run("unmarshall", func(t *testing.T) {
				type test struct {
					A string `json:"a"`
				}
				var v test
				err := j.Unmarshal([]byte(`{"a":"a"}`), &v)
				require.NoError(t, err)
				require.Equal(t, "a", v.A)

				t.Run("json.RawMessage", func(t *testing.T) {
					type test struct {
						A json.RawMessage `json:"a"`
					}
					var v test
					err := j.Unmarshal([]byte(`{"a":{"a":"a"}}`), &v)
					require.NoError(t, err)
					require.Equal(t, `{"a":"a"}`, string(v.A))
				})
			})

			t.Run("marshalToString", func(t *testing.T) {
				type test struct {
					A string `json:"a"`
				}
				data, err := j.MarshalToString(test{A: "a"})
				require.NoError(t, err)
				require.Equal(t, `{"a":"a"}`, data)
			})

			t.Run("unmarshal and marshal unicode", func(t *testing.T) {
				expect := func(input, output string, expectedErr ...error) {
					t.Run(input, func(t *testing.T) {
						tpl := `{"batch":[{"anonymousId":"anon_id","sentAt":"2019-08-12T05:08:30.909Z","type":"here"}]}`
						var v any
						err := j.Unmarshal(bytes.Replace([]byte(tpl), []byte("here"), []byte(input), 1), &v)
						if len(expectedErr) > 0 && expectedErr[0] != nil {
							require.Error(t, err, "expected an error")
							require.ErrorContains(t, err, expectedErr[0].Error())
						} else {
							require.NoError(t, err)
							res, err := j.Marshal(v)
							require.NoError(t, err)
							require.Equal(t, string(bytes.Replace([]byte(tpl), []byte("here"), []byte(output), 1)), string(res))

						}
					})
				}
				escapeChar := "�"
				if name == jsonrs.JsoniterLib {
					escapeChar = `\ufffd` // unique behaviour: jsoniter uses the \ufffd instead of � that all other libraries use
				}
				ecTimes := func(i int) string {
					return strings.Repeat(escapeChar, i)
				}

				expect("☺b☺", "☺b☺")
				expect("", "")
				expect("abc", "abc")
				expect("\uFDDD", "\uFDDD")
				expect("a\xffb", "a"+ecTimes(1)+"b")
				expect("\xC0\xAF", ecTimes(2))
				expect("\xE0\x80\xAF", ecTimes(3))
				expect("\xed\xa0\x80", ecTimes(3))
				expect("\xFC\x80\x80\x80\x80\xAF", ecTimes(6))
				expect(`\uD83D\ub000`, string([]byte{239, 191, 189, 235, 128, 128}))
				expect(`\uD83D\ude04`, `😄`)
				expect(`\u4e2d\u6587`, `中文`)
				expect(`\ud83d\udc4a`, `👊`)
				expect(`\U0001f64f`, "", errors.New("invalid"))
				expect(`\uD83D\u00`, "", errors.New(``))
				expect(`\ud800`, "�")
				expect(`\uDEAD`, "�")
			})
		})
	}
	run(t, jsonrs.StdLib)
	run(t, jsonrs.SonnetLib)
	run(t, jsonrs.JsoniterLib)
}
