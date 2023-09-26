# Simple tables in terminal with Go

[![Travis](https://img.shields.io/travis/alexeyco/simpletable.svg)](https://travis-ci.org/alexeyco/simpletable)&nbsp;[![Coverage Status](https://coveralls.io/repos/github/alexeyco/simpletable/badge.svg?branch=master)](https://coveralls.io/github/alexeyco/simpletable?branch=master)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/alexeyco/simpletable)](https://goreportcard.com/report/github.com/alexeyco/simpletable)&nbsp;[![GoDoc](https://godoc.org/github.com/alexeyco/simpletable?status.svg)](https://godoc.org/github.com/alexeyco/simpletable)

This package allows to generate and display ascii tables in the terminal, f.e.:

```
+----+------------------+--------------+-----------------------------+------+
| #  |       NAME       |    PHONE     |            EMAIL            | QTTY |
+----+------------------+--------------+-----------------------------+------+
|  1 | Newton G. Goetz  | 252-585-5166 | NewtonGGoetz@dayrep.com     |   10 |
|  2 | Rebecca R. Edney | 865-475-4171 | RebeccaREdney@armyspy.com   |   12 |
|  3 | John R. Jackson  | 810-325-1417 | JohnRJackson@armyspy.com    |   15 |
|  4 | Ron J. Gomes     | 217-450-8568 | RonJGomes@rhyta.com         |   25 |
|  5 | Penny R. Lewis   | 870-794-1666 | PennyRLewis@rhyta.com       |    5 |
|  6 | Sofia J. Smith   | 770-333-7379 | SofiaJSmith@armyspy.com     |    3 |
|  7 | Karlene D. Owen  | 231-242-4157 | KarleneDOwen@jourrapide.com |   12 |
|  8 | Daniel L. Love   | 978-210-4178 | DanielLLove@rhyta.com       |   44 |
|  9 | Julie T. Dial    | 719-966-5354 | JulieTDial@jourrapide.com   |    8 |
| 10 | Juan J. Kennedy  | 908-910-8893 | JuanJKennedy@dayrep.com     |   16 |
+----+------------------+--------------+-----------------------------+------+
|                                                           Subtotal |  150 |
+----+------------------+--------------+-----------------------------+------+
```

There are the following key features:
* **Declarative style.** _Have to write more code, and hell with it._
* **Styling.** _With 7 predefined styles: MySql-like (default), compact, compact lite, compact classic, markdown, 
  rounded and unicode. And you can change it._
* **Header and footer.** _Separated from table body._
* **Multiline cells support.** _See [_example/main.go/_example/04-multiline/main.go](https://github.com/alexeyco/simpletable/blob/master/_example/04-multiline/main.go) for example._
* **Cell content alignment.** _Left, right or center._
* **Row spanning.** _By analogy with the way it is done in HTML. See `Cell.Span` attribute 
  description._
* **Fast!** _Really fast, see [_example/main.go/_example/03-benchmarks-with-others](https://github.com/alexeyco/simpletable/blob/master/_example/03-benchmarks-with-others)._

## Installation
```
$ go get -u github.com/alexeyco/simpletable
```
To run unit tests:
```
$ cd $GOPATH/src/github.com/alexeyco/simpletable
$ go test -cover
```
To run benchmarks:
```
$ cd $GOPATH/src/github.com/alexeyco/simpletable
$ go test -bench=.
```
Comparison with similar libraries see [_example/main.go/_example/03-benchmarks-with-others](https://github.com/alexeyco/simpletable/blob/master/_example/03-benchmarks-with-others)

## Basic example
```go
package main

import (
	"fmt"

	"github.com/alexeyco/simpletable"
)

var (
	data = [][]interface{}{
		{1, "Newton G. Goetz", 532.7},
		{2, "Rebecca R. Edney", 1423.25},
		{3, "John R. Jackson", 7526.12},
		{4, "Ron J. Gomes", 123.84},
		{5, "Penny R. Lewis", 3221.11},
	}
)

func main() {
	table := simpletable.New()

	table.Header = &simpletable.Header{
		Cells: []*simpletable.Cell{
			{Align: simpletable.AlignCenter, Text: "#"},
			{Align: simpletable.AlignCenter, Text: "NAME"},
			{Align: simpletable.AlignCenter, Text: "TAX"},
		},
	}

	subtotal := float64(0)
	for _, row := range data {
		r := []*simpletable.Cell{
			{Align: simpletable.AlignRight, Text: fmt.Sprintf("%d", row[0].(int))},
			{Text: row[1].(string)},
			{Align: simpletable.AlignRight, Text: fmt.Sprintf("$ %.2f", row[2].(float64))},
		}

		table.Body.Cells = append(table.Body.Cells, r)
		subtotal += row[2].(float64)
	}

	table.Footer = &simpletable.Footer{
		Cells: []*simpletable.Cell{
			{},
			{Align: simpletable.AlignRight, Text: "Subtotal"},
			{Align: simpletable.AlignRight, Text: fmt.Sprintf("$ %.2f", subtotal)},
		},
	}

	table.SetStyle(simpletable.StyleCompactLite)
	fmt.Println(table.String())
}
```

Result:
```
 #         NAME            TAX
--- ------------------ ------------
 1   Newton G. Goetz      $ 532.70
 2   Rebecca R. Edney    $ 1423.25
 3   John R. Jackson     $ 7526.12
 4   Ron J. Gomes         $ 123.84
 5   Penny R. Lewis      $ 3221.11
--- ------------------ ------------
             Subtotal   $ 12827.02
```

You can see also [_example/main.go/01-styles-demo/main.go](https://github.com/alexeyco/simpletable/blob/master/_example/01-styles-demo/main.go) for styles demonstration.
```
$ cd $GOPATH/src/github.com/alexeyco/simpletable/_example/01-styles-demo
$ go run main.go
```

More examples:
For more examples see [_example](https://github.com/alexeyco/simpletable/tree/master/_example) directory:
```
$ cd $GOPATH/src/github.com/alexeyco/simpletable/_example
$ ls -F | grep /

01-styles-demo/
02-ugly-span/
03-benchmarks-with-others/
04-multiline/
```

## Styling
There is 6 styles available. To view them, run [_example/main.go/01-styles-demo/main.go](https://github.com/alexeyco/simpletable/blob/master/_example/01-styles-demo/main.go):
```
$ cd $GOPATH/src/github.com/alexeyco/simpletable/_example/01-styles-demo
$ go run main.go
```

## Cell content alignment
You can set cell content alignment:
```go
c := &simpletable.Cell{
	// or simpletable.AlignLeft (default), or simpletable.AlignCenter
	Align:   simpletable.AlignRight, 
	Content: "Subtotal",
}
```

## Column spanning
By analogy with HTML:
```go
c := &simpletable.Cell{
	Span:    2, // Default: 1
	Content: "Subtotal",
}
```
Note: by default `Span` is `1`. If you try to set it to `0`, the value will still be `1`.

## License
```
Copyright (c) 2017 Alexey Popov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
