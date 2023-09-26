// Copyright (c) 2017 Alexey Popov
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

/*
Package simpletable allows to generate and display ascii tables in the terminal, f.e.:

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

There are the following key features:

* Declarative style. Have to write more code, and hell with it.

* Styling. With 6 predefined styles: MySql-like (default), compact, compact lite, markdown, rounded and unicode. And you can change it.

* Header and footer. Separeted from table body.

* Multiline cells support. See https://github.com/alexeyco/simpletable/blob/master/_example/04-multiline/main.go for example.

* cellInterface content alignment. Left, right or center.

* Row spanning. By analogy with the way it is done in HTML. See Cell.Span attribute description.

* Fast! Really fast, see https://github.com/alexeyco/simpletable/blob/master/_example/03-benchmarks-with-others

Examples: https://github.com/alexeyco/simpletable/tree/master/_example
*/
package simpletable
