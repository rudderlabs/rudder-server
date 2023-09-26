package tableprinter

import (
	"math"
	"strconv"
	"strings"
)

var units = [...]string{"K", "M", "B", "T"}

/*
references & credits:
- https://github.com/DeyV/gotools/blob/master/numbers.go
- https://golang.org/pkg/math/#Modf
*/
func nearestThousandFormat(num float64) string {
	if math.Abs(num) < 999.5 {
		xNum := formatNumber(num)
		xNumStr := xNum[:len(xNum)-3]
		return xNumStr
	}

	xNum := formatNumber(num)
	xNumStr := xNum[:len(xNum)-3]
	xNumCleaned := strings.Replace(xNumStr, ",", " ", -1)
	xNumSlice := strings.Fields(xNumCleaned)
	count := len(xNumSlice) - 2

	xPart := units[count]

	afterDecimal := ""
	if xNumSlice[1][0] != 0 {
		afterDecimal = "." + string(xNumSlice[1][0])
	}
	final := xNumSlice[0] + afterDecimal + xPart
	return final
}

func formatNumber(input float64) string {
	x := roundInt(input)
	xFormatted := numberFormat(float64(x), 2, ".", ",")
	return xFormatted
}

func roundInt(input float64) int {
	var result float64

	if input < 0 {
		result = math.Ceil(input - 0.5)
	} else {
		result = math.Floor(input + 0.5)
	}

	i, _ := math.Modf(result)

	return int(i)
}

func numberFormat(number float64, decimals int, decPoint, thousandsSep string) string {
	if math.IsNaN(number) || math.IsInf(number, 0) {
		number = 0
	}

	var ret string
	var negative bool

	if number < 0 {
		number *= -1
		negative = true
	}

	d, fract := math.Modf(number)

	if decimals <= 0 {
		fract = 0
	} else {
		pow := math.Pow(10, float64(decimals))
		fract = roundPrec(fract*pow, 0)
	}

	if thousandsSep == "" {
		ret = strconv.FormatFloat(d, 'f', 0, 64)
	} else if d >= 1 {
		var x float64
		for d >= 1 {
			d, x = math.Modf(d / 1000)
			x = x * 1000
			ret = strconv.FormatFloat(x, 'f', 0, 64) + ret
			if d >= 1 {
				ret = thousandsSep + ret
			}
		}
	} else {
		ret = "0"
	}

	fracts := strconv.FormatFloat(fract, 'f', 0, 64)

	for i := len(fracts); i < decimals; i++ {
		fracts = "0" + fracts
	}

	ret += decPoint + fracts

	if negative {
		ret = "-" + ret
	}
	return ret
}

func roundPrec(x float64, prec int) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return x
	}

	sign := 1.0
	if x < 0 {
		sign = -1
		x *= -1
	}

	var rounder float64
	pow := math.Pow(10, float64(prec))
	intermed := x * pow
	_, frac := math.Modf(intermed)

	if frac >= 0.5 {
		rounder = math.Ceil(intermed)
	} else {
		rounder = math.Floor(intermed)
	}

	return rounder / pow * sign
}
