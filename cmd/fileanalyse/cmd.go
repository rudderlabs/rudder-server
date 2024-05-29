package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

func main() {
	// Open the CSV file
	file, err := os.Open("example.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read the header row to get the number of columns
	header, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading header row:", err)
		return
	}
	numColumns := len(header)

	// Initialize an array to hold the maximum size per column
	maxSizes := make([]int, numColumns)

	// Loop through each row in the CSV file
	for {
		// Read the next row
		row, err := reader.Read()
		if err == io.EOF {
			// End of file
			break
		} else if err != nil {
			fmt.Println("Error reading row:", err)
			return
		}

		// Loop through each column in the row
		for i := 0; i < numColumns; i++ {
			// Get the length of the current cell
			cellLength := len(row[i])

			// Update the maximum size for this column if necessary
			if cellLength > maxSizes[i] {
				maxSizes[i] = cellLength
			}
		}
	}

	// Print out the maximum size per column
	fmt.Println("Maximum size per column:")
	for i := 0; i < numColumns; i++ {
		fmt.Printf("%s: %d\n", header[i], maxSizes[i])
	}
}
