package sqlutil

import (
	"database/sql"
	"fmt"
	"io"
	"text/tabwriter"
)

// PrintRowsToTable prints the rows to the output in a table format.
func PrintRowsToTable(rows *sql.Rows, output io.Writer) error {
	w := tabwriter.NewWriter(output, 0, 0, 1, ' ', tabwriter.AlignRight|tabwriter.Debug)
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("getting column names: %w", err)
	}

	for _, column := range columns {
		fmt.Fprint(w, "\t"+column)
	}
	fmt.Fprintln(w, "\t")
	for range columns {
		fmt.Fprint(w, "\t---")
	}
	fmt.Fprintln(w, "\t")

	values := make([]any, len(columns))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}
		for _, value := range values {
			switch value := value.(type) {
			case nil:
				fmt.Fprint(w, "\tNULL")
			case []byte:
				fmt.Fprint(w, "\t"+string(value))
			default:
				fmt.Fprintf(w, "\t%v", value)
			}
		}
		fmt.Fprintln(w, "\t")
	}
	w.Flush()
	return rows.Err()
}
