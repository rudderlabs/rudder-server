package loadfiles

import (
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

/*
BenchmarkGroupBySize/few_tables-12                  8164            147395 ns/op
BenchmarkGroupBySize/few_tables_big_files-12                  99          12253217 ns/op
BenchmarkGroupBySize/many_tables_few_files-12               1950            623900 ns/op
BenchmarkGroupBySize/many_tables_many_files-12                37          30811622 ns/op
BenchmarkGroupBySize/many_tables_many_many_files-12            1        3283076458 ns/op
*/
func BenchmarkGroupBySize(b *testing.B) {
	lf := &LoadFileGenerator{
		Logger: logger.NOP,
		Conf:   config.New(),
	}

	// Helper function to create staging files with specified table sizes
	createStagingFiles := func(count int, tableSizes map[string]int64) []*model.StagingFile {
		files := make([]*model.StagingFile, count)
		for i := 0; i < count; i++ {
			files[i] = &model.StagingFile{
				ID:            int64(i),
				BytesPerTable: tableSizes,
			}
		}
		return files
	}

	manyTables := map[string]int64{
		"table1":  100 * 1024,
		"table2":  200 * 1024,
		"table3":  300 * 1024,
		"table4":  400 * 1024,
		"table5":  500 * 1024,
		"table6":  600 * 1024,
		"table7":  700 * 1024,
		"table8":  800 * 1024,
		"table9":  900 * 1024,
		"table10": 1000 * 1024,
	}
	testCases := []struct {
		name      string
		fileCount int
		tables    map[string]int64
	}{
		{
			name:      "few_tables",
			fileCount: 960,
			tables: map[string]int64{
				"table1": 100 * 1024,
				"table2": 200 * 1024,
			},
		},
		{
			name:      "few_tables_big_files",
			fileCount: 960,
			tables: map[string]int64{
				"table1": 100 * 1024 * 1024,
				"table2": 120 * 1024 * 1024,
			},
		},
		{
			name:      "many_tables_few_files",
			fileCount: 960,
			tables:    manyTables,
		},
		{
			name:      "many_tables_many_files",
			fileCount: 9600,
			tables:    manyTables,
		},
		{
			name:      "many_tables_many_many_files",
			fileCount: 96000,
			tables:    manyTables,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			files := createStagingFiles(tc.fileCount, tc.tables)

			b.ResetTimer() // Reset timer to exclude setup time
			for i := 0; i < b.N; i++ {
				groups := lf.GroupStagingFiles(files, 128)
				// Prevent compiler from optimizing away the result
				if len(groups) == 0 {
					b.Fatal("expected non-zero groups")
				}
			}
		})
	}
}
