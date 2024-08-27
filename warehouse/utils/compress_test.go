package warehouseutils

import (
	"bytes"
	"testing"
)

func TestCompress(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{"Empty input", []byte{}, false},
		{"Small input", []byte("Hello, World!"), false},
		{"Large input", bytes.Repeat([]byte("GoLang"), 1000), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := Compress(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Compress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Verify that the compressed output is smaller than the input (for non-empty data)
			if len(tt.input) > 0 && len(compressed) >= len(tt.input) {
				t.Errorf("Expected compressed size to be smaller, got input length %d and compressed length %d", len(tt.input), len(compressed))
			}

			// Verify the compressed data can be decompressed back to the original input
			if !tt.wantErr {
				decompressed, err := Decompress(compressed)
				if err != nil {
					t.Errorf("Decompress() error = %v", err)
				}
				if !bytes.Equal(decompressed, tt.input) {
					t.Errorf("Decompressed data does not match original, got %v, want %v", decompressed, tt.input)
				}
			}
		})
	}
}

// TestDecompress tests the Decompress function
func TestDecompress(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []byte
		wantErr bool
	}{
		{"Valid compressed data", func() []byte {
			var buf bytes.Buffer
			gzipWriter := gzip.NewWriter(&buf)
			gzipWriter.Write([]byte("Hello, Go!"))
			gzipWriter.Close()
			return buf.Bytes()
		}(), []byte("Hello, Go!"), false},
		{"Invalid compressed data", []byte("Not Gzip data"), nil, true},
		{"Empty input", []byte{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decompress(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decompress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("Decompress() = %v, want %v", got, tt.want)
			}
		})
	}
}
