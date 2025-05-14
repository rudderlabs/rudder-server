package schemarepository

type Table struct {
	Name    string
	Columns []Column
	// Add more fields as needed (e.g., StorageDescriptor, PartitionKeys, etc.)
}

type Column struct {
	Name string
	Type string
}

type Partition struct {
	Values   []string
	Location string
}
