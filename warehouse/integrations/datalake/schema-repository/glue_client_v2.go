package schemarepository

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluev2 "github.com/aws/aws-sdk-go-v2/service/glue"
	gluev2types "github.com/aws/aws-sdk-go-v2/service/glue/types"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type GlueClientV2 struct {
	client *gluev2.Client
}

// Conversion helpers between v1 and v2 types will be implemented below.

func (g *GlueClientV2) GetTables(ctx context.Context, databaseName string) ([]Table, error) {
	input := &gluev2.GetTablesInput{
		DatabaseName: aws.String(databaseName),
	}
	var tables []Table
	var nextToken *string
	for {
		if nextToken != nil {
			input.NextToken = nextToken
		}
		resp, err := g.client.GetTables(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, t := range resp.TableList {
			tables = append(tables, fromV2Table(t))
		}
		nextToken = resp.NextToken
		if nextToken == nil {
			break
		}
	}
	return tables, nil
}

func (g *GlueClientV2) CreateDatabase(ctx context.Context, databaseName string) error {
	input := &gluev2.CreateDatabaseInput{
		DatabaseInput: &gluev2types.DatabaseInput{Name: aws.String(databaseName)},
	}
	_, err := g.client.CreateDatabase(ctx, input)
	return err
}

func (g *GlueClientV2) CreateTable(ctx context.Context, databaseName string, table Table) error {
	input := &gluev2.CreateTableInput{
		DatabaseName: aws.String(databaseName),
		TableInput:   toV2TableInput(table),
	}
	_, err := g.client.CreateTable(ctx, input)
	return err
}

func (g *GlueClientV2) UpdateTable(ctx context.Context, databaseName string, table Table) error {
	input := &gluev2.UpdateTableInput{
		DatabaseName: aws.String(databaseName),
		TableInput:   toV2TableInput(table),
	}
	_, err := g.client.UpdateTable(ctx, input)
	return err
}

func (g *GlueClientV2) DeleteDatabase(ctx context.Context, databaseName string) error {
	input := &gluev2.DeleteDatabaseInput{Name: aws.String(databaseName)}
	_, err := g.client.DeleteDatabase(ctx, input)
	return err
}

func (g *GlueClientV2) RefreshPartitions(ctx context.Context, tableName string, loadFiles []warehouseutils.LoadFile) error {
	partitionFolderRegex := regexp.MustCompile(`.*/(?P<name>.*)=(?P<value>.*)$`)
	var (
		locationsToPartition = make(map[string]*gluev2types.PartitionInput)
		locationFolder       string
		err                  error
		partitionInputs      []gluev2types.PartitionInput
		partitionGroups      map[string]string
	)

	for _, loadFile := range loadFiles {
		if locationFolder, err = url.QueryUnescape(warehouseutils.GetS3LocationFolder(loadFile.Location)); err != nil {
			return fmt.Errorf("unescape location folder: %w", err)
		}
		if _, ok := locationsToPartition[locationFolder]; ok {
			continue
		}
		if partitionGroups, err = warehouseutils.CaptureRegexGroup(partitionFolderRegex, locationFolder); err != nil {
			continue
		}
		locationsToPartition[locationFolder] = &gluev2types.PartitionInput{
			StorageDescriptor: &gluev2types.StorageDescriptor{
				Location: aws.String(locationFolder),
			},
			Values: []string{partitionGroups["value"]},
		}
	}

	// Assume databaseName is required; you may need to pass it as an argument or store it in the adapter
	databaseName := "default" // TODO: Replace with actual database name if available

	for _, partition := range locationsToPartition {
		_, err := g.client.GetPartition(ctx, &gluev2.GetPartitionInput{
			DatabaseName:    aws.String(databaseName),
			PartitionValues: partition.Values,
			TableName:       aws.String(tableName),
		})
		if err != nil {
			var notFound *gluev2types.EntityNotFoundException
			if !errors.As(err, &notFound) {
				return fmt.Errorf("get partition: %w", err)
			}
			partitionInputs = append(partitionInputs, *partition)
		}
	}
	if len(partitionInputs) == 0 {
		return nil
	}
	_, err = g.client.BatchCreatePartition(ctx, &gluev2.BatchCreatePartitionInput{
		DatabaseName:       aws.String(databaseName),
		PartitionInputList: partitionInputs,
		TableName:          aws.String(tableName),
	})
	if err != nil {
		return fmt.Errorf("batch create partitions: %w", err)
	}
	return nil
}

func fromV2Table(t gluev2types.Table) Table {
	var cols []Column
	if t.StorageDescriptor != nil && t.StorageDescriptor.Columns != nil {
		for _, c := range t.StorageDescriptor.Columns {
			cols = append(cols, Column{Name: aws.ToString(c.Name), Type: aws.ToString(c.Type)})
		}
	}
	return Table{Name: aws.ToString(t.Name), Columns: cols}
}

func toV2TableInput(table Table) *gluev2types.TableInput {
	var cols []gluev2types.Column
	for _, c := range table.Columns {
		cols = append(cols, gluev2types.Column{Name: aws.String(c.Name), Type: aws.String(c.Type)})
	}
	return &gluev2types.TableInput{
		Name: aws.String(table.Name),
		StorageDescriptor: &gluev2types.StorageDescriptor{
			Columns: cols,
		},
	}
}
