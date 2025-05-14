package schemarepository

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	gluev1 "github.com/aws/aws-sdk-go/service/glue"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type GlueClientV1 struct {
	client *gluev1.Glue
}

func (g *GlueClientV1) GetTables(ctx context.Context, databaseName string) ([]Table, error) {
	input := &gluev1.GetTablesInput{
		DatabaseName: aws.String(databaseName),
	}
	var tables []Table
	var nextToken *string
	for {
		if nextToken != nil {
			input.NextToken = nextToken
		}
		resp, err := g.client.GetTablesWithContext(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, t := range resp.TableList {
			tables = append(tables, fromV1Table(t))
		}
		nextToken = resp.NextToken
		if nextToken == nil {
			break
		}
	}
	return tables, nil
}

func (g *GlueClientV1) CreateDatabase(ctx context.Context, databaseName string) error {
	input := &gluev1.CreateDatabaseInput{
		DatabaseInput: &gluev1.DatabaseInput{Name: aws.String(databaseName)},
	}
	_, err := g.client.CreateDatabaseWithContext(ctx, input)
	return err
}

func (g *GlueClientV1) CreateTable(ctx context.Context, databaseName string, table Table) error {
	input := &gluev1.CreateTableInput{
		DatabaseName: aws.String(databaseName),
		TableInput:   toV1TableInput(table),
	}
	_, err := g.client.CreateTableWithContext(ctx, input)
	return err
}

func (g *GlueClientV1) UpdateTable(ctx context.Context, databaseName string, table Table) error {
	input := &gluev1.UpdateTableInput{
		DatabaseName: aws.String(databaseName),
		TableInput:   toV1TableInput(table),
	}
	_, err := g.client.UpdateTableWithContext(ctx, input)
	return err
}

func (g *GlueClientV1) GetPartitionWithContext(ctx context.Context, input *gluev1.GetPartitionInput) (*gluev1.GetPartitionOutput, error) {
	return g.client.GetPartitionWithContext(ctx, input)
}

func (g *GlueClientV1) BatchCreatePartitionWithContext(ctx context.Context, input *gluev1.BatchCreatePartitionInput) (*gluev1.BatchCreatePartitionOutput, error) {
	return g.client.BatchCreatePartitionWithContext(ctx, input)
}

func (g *GlueClientV1) DeleteDatabase(ctx context.Context, databaseName string) error {
	input := &gluev1.DeleteDatabaseInput{Name: &databaseName}
	_, err := g.client.DeleteDatabaseWithContext(ctx, input)
	return err
}

func (g *GlueClientV1) RefreshPartitions(ctx context.Context, tableName string, loadFiles []warehouseutils.LoadFile) error {
	// This logic is adapted from the original repository implementation
	partitionFolderRegex := regexp.MustCompile(`.*/(?P<name>.*)=(?P<value>.*)$`)
	var (
		locationsToPartition = make(map[string]*gluev1.PartitionInput)
		locationFolder       string
		err                  error
		partitionInputs      []*gluev1.PartitionInput
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
		locationsToPartition[locationFolder] = &gluev1.PartitionInput{
			StorageDescriptor: &gluev1.StorageDescriptor{
				Location: aws.String(locationFolder),
			},
			Values: []*string{aws.String(partitionGroups["value"])},
		}
	}

	for _, partition := range locationsToPartition {
		_, err := g.client.GetPartitionWithContext(ctx, &gluev1.GetPartitionInput{
			DatabaseName:    aws.String(*g.client.Config.Region), // You may need to pass the correct database name
			PartitionValues: partition.Values,
			TableName:       aws.String(tableName),
		})
		if err != nil {
			var entityNotFoundException *gluev1.EntityNotFoundException
			if !errors.As(err, &entityNotFoundException) {
				return fmt.Errorf("get partition: %w", err)
			}
			partitionInputs = append(partitionInputs, partition)
		}
	}
	if len(partitionInputs) == 0 {
		return nil
	}
	_, err = g.client.BatchCreatePartitionWithContext(ctx, &gluev1.BatchCreatePartitionInput{
		DatabaseName:       aws.String(*g.client.Config.Region), // You may need to pass the correct database name
		PartitionInputList: partitionInputs,
		TableName:          aws.String(tableName),
	})
	if err != nil {
		return fmt.Errorf("batch create partitions: %w", err)
	}
	return nil
}

func fromV1Table(t *gluev1.TableData) Table {
	var cols []Column
	if t.StorageDescriptor != nil && t.StorageDescriptor.Columns != nil {
		for _, c := range t.StorageDescriptor.Columns {
			var colName, colType string
			if c.Name != nil {
				colName = *c.Name
			}
			if c.Type != nil {
				colType = *c.Type
			}
			cols = append(cols, Column{Name: colName, Type: colType})
		}
	}
	var tableName string
	if t.Name != nil {
		tableName = *t.Name
	}
	return Table{Name: tableName, Columns: cols}
}

func toV1TableInput(table Table) *gluev1.TableInput {
	var cols []*gluev1.Column
	for _, c := range table.Columns {
		cols = append(cols, &gluev1.Column{
			Name: aws.String(c.Name),
			Type: aws.String(c.Type),
		})
	}
	return &gluev1.TableInput{
		Name: aws.String(table.Name),
		StorageDescriptor: &gluev1.StorageDescriptor{
			Columns: cols,
		},
	}
}
