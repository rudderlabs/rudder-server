CREATE OR REPLACE PROCEDURE sync_schemas(source_schema varchar, destination_schema varchar)
    LANGUAGE plpgsql
AS
$$
DECLARE
table_record           RECORD;
    column_record          RECORD;
    source_tables          varchar(max) := '';
    table_name             varchar(max);
    source_table_name      varchar(max);
    destination_table_name varchar(max);
    tables_count           int          := 0;
    i                      int          := 1;
    columns                varchar(max);
    insert_query           varchar(max);
    create_table_query     varchar(max);
    table_delimiter        varchar(max) = '~~';
    column_delimiter       varchar(max) = ',';
BEGIN
    /* Getting the source tables */
FOR table_record IN SELECT *
                    FROM INFORMATION_SCHEMA.tables
                    WHERE table_schema = source_schema
                      AND table_type = 'BASE TABLE'
                        LOOP
            source_tables :=
                    concat(source_tables, table_record.table_name || table_delimiter);
END LOOP;

    /* Trimming the table_delimiter from source_tables */
    source_tables := rtrim(source_tables, table_delimiter);
    RAISE INFO 'TABLES %', source_tables;


    /* Getting the tables count */
    tables_count := REGEXP_COUNT(source_tables, table_delimiter) + 1;

    /* Iterating through the tables */
    while i <= tables_count
        LOOP
            table_name := SPLIT_PART(source_tables, table_delimiter, i);
            source_table_name := source_schema || '.' || table_name;
            destination_table_name := destination_schema || '.' || table_name;
            columns := '';
            RAISE INFO 'Syncing from source table % to destination table %', source_table_name, destination_table_name;

FOR column_record IN SELECT col_name
                     FROM pg_get_cols(source_table_name) cols(view_schema name, view_name name,
                                                              col_name name,
                                                              col_type varchar,
                                                              col_num int)
                     order by col_num
                         LOOP
                    columns := concat(columns, column_record.col_name || column_delimiter);
END LOOP;

            columns := rtrim(columns, column_delimiter);

BEGIN
                create_table_query := 'CREATE TABLE IF NOT EXISTS ' || destination_table_name || ' ( LIKE ' ||
                                      source_table_name || ' );';
                RAISE INFO 'Create table query: => %', create_table_query;

EXECUTE create_table_query;

RAISE INFO 'Created table from source % to destination %', source_table_name, destination_table_name;
END;

BEGIN
                insert_query := 'INSERT INTO ' || destination_table_name || ' ( ' || columns || ' ) ' ||
                                ' ( SELECT * FROM ' || source_table_name || ');';
                RAISE INFO 'Insert query: => %', insert_query;

EXECUTE insert_query;

RAISE INFO 'Synced from source table % to destination table %', source_table_name, destination_table_name;
END;
            i := i + 1;
END loop;
END;
$$
