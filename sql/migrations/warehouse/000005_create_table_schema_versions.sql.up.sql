--
-- wh_schema_versions
--

CREATE TABLE IF NOT EXISTS wh_schema_versions (
                                          id BIGSERIAL PRIMARY KEY,
                                          wh_schema_id BIGSERIAL NOT NULL,
                                          source_id VARCHAR(64) NOT NULL,
                                          namespace VARCHAR(64) NOT NULL,
                                          destination_id VARCHAR(64) NOT NULL,
                                          destination_type VARCHAR(64) NOT NULL,
                                          schema JSONB NOT NULL,
                                          error TEXT,
                                          changed_at TIMESTAMP NOT NULL);

CREATE INDEX IF NOT EXISTS wh_schema_versions_wh_schema_id_index ON wh_schema_versions (wh_schema_id);

-- create a trigger function inserts into wh_schema_versions for every insert/update on wh_schemas table
CREATE OR REPLACE FUNCTION warehouse_schema_history()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN
    INSERT INTO wh_schema_versions("wh_schema_id","source_id", "namespace","destination_id", "destination_type","schema","error","changed_at")
    VALUES(new.id, new.source_id, new.namespace, new.destination_id, new.destination_type, new.schema, new.error, new.updated_at);
    RETURN new;
END ;
$$;

-- create a trigger when there is a insert or update on wh_schemas
-- creates a transaction. if the trigger already exists then does nothing
DO $$
    BEGIN
        create trigger  warehouse_schema_history_trigger AFTER INSERT OR UPDATE ON "wh_schemas" FOR EACH ROW EXECUTE PROCEDURE warehouse_schema_history();
    EXCEPTION
        when others then null;
    END $$


