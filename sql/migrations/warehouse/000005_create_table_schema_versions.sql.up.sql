--
-- wh_schema_versions
--

CREATE TABLE IF NOT EXISTS wh_schema_versions (
                                          id BIGSERIAL PRIMARY KEY,
                                          wh_schema_id BIGSERIAL NOT NULL,
                                          schema JSONB NOT NULL,
                                          created_at TIMESTAMP NOT NULL);

CREATE INDEX IF NOT EXISTS wh_schema_versions_wh_schema_id_index ON wh_schema_versions (wh_schema_id);

-- create a trigger function inserts into wh_schema_versions for every insert/update on wh_schemas table
CREATE OR REPLACE FUNCTION warehouse_schema_history()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN
    INSERT INTO wh_schema_versions("wh_schema_id","schema","created_at")
    VALUES(new.id, new.schema, new.updated_at);
    RETURN new;
END ;
$$;

-- create a trigger when there is a insert or update on wh_schemas
create trigger warehouse_schema_history_trigger AFTER INSERT OR UPDATE ON "wh_schemas" FOR EACH ROW EXECUTE PROCEDURE warehouse_schema_history()


