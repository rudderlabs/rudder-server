--
-- wh_schemas
--

ALTER TABLE wh_schemas ADD COLUMN IF NOT EXISTS updated_at timestamp;

-- adds a constraint to wh_schemas which allows us to do upsert(update/insert) on non primary columns
-- creates a transaction. if the constraint already exists then does nothing
DO $$
    BEGIN
        ALTER TABLE wh_schemas ADD CONSTRAINT unique_wh_identifier UNIQUE("source_id","destination_id","namespace");
    EXCEPTION
        when others then null;
    END $$
