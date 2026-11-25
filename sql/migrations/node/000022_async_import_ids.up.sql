-- async_import_ids holds the newest import ID of each async destination. Importing job
-- statuses reference a row by its id instead of repeating the import ID.
CREATE TABLE IF NOT EXISTS async_import_ids (
    destination_id TEXT NOT NULL,
    id             TEXT NOT NULL,
    import_id      TEXT NOT NULL,
    PRIMARY KEY (destination_id, id)
);
