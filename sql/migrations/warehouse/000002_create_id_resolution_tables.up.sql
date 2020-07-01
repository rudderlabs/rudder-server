
--
-- wh_table_uploads
--

ALTER TABLE wh_table_uploads
    ADD COLUMN location TEXT;

--
-- wh_identity_merge_rules
--

CREATE TABLE IF NOT EXISTS rudder_identity_merge_rules (
    id BIGSERIAL PRIMARY KEY,
    merge_property_1_type VARCHAR(64) NOT NULL,
    merge_property_1_value TEXT NOT NULL,
    merge_property_2_type VARCHAR(64),
    merge_property_2_value TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW());


--
-- wh_identity_mappings
--

CREATE TABLE IF NOT EXISTS rudder_identity_mappings (
    id BIGSERIAL PRIMARY KEY,
    merge_property_type VARCHAR(64) NOT NULL,
    merge_property_value TEXT NOT NULL,
    rudder_id VARCHAR(64) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW());


ALTER TABLE rudder_identity_mappings
    ADD CONSTRAINT unique_merge_property UNIQUE (merge_property_type, merge_property_value);
