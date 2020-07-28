---
--- Event Models
---

DROP TABLE IF EXISTS event_models;

DROP INDEX IF EXISTS event_model_write_key_index;

---
---  Schema Versions
---

DROP TABLE IF EXISTS schema_versions;

DROP INDEX IF EXISTS event_model_id_index;
DROP INDEX IF EXISTS event_model_id_schema_hash_index;