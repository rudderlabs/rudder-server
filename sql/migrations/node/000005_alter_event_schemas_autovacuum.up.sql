---
--- Event Models
---

ALTER TABLE event_models SET (autovacuum_vacuum_scale_factor = 0.01);
ALTER TABLE event_models SET (autovacuum_vacuum_cost_delay = 0);

---
---  Schema Versions
---

ALTER TABLE schema_versions SET (autovacuum_vacuum_scale_factor = 0.01);
ALTER TABLE schema_versions SET (autovacuum_vacuum_cost_delay = 0);
