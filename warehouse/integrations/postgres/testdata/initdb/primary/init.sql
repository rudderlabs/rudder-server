ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 5;
ALTER SYSTEM SET max_wal_senders = 5;
ALTER SYSTEM SET wal_sender_timeout = '60s';
SELECT pg_reload_conf();
