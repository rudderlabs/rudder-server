alter table tracked_users_reports
    rename column userid_hll to tracked_identifiers_hll;

alter table tracked_users_reports
    rename column identified_anonymousid_hll to merged_identifiers_hll;

-- we are dropping this column because it is not used from the last 2 releases, so it should be safe to drop it
alter table tracked_users_reports
    drop column anonymousid_hll;
