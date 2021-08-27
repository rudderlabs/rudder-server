DELETE FROM wh_table_uploads
	WHERE id in (
		SELECT id
			FROM (
				SELECT
					ROW_NUMBER() OVER (PARTITION BY wh_upload_id, table_name ORDER BY id DESC) AS row_number,
					t.id
				FROM
					wh_table_uploads t

			) grouped_table_uploads
			WHERE
				grouped_table_uploads.row_number > 1
	);

create or replace function create_constraint_if_not_exists (
    t_name text, c_name text, constraint_sql text
)
returns void AS
$$
begin
    -- Look for our constraint
    if not exists (select constraint_name
                   from information_schema.constraint_column_usage
                   where table_name = t_name  and constraint_name = c_name) then
        execute constraint_sql;
    end if;
end;
$$ language 'plpgsql';

SELECT create_constraint_if_not_exists(
        'wh_table_uploads',
        'unique_table_upload_wh_upload',
        'ALTER TABLE wh_table_uploads ADD CONSTRAINT unique_table_upload_wh_upload UNIQUE (wh_upload_id, table_name);')