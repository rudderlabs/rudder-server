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

ALTER TABLE wh_table_uploads ADD CONSTRAINT unique_table_upload_wh_upload UNIQUE (wh_upload_id, table_name)
