DO
$do$
DECLARE
   dropstmt text;
BEGIN
FOR dropstmt  IN
      SELECT 'DROP TABLE "' || TABLE_NAME || '"' as col1
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_NAME LIKE 'failed_keys_%'
LOOP
   EXECUTE dropstmt;
END LOOP;

END
$do$;