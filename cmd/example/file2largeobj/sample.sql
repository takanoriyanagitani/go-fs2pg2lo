DROP TABLE IF EXISTS go_fs2pg2lo_files;

CREATE TABLE IF NOT EXISTS go_fs2pg2lo_files (
	id               BIGSERIAL PRIMARY KEY,
	filename         TEXT NOT NULL,
	size             BIGINT NOT NULL,
	small_content    BYTEA,
	large_content_id OID,
	CONSTRAINT go_fs2pg2lo_files_chk CHECK(
		1 = (
			(CASE WHEN small_content IS NULL THEN 0 ELSE 1 END)
			+ (CASE WHEN large_content_id IS NULL THEN 0 ELSE 1 END)
		)
	)
)
