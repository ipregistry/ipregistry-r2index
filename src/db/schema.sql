CREATE TABLE IF NOT EXISTS files (
    id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT NOT NULL,
    entity TEXT NOT NULL,
    extension TEXT NOT NULL,
    media_type TEXT NOT NULL,
    remote_path TEXT NOT NULL,
    remote_filename TEXT NOT NULL,
    remote_version TEXT NOT NULL,
    metadata_path TEXT,
    size INTEGER,
    checksum_md5 TEXT,
    checksum_sha1 TEXT,
    checksum_sha256 TEXT,
    checksum_sha512 TEXT,
    extra TEXT,
    deprecated INTEGER DEFAULT 0,
    deprecation_reason TEXT DEFAULT '',
    created INTEGER NOT NULL,
    updated INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS file_tags (
    file_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (file_id, tag),
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_files_category ON files(category);
CREATE INDEX IF NOT EXISTS idx_files_entity ON files(entity);
CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension);
CREATE INDEX IF NOT EXISTS idx_files_media_type ON files(media_type);
CREATE INDEX IF NOT EXISTS idx_files_deprecated ON files(deprecated);
CREATE UNIQUE INDEX IF NOT EXISTS idx_files_remote_unique ON files(remote_path, remote_filename, remote_version);
CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag);
