export interface Env {
  API_TOKEN: string;
  CACHE_MAX_AGE?: string;
  DB: D1Database;
}

export interface Variables {
  requestId: string;
}

export interface FileRecord {
  category: string;
  checksum_md5: string | null;
  checksum_sha1: string | null;
  checksum_sha256: string | null;
  checksum_sha512: string | null;
  created: number;
  deprecated: boolean;
  deprecation_reason: string;
  entity: string;
  extension: string;
  extra: Record<string, unknown> | null;
  id: string;
  media_type: string;
  metadata_path: string | null;
  name: string | null;
  remote_filename: string;
  remote_path: string;
  remote_version: string;
  size: number | null;
  tags?: string[];
  updated: number;
}

export interface CreateFileInput {
  category: string;
  checksum_md5?: string;
  checksum_sha1?: string;
  checksum_sha256?: string;
  checksum_sha512?: string;
  entity: string;
  extension: string;
  extra?: Record<string, unknown>;
  media_type: string;
  metadata_path?: string;
  name?: string;
  remote_filename: string;
  remote_path: string;
  remote_version: string;
  size?: number;
  tags?: string[];
}

export interface UpdateFileInput {
  category?: string;
  checksum_md5?: string;
  checksum_sha1?: string;
  checksum_sha256?: string;
  checksum_sha512?: string;
  deprecated?: boolean;
  deprecation_reason?: string;
  entity?: string;
  extension?: string;
  extra?: Record<string, unknown>;
  media_type?: string;
  metadata_path?: string;
  name?: string;
  remote_filename?: string;
  remote_path?: string;
  remote_version?: string;
  size?: number;
  tags?: string[];
}

export interface SearchParams {
  category?: string;
  deprecated?: string;
  entity?: string;
  extension?: string;
  group_by?: string;
  limit?: string;
  media_type?: string;
  offset?: string;
  tags?: string;
}

export interface GroupedResult {
  count: number;
  value: string;
}

export interface SearchResult {
  files: FileRecord[];
  total: number;
}

export interface GroupedSearchResult {
  groups: GroupedResult[];
  total: number;
}

export interface FileIndexEntry {
  checksums: {
    md5?: string;
    sha1?: string;
    sha256?: string;
    sha512?: string;
  };
  file_size?: string;
  last_updated?: string;
  name?: string;
  [key: string]: unknown;
}

export type NestedIndex = Record<string, Record<string, FileIndexEntry>>;
