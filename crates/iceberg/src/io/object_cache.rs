// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ops::Range;
use std::sync::Arc;
use bytes::Bytes;
use parquet::arrow::async_reader::{MetadataFetch, MetadataLoader};
use parquet::file::metadata::ParquetMetaData;

use crate::io::{FileIO, FileRead};
use crate::spec::{
    FormatVersion, Manifest, ManifestFile, ManifestList, SchemaId, SnapshotRef, TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

const DEFAULT_CACHE_SIZE_BYTES: u64 = 1024 * 1024 * 1024; // 32MB

#[derive(Clone, Debug)]
pub(crate) enum CachedItem {
    ManifestList(Arc<ManifestList>),
    Manifest(Arc<Manifest>),
    ParquetMetaData(Arc<ParquetMetaData>),
    ParquetDataBytes(Bytes),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum CachedObjectKey {
    ManifestList((String, FormatVersion, SchemaId)),
    Manifest(String),
    ParquetMetaData(String),
    ParquetDataBytes(String),
}

/// Caches metadata objects deserialized from immutable files
#[derive(Clone, Debug)]
pub struct ObjectCache {
    cache: moka::future::Cache<CachedObjectKey, CachedItem>,
    file_io: FileIO,
    cache_disabled: bool,
}

impl ObjectCache {
    /// Creates a new [`ObjectCache`]
    /// with the default cache size
    pub(crate) fn new(file_io: FileIO) -> Self {
        Self::new_with_capacity(file_io, DEFAULT_CACHE_SIZE_BYTES)
    }

    /// Creates a new [`ObjectCache`]
    /// with a specific cache size
    pub(crate) fn new_with_capacity(file_io: FileIO, cache_size_bytes: u64) -> Self {
        if cache_size_bytes == 0 {
            Self::with_disabled_cache(file_io)
        } else {
            Self {
                cache: moka::future::Cache::new(cache_size_bytes),
                file_io,
                cache_disabled: false,
            }
        }
    }

    /// Creates a new [`ObjectCache`]
    /// with caching disabled
    pub(crate) fn with_disabled_cache(file_io: FileIO) -> Self {
        Self {
            cache: moka::future::Cache::new(0),
            file_io,
            cache_disabled: true,
        }
    }

    /// Retrieves an Arc [`Manifest`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest(&self, manifest_file: &ManifestFile) -> Result<Arc<Manifest>> {
        if self.cache_disabled {
            return manifest_file
                .load_manifest(&self.file_io)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::Manifest(manifest_file.manifest_path.clone());

        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest(manifest_file))
            .await
            .map_err(|err| Error::new(ErrorKind::Unexpected, err.as_ref().message()))?
            .into_value();

        match cache_entry {
            CachedItem::Manifest(arc_manifest) => Ok(arc_manifest),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for key '{:?}' is not a Manifest", key),
            )),
        }
    }

    /// Retrieves an Arc [`ManifestList`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<Arc<ManifestList>> {
        if self.cache_disabled {
            return snapshot
                .load_manifest_list(&self.file_io, table_metadata)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::ManifestList((
            snapshot.manifest_list().to_string(),
            table_metadata.format_version,
            snapshot.schema_id().unwrap(),
        ));
        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest_list(snapshot, table_metadata))
            .await
            .map_err(|err| Error::new(ErrorKind::Unexpected, err.as_ref().message()))?
            .into_value();

        match cache_entry {
            CachedItem::ManifestList(arc_manifest_list) => Ok(arc_manifest_list),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for path '{:?}' is not a Manifest", key),
            )),
        }
    }

    /// Retrieves an Arc [`ParquetMetaData`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    // #[tracing::instrument(level = "info", skip_all)]
    pub(crate) async fn get_parquet_metadata<R: MetadataFetch>(
        &self,
        file_path: &str,
        file_reader: R,
        file_size: u64,
    ) -> Result<Arc<ParquetMetaData>> {
        if self.cache_disabled {
            return self
                .fetch_and_parse_parquet_metadata(file_reader, file_size)
                .await
                .map(|ci| match ci {
                    CachedItem::ParquetMetaData(arc_parquet_metadata) => arc_parquet_metadata,
                    _ => {
                        panic!("Should not happen");
                    }
                });
        }

        let key = CachedObjectKey::ParquetMetaData(file_path.to_string());
        // tracing::info!(?key);
        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_parquet_metadata(file_reader, file_size))
            .await
            .map_err(|err| Error::new(ErrorKind::Unexpected, err.as_ref().message()))?
            .into_value();

        match cache_entry {
            CachedItem::ParquetMetaData(arc_parquet_metadata) => Ok(arc_parquet_metadata),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for path '{:?}' is not a Manifest", key),
            )),
        }
    }

    /// Retrieves a slice of raw bytes from the cached file
    /// or retrieves one from FileIO if not present
    // #[tracing::instrument(level = "info", skip_all)]
    pub(crate) async fn get_file_bytes<R: FileRead>(
        &self,
        file_path: &str,
        file_reader: R,
        file_size: u64,
        range: Range<u64>,
    ) -> Result<Bytes> {
        if self.cache_disabled {
            let bytes =  file_reader.read(range).await?;
            return Ok(bytes);
        }

        let key = CachedObjectKey::ParquetDataBytes(file_path.to_string());
        // tracing::info!(?key);
        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_all_parquet_bytes(file_reader, file_size))
            .await
            .map_err(|err| Error::new(ErrorKind::Unexpected, err.as_ref().message()))?
            .into_value();

        match cache_entry {
            CachedItem::ParquetDataBytes(bytes) => Ok(bytes.slice(range.start as usize..range.end as usize)),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for path '{:?}' is not a FileRawBytes", key),
            )),
        }
    }

    async fn fetch_and_parse_manifest(&self, manifest_file: &ManifestFile) -> Result<CachedItem> {
        let manifest = manifest_file.load_manifest(&self.file_io).await?;

        Ok(CachedItem::Manifest(Arc::new(manifest)))
    }

    async fn fetch_and_parse_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<CachedItem> {
        let manifest_list = snapshot
            .load_manifest_list(&self.file_io, table_metadata)
            .await?;

        Ok(CachedItem::ManifestList(Arc::new(manifest_list)))
    }

    // #[tracing::instrument(level = "info", skip_all)]
    async fn fetch_and_parse_parquet_metadata<R: MetadataFetch>(
        &self,
        file_reader: R,
        file_size: u64,
    ) -> Result<CachedItem> {
        let mut loader = MetadataLoader::load(file_reader, file_size as usize, None).await?;
        loader.load_page_index(true, true).await?;

        Ok(CachedItem::ParquetMetaData(Arc::new(loader.finish())))
    }

    async fn fetch_all_parquet_bytes<R: FileRead>(
        &self,
        file_reader: R,
        file_size: u64,
    ) -> Result<CachedItem> {
        let bytes = file_reader.read(0..file_size).await?;

        Ok(CachedItem::ParquetDataBytes(bytes))
    }
}
