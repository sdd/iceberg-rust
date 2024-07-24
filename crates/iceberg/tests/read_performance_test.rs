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

//! Performance tests for FileIO S3 table scans

use ctor::{ctor, dtor};
use futures_util::stream::TryStreamExt;
use iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::Catalog;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use std::collections::HashMap;
use std::process::Command;
use std::sync::RwLock;

static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

fn spark_submit(
    docker_compose: &DockerCompose,
    file_path: impl AsRef<str>,
    description: impl AsRef<str>,
) {
    docker_compose.exec_on_container(
        "spark-iceberg".to_string(),
        "spark-submit".to_string(),
        &[format!("/home/iceberg/{}", file_path.as_ref())],
        format!(
            "Submit file '{}' to Spark ({})",
            file_path.as_ref(),
            description.as_ref()
        ),
    )
}

fn spark_sql_file(
    docker_compose: &DockerCompose,
    file_path: impl AsRef<str>,
    description: impl AsRef<str>,
) {
    docker_compose.exec_on_container(
        "spark-iceberg".to_string(),
        "spark-sql".to_string(),
        &[
            "-f".to_string(),
            format!("/home/iceberg/{}", file_path.as_ref()),
        ],
        format!(
            "Submit file '{}' to Spark SQL ({})",
            file_path.as_ref(),
            description.as_ref()
        ),
    )
}

#[ctor]
fn before_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    let docker_compose = DockerCompose::new(
        normalize_test_name(module_path!()),
        format!("{}/testdata/performance", env!("CARGO_MANIFEST_DIR")),
    );
    // docker_compose.run();

    // Set up the DB and tables
    // spark_sql_file(&docker_compose, "spark_scripts/setup.sql", "Create DB and tables");
    // spark_submit(&docker_compose, "spark_scripts/insert_data.py", "Insert the test data");

    guard.replace(docker_compose);
}

#[dtor]
fn after_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    // guard.take();
}

async fn get_file_io() -> FileIO {
    set_up();

    let guard = DOCKER_COMPOSE_ENV.read().unwrap();
    let docker_compose = guard.as_ref().unwrap();
    let host_port = docker_compose.get_host_port("rest", 8181);
    let ip_and_port = format!("localhost:{}", host_port);

    FileIOBuilder::new("s3")
        .with_props(vec![
            (S3_ENDPOINT, format!("http://{}", ip_and_port)),
            (S3_ACCESS_KEY_ID, "admin".to_string()),
            (S3_SECRET_ACCESS_KEY, "password".to_string()),
            (S3_REGION, "us-east-1".to_string()),
        ])
        .build()
        .unwrap()
}

async fn get_rest_catalog() -> RestCatalog {
    set_up();

    let guard = DOCKER_COMPOSE_ENV.read().unwrap();
    let docker_compose = guard.as_ref().unwrap();
    let rest_api_host_port = docker_compose.get_host_port("rest", 8181);
    let minio_host_port = docker_compose.get_host_port("minio", 9000);

    let catalog_uri = format!("http://localhost:{}", rest_api_host_port);
    let minio_socket_addr = format!("localhost:{}", minio_host_port);

    let user_props = HashMap::from_iter(
        vec![
            (
                S3_ENDPOINT.to_string(),
                format!("http://{}", minio_socket_addr),
            ),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]
        .into_iter(),
    );

    let catalog = RestCatalog::new(
        RestCatalogConfig::builder()
            .uri(catalog_uri)
            .props(user_props)
            .build(),
    );

    catalog
}

#[tokio::test]
async fn test_file_io_s3_is_exist() {
    let file_io = get_file_io().await;
    assert!(file_io.is_exist("s3://bucket1/").await.unwrap());
}

#[tokio::test]
async fn test_file_io_rest_catalog_connection() {
    let catalog = get_rest_catalog().await;

    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces[0].join("."), "nyc");
}

#[tokio::test]
async fn test_query_all() {
    let catalog = get_rest_catalog().await;
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    let table_idents = catalog.list_tables(&namespaces[0]).await.unwrap();
    let table = catalog.load_table(&table_idents[0]).await.unwrap();

    let scan = table.scan().build().unwrap();
    let record_batches = scan
        .to_arrow()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let record_count = record_batches.iter().fold(0, |acc, rb| acc + rb.num_rows());

    assert_eq!(record_count, 1_000_000);
}
