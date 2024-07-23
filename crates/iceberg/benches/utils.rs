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

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_test_utils::docker::DockerCompose;
use std::collections::HashMap;

pub fn get_docker_compose() -> DockerCompose {
    DockerCompose::new(
        "iceberg-performance-tests",
        format!(
            "{}/../iceberg/testdata/performance",
            env!("CARGO_MANIFEST_DIR")
        ),
    )
}

pub async fn build_catalog() -> RestCatalog {
    let docker_compose = get_docker_compose();

    // determine which ports on the host that docker has exposed the specified port to for the given containers
    let rest_api_host_port = docker_compose.get_host_port("rest", 8181);
    let haproxy_host_port = docker_compose.get_host_port("haproxy", 9080);

    let catalog_uri = format!("http://localhost:{}", rest_api_host_port);
    let haproxy_uri = format!("http://localhost:{}", haproxy_host_port);

    let user_props = HashMap::from_iter(
        vec![
            (S3_ENDPOINT.to_string(), haproxy_uri),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]
        .into_iter(),
    );

    RestCatalog::new(
        RestCatalogConfig::builder()
            .uri(catalog_uri)
            .props(user_props)
            .build(),
    )
}
