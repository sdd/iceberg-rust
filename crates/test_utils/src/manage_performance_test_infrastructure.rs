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

use crate::docker::DockerCompose;
use std::sync::RwLock;

pub static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

pub fn get_docker_compose() -> DockerCompose {
    DockerCompose::new(
        "iceberg-performance-tests",
        format!(
            "{}/../iceberg/testdata/performance",
            env!("CARGO_MANIFEST_DIR")
        ),
    )
}

/// Must be run once the performance testing docker compose project has been started
/// by previously calling start_project(). Changes made here will
/// persist after calling stop_project() but will need to be re-run
/// after a call to remove_project().
pub fn init_project() {
    let docker_compose = get_docker_compose();

    // Run some SQL in spark to create the Iceberg table(s)
    // for the tests
    spark_run_sql_file(
        &docker_compose,
        "spark_scripts/setup.sql",
        "Create DB and tables",
    );

    // Runs some commands on spark to insert the test data
    // into the Iceberg table
    spark_submit_file(
        &docker_compose,
        "spark_scripts/insert_data.py",
        "Insert the test data",
    );
}

/// Creates the docker-compose project for the performance tests.
/// Must be run at least once to set up the performance testing
/// docker-compose project on the testing host machine.
/// Needs to be re-run if either stop_project() or remove_project()
/// have been called, to restart the project for another test run.
pub fn start_project() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();

    let docker_compose = get_docker_compose();

    docker_compose.run();
    guard.replace(docker_compose);
}

/// Run this at the end of a performance test run, or at the end of a
/// persistent performance testing session, to stop all the
/// containers that are running within the performance testing
/// docker-compose project. Does not delete the test data or
/// containers.
pub fn stop_project() {
    let mut docker_compose = get_docker_compose();

    docker_compose.stop();
}

/// Run this if you are done with performance testing and want to clean
/// up any test data files and remove the docker-compose containers
/// and project.
pub fn remove_project() {
    let mut docker_compose = get_docker_compose();

    docker_compose.remove();
}

pub fn spark_submit_file(
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

pub fn spark_run_sql_file(
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
