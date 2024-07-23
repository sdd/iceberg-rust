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

use anyhow::{anyhow, Result};
use iceberg_test_utils::manage_performance_test_infrastructure::{
    init_project, remove_project, start_project, stop_project,
};
use std::collections::VecDeque;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args: VecDeque<_> = env::args().collect();
    args.pop_front();

    let Some(cmd) = args.pop_front() else {
        return Err(anyhow!(
            "No command specified. Must be either start, stop, init or cleanup"
        ));
    };

    match cmd.as_str() {
        "start" => {
            println!("Starting performance testing environment");
            start_project();
        }

        "stop" => {
            println!("Stopping performance testing environment");
            stop_project();
        }

        "init" => {
            println!("Initializing performance testing environment");
            init_project();
        }

        "remove" => {
            println!("Removing performance testing environment");
            remove_project();
        }

        _ => {
            return Err(anyhow!(format!("Unknown command '{}'", cmd)));
        }
    }

    Ok(())
}
