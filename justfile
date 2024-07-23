# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

default:
    just --list --unsorted

perf_tests_start:
    cd crates/iceberg/testdata/performance && cargo run -p iceberg_test_utils --bin perf_testing -- start && cd -

perf_tests_stop:
    cd crates/iceberg/testdata/performance && cargo run -p iceberg_test_utils --bin perf_testing -- stop && cd -

perf_tests_init:
    cd crates/iceberg/testdata/performance && cargo run -p iceberg_test_utils --bin perf_testing -- init && cd -

perf_tests_remove:
    cd crates/iceberg/testdata/performance && cargo run -p iceberg_test_utils --bin perf_testing -- remove && cd -

perf_tests_run:
    cargo criterion --benches

