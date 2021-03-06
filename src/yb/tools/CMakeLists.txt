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
#
# The following only applies to changes made to this file as part of YugaByte development.
#
# Portions Copyright (c) YugaByte, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.
#

set(LINK_LIBS
  yb_client
  log
  consensus
  tserver
  yb_common
  yb_fs
  yb_util
  gutil
  cfile
  tablet
  ${YB_BASE_LIBS}
)

add_library(yb_tools_util
  data_gen_util.cc)
target_link_libraries(yb_tools_util
  ${LINK_LIBS})

add_executable(insert-generated-rows insert-generated-rows.cc)
target_link_libraries(insert-generated-rows
  yb_tools_util
  ${LINK_LIBS})

add_executable(yb-admin yb-admin.cc)
target_link_libraries(yb-admin
  ${LINK_LIBS})

add_executable(yb-ts-cli ts-cli.cc)
target_link_libraries(yb-ts-cli
  ${LINK_LIBS})

add_library(fs_tool fs_tool.cc)
target_link_libraries(fs_tool
  gutil
  yb_common
  server_common
  consensus
  tablet)

add_executable(yb-fs_list fs_list-tool.cc)
target_link_libraries(yb-fs_list
  ${LINK_LIBS}
  fs_tool)

add_executable(yb-fs_dump fs_dump-tool.cc)
target_link_libraries(yb-fs_dump
  ${LINK_LIBS}
  fs_tool)

add_library(ysck
    ysck.cc
    ysck_remote.cc
)
target_link_libraries(ysck
  yb_client
  master_proto
  server_base_proto
  tserver_proto
  tserver_service_proto
  ${YB_BASE_LIBS}
)

add_executable(yb-ysck yb-ysck.cc)
target_link_libraries(yb-ysck
  ysck
)

add_library(yb-generate_partitions yb-generate_partitions.cc)
target_link_libraries(yb-generate_partitions
  yb_client
  yb_util
  master_proto
)

add_executable(yb-generate_partitions_main yb-generate_partitions_main.cc)
target_link_libraries(yb-generate_partitions_main
  yb-generate_partitions
)

add_library(bulk_load_docdb_util bulk_load_docdb_util.cc)
target_link_libraries(bulk_load_docdb_util
  yb_docdb
)

add_executable(yb-bulk_load yb-bulk_load.cc)
target_link_libraries(yb-bulk_load
  gutil
  rocksdb
  ql_protocol_proto
  yb_client
  bulk_load_docdb_util
  yb-generate_partitions
)

add_executable(yb-pbc-dump pbc-dump.cc)
target_link_libraries(yb-pbc-dump
  ${LINK_LIBS}
)

set(YB_TEST_LINK_LIBS
  ysck
  yb_tools_util
  yb-generate_partitions
  integration-tests
  yb_docdb_test_common
  ql_util
  ${YB_MIN_TEST_LIBS})
ADD_YB_TEST(ysck-test)
ADD_YB_TEST(yb-bulk_load-test)
ADD_YB_TEST_DEPENDENCIES(yb-bulk_load-test
  yb-generate_partitions_main
  yb-bulk_load)
ADD_YB_TEST(ysck_remote-test)
ADD_YB_TEST(yb-admin-test)
ADD_YB_TEST_DEPENDENCIES(yb-admin-test
  yb-admin)
ADD_YB_TEST(yb-ts-cli-test)
ADD_YB_TEST_DEPENDENCIES(yb-ts-cli-test
  yb-ts-cli)

add_executable(run-with-timeout run-with-timeout.cc)

# If ${YB_BASE_LIBS} is removed below, there is an error in static linking mode
# (but no error with dynamic linking!)
target_link_libraries(run-with-timeout yb_util ${YB_BASE_LIBS})
