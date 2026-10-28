// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#pragma once

#include <cstddef>
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

int YBCMdbInit();

// Close the process-wide MDB adapter before thread-local/static teardown.
// Returns nonzero on callback re-entry; repeated successful calls are safe.
int YBCMdbShutdown();

// Replaces the prior adapter connection after releasing its dependent model.
// After argument validation, initialization failures leave it disconnected.
// Null arguments leave an existing connection unchanged. Successful
// reinitialization preserves query configuration.
int YBCMdbInitCluster(const char* master_addrs, const char* keyspace);

int YBCMdbImportFile(const char* file_path);

// Calling-thread profile for the last streaming query, valid until its next
// query. Empty for non-streaming requests. Counts operations, not wire RPCs.
const char* YBCMdbGetLastQueryProfile();

typedef void (*YBCMdbRowCallback)(
    const char* name, const char* value, void* ctx);

enum YBCMdbResultKind { YBC_MDB_BINDINGS, YBC_MDB_BOOLEAN, YBC_MDB_GRAPH };
enum YBCMdbTermKind { YBC_MDB_UNBOUND, YBC_MDB_IRI, YBC_MDB_BLANK, YBC_MDB_LITERAL };

struct YBCMdbColumn {
  const char* name;
  const char* internal_name;
};
struct YBCMdbTerm {
  YBCMdbTermKind kind;
  const char* value;
  const char* datatype;
  const char* language;
};
// Callback data is borrowed for the duration of each call. Output is
// provisional until execution returns zero; discard all output on failure.
struct YBCMdbResultCallbacks {
  void (*begin)(YBCMdbResultKind, const YBCMdbColumn*, size_t, void*);
  void (*row)(const YBCMdbTerm*, size_t, void*);
  void (*boolean_result)(int, void*);
  void (*triple)(const YBCMdbTerm*, void*);
  void (*end)(void*);
};
int YBCMdbExecuteSparqlWithResults(
    const char* query_text, const YBCMdbResultCallbacks* callbacks, void* ctx);

int YBCMdbExecuteSparql(
    const char* query_text,
    YBCMdbRowCallback row_callback,
    void* ctx);

#ifdef __cplusplus
}
#endif
