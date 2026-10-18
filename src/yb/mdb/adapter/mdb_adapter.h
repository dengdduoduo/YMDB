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

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

int YBCMdbInit();

int YBCMdbInitCluster(const char* master_addrs, const char* keyspace);

// Owns its import transactions; rejects an active caller read/write scope unchanged.
int YBCMdbImportFile(const char* file_path);

typedef void (*YBCMdbRowCallback)(
    const char* name, const char* value, void* ctx);

int YBCMdbExecuteSparql(
    const char* query_text,
    YBCMdbRowCallback row_callback,
    void* ctx);

// Byte spans and row values are borrowed until the callback returns. Embedded NULs are valid.
typedef struct {
  const char* data;
  size_t size;
} YBCMdbString;

typedef enum {
  YBC_MDB_UNBOUND,
  YBC_MDB_IRI,
  YBC_MDB_BLANK,
  YBC_MDB_LITERAL
} YBCMdbTermKind;

typedef struct {
  YBCMdbTermKind kind;
  YBCMdbString lexical;
  YBCMdbString language;
  YBCMdbString datatype;
} YBCMdbValue;

typedef int (*YBCMdbSchemaCallback)(const YBCMdbString* columns, size_t count, void* ctx);
typedef int (*YBCMdbResultRowCallback)(const YBCMdbValue* values, size_t count, void* ctx);

// SELECT: schema once, including empty results, then one callback per solution (even empty).
// Nonempty language/datatype describe literal identity; unbound values have no lexical bytes.
// Callbacks must not reenter queries or mutate the shared model. Zero-count arrays may be null.
// Direct-IRI DESCRIBE emits subject/predicate/object rows; other non-SELECT forms are rejected.
// Callback nonzero aborts. Events are provisional until this function returns zero; callers
// that require all-or-nothing publication must buffer them. INSERT emits no result events.
// The old name/value API remains ABI-compatible but cannot represent unbound/empty rows.
int YBCMdbExecuteSparqlRows(
    const char* query_text, YBCMdbSchemaCallback schema_callback,
    YBCMdbResultRowCallback row_callback, void* ctx);

#ifdef __cplusplus
}
#endif
