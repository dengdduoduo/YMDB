// Copyright (c) YugabyteDB, Inc.

#pragma once

#include "yb/client/schema.h"

namespace yb::mdb {

// All six permutations have the same physical schema. The third column must
// be part of the key, otherwise distinct triples sharing a prefix overwrite.
inline void ConfigureRdfTableSchema(client::YBSchemaBuilder* builder) {
  builder->AddColumn("c0")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
  builder->AddColumn("c1")->Type(DataType::INT64)->PrimaryKey()->NotNull();
  builder->AddColumn("c2")->Type(DataType::INT64)->PrimaryKey()->NotNull();
  TableProperties properties;
  properties.SetTransactional(true);
  builder->SetTableProperties(properties);
}

}  // namespace yb::mdb
