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

#include <gtest/gtest.h>
#include <limits>
#include <stdexcept>

#include "yb/mdb/import/csv_parser.h"
#include "yb/mdb/import/import.h"
#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/import/rdf_xml_parser.h"

namespace yb::mdb {

TEST(NtParserTest, LiteralEscapesAndStatementBoundariesPreserveIdentity) {
  auto parsed = NtParser::ParseLine(R"(<http://ex/s> <http://ex/p> "a\" b\u0041\n"@EN .)");
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  EXPECT_EQ(parsed->obj, "a\" bA\n");
  EXPECT_EQ(parsed->object_type, "@EN");
  auto statements = NtParser::SplitStatements(
      R"(<http://ex/s> <http://ex/p> "a.b" . _:b <http://ex/p> "1.0"^^<urn:dt> .)");
  ASSERT_TRUE(statements.ok());
  ASSERT_EQ(statements->size(), 2);
  EXPECT_TRUE(NtParser::ParseLine((*statements)[0]).ok());
  EXPECT_TRUE(NtParser::ParseLine((*statements)[1]).ok());
  statements = NtParser::SplitStatements(
      "\n<urn:s> <urn:p> \"same\"@en .\r\n<urn:t> <urn:p> \"same\"@fr .\n");
  ASSERT_TRUE(statements.ok());
  ASSERT_EQ(statements->size(), 2);
  EXPECT_TRUE(NtParser::ParseLine((*statements)[0]).ok());
  EXPECT_TRUE(NtParser::ParseLine((*statements)[1]).ok());
  EXPECT_FALSE(NtParser::ParseLine(R"(<urn:s> <urn:p> "x"@ .)").ok());
  EXPECT_FALSE(NtParser::ParseLine(R"(<urn:s> <urn:p> "\uD800" .)").ok());
}

TEST(NtParserTest, ParseTripleWithUris) {
  auto result = NtParser::ParseLine(
      "<http://ex/s> <http://ex/p> <http://ex/o> .");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->sub, "http://ex/s");
  ASSERT_EQ(result->pred, "http://ex/p");
  ASSERT_EQ(result->obj, "http://ex/o");
  ASSERT_EQ(result->sub_type, TripleTermType::URI);
  ASSERT_EQ(result->obj_type, TripleTermType::URI);
}

TEST(NtParserTest, ParseTripleWithLiteral) {
  auto result = NtParser::ParseLine(
      "<http://ex/s> <http://ex/name> \"Alice\" .");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->obj, "Alice");
  ASSERT_EQ(result->obj_type, TripleTermType::LITERAL);
}

TEST(NtParserTest, ParseTripleWithLanguageLiteral) {
  auto result = NtParser::ParseLine(
      "<http://ex/s> <http://ex/name> \"Charlie\"@en .");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->obj, "Charlie");
  ASSERT_EQ(result->obj_type, TripleTermType::LITERAL);
  ASSERT_EQ(result->object_type, "@en");
}

TEST(NtParserTest, ParseTripleWithTypedLiteral) {
  auto result = NtParser::ParseLine(
      "<http://ex/s> <http://ex/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->obj, "30");
  ASSERT_EQ(result->obj_type, TripleTermType::LITERAL);
  ASSERT_EQ(result->object_type, "http://www.w3.org/2001/XMLSchema#integer");
}

TEST(NtParserTest, ParseTripleWithBlankNodeSubject) {
  auto result = NtParser::ParseLine(
      "_:b1 <http://ex/p> <http://ex/o> .");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->sub, "_:b1");
  ASSERT_EQ(result->sub_type, TripleTermType::BLANK);
}

TEST(NtParserTest, RejectInvalidLine) {
  auto result = NtParser::ParseLine("this is not valid");
  ASSERT_FALSE(result.ok());
}

TEST(NtParserTest, RejectMissingDot) {
  auto result = NtParser::ParseLine(
      "<http://ex/s> <http://ex/p> <http://ex/o>");
  ASSERT_FALSE(result.ok());
}

TEST(CsvParserTest, ParseLineWithQuotedValue) {
  CsvParseConfig config;
  config.has_header = false;
  auto result = CsvParser::ParseLine(
      "http://ex/s,http://ex/name,\"Alice\"", config);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->sub, "http://ex/s");
  ASSERT_EQ(result->pred, "http://ex/name");
  ASSERT_EQ(result->obj, "Alice");
  ASSERT_EQ(result->obj_type, TripleTermType::LITERAL);
}

TEST(CsvParserTest, ParseLineWithUriObject) {
  CsvParseConfig config;
  config.has_header = false;
  auto result = CsvParser::ParseLine(
      "http://ex/s,http://ex/type,http://ex/Person", config);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->obj, "http://ex/Person");
  ASSERT_EQ(result->obj_type, TripleTermType::URI);
}

TEST(CsvParserTest, ParseLineWithQuotesContainingComma) {
  CsvParseConfig config;
  config.has_header = false;
  auto result = CsvParser::ParseLine(
      "http://ex/s,http://ex/desc,\"hello, world\"", config);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->obj, "hello, world");
}

TEST(RdfXmlParserTest, ParseDescriptionWithLiteral) {
  const char* xml = R"(
    <?xml version="1.0"?>
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns:ex="http://example.org/">
      <rdf:Description rdf:about="http://example.org/s1">
        <ex:name>Alice</ex:name>
      </rdf:Description>
    </rdf:RDF>
  )";
  auto result = RdfXmlParser::ParseString(xml);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1);
  ASSERT_EQ((*result)[0].sub, "http://example.org/s1");
  ASSERT_EQ((*result)[0].pred, "http://example.org/name");
  ASSERT_EQ((*result)[0].obj, "Alice");
  ASSERT_EQ((*result)[0].obj_type, TripleTermType::LITERAL);
}

TEST(RdfXmlParserTest, ParseDescriptionWithResourceRef) {
  const char* xml = R"(
    <?xml version="1.0"?>
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns:ex="http://example.org/">
      <rdf:Description rdf:about="http://example.org/s1">
        <ex:knows rdf:resource="http://example.org/s2"/>
      </rdf:Description>
    </rdf:RDF>
  )";
  auto result = RdfXmlParser::ParseString(xml);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1);
  ASSERT_EQ((*result)[0].obj, "http://example.org/s2");
  ASSERT_EQ((*result)[0].obj_type, TripleTermType::URI);
}

TEST(RdfXmlParserTest, ParseDescriptionWithDatatype) {
  const char* xml = R"(
    <?xml version="1.0"?>
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns:ex="http://example.org/">
      <rdf:Description rdf:about="http://example.org/s1">
        <ex:age rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">30</ex:age>
      </rdf:Description>
    </rdf:RDF>
  )";
  auto result = RdfXmlParser::ParseString(xml);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ((*result)[0].obj, "30");
  ASSERT_EQ((*result)[0].obj_type, TripleTermType::LITERAL);
  ASSERT_EQ((*result)[0].object_type, "http://www.w3.org/2001/XMLSchema#integer");
}

TEST(IdMapTest, GetOrCreate) {
  IdMap id_map;
  uint64_t id1 = id_map.GetOrCreate("http://ex/s1");
  uint64_t id2 = id_map.GetOrCreate("http://ex/s1");
  ASSERT_EQ(id1, id2);
  uint64_t id3 = id_map.GetOrCreate("http://ex/s2");
  ASSERT_NE(id1, id3);
}

TEST(IdMapTest, Lookup) {
  IdMap id_map;
  uint64_t id = id_map.GetOrCreate("http://ex/foo");
  ASSERT_EQ(id_map.Lookup(id), "http://ex/foo");
}

TEST(IdMapTest, Contains) {
  IdMap id_map;
  ASSERT_FALSE(id_map.Contains("http://ex/foo"));
  id_map.GetOrCreate("http://ex/foo");
  ASSERT_TRUE(id_map.Contains("http://ex/foo"));
}

TEST(IdMapTest, RestoreOutOfOrderAndContinueAllocation) {
  IdMap id_map;
  ASSERT_TRUE(id_map.Restore(1000, "http://ex/last"));
  ASSERT_TRUE(id_map.Restore(7, "http://ex/first"));
  ASSERT_EQ(id_map.GetOrCreate("http://ex/first"), 7);
  ASSERT_EQ(id_map.GetOrCreate("http://ex/new"), 1001);
  ASSERT_EQ(id_map.Lookup(1000), "http://ex/last");
  ASSERT_EQ(id_map.Size(), 3);
  ASSERT_TRUE(id_map.Lookup(8).empty());
}

TEST(IdMapTest, RejectConflictingRestoredMappings) {
  IdMap id_map;
  ASSERT_TRUE(id_map.Restore(7, "http://ex/a"));
  ASSERT_TRUE(id_map.Restore(7, "http://ex/a"));
  ASSERT_FALSE(id_map.Restore(7, "http://ex/b"));
  ASSERT_FALSE(id_map.Restore(8, "http://ex/a"));
  ASSERT_FALSE(id_map.Restore(0, "http://ex/zero"));
  ASSERT_EQ(id_map.Size(), 1);
  ASSERT_EQ(id_map.MaxId(), 7);
  ASSERT_EQ(id_map.GetOrCreate("http://ex/b"), 8);
}

TEST(IdMapTest, RestoreEmptyTermAndDetectExhaustion) {
  IdMap id_map;
  ASSERT_TRUE(id_map.Restore(5, ""));
  ASSERT_TRUE(id_map.Contains(""));
  ASSERT_EQ(id_map.GetOrCreate(""), 5);
  ASSERT_TRUE(id_map.Restore(std::numeric_limits<uint64_t>::max(), "http://ex/max"));
  ASSERT_THROW(id_map.GetOrCreate("http://ex/overflow"), std::overflow_error);
}

}  // namespace yb::mdb
