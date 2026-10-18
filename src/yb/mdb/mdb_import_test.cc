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

#include <array>
#include <bit>
#include <cmath>
#include <limits>
#include <set>

#include <gtest/gtest.h>
#include "query/executor/binding_iter/filter.h"
#include "query/executor/binding_iter/expr_evaluator.h"
#include "query/executor/binding_iter/aggregation/aggregation.h"
#include "query/executor/binding_iter/empty_binding_iter.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding_iter/aggregation/sparql/aggs.h"
#pragma clang diagnostic pop

#include "graph_models/rdf_model/comparisons.h"
#include "query/executor/binding_iter/binding_expr/binding_expr_term.h"
#include "query/executor/binding_iter/binding_expr/binding_expr_var.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_equals.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_same_term.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_is_numeric.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_ceil.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_floor.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_round.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_less.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_less_or_equal.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_greater.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_greater_or_equal.h"
#include "yb/mdb/import/csv_parser.h"
#include "yb/mdb/import/import.h"
#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/import/rdf_xml_parser.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/query/executor/index_nested_loop_join.h"
#include "yb/mdb/query/executor/binding_iter/order_by.h"
#include "yb/mdb/query/executor/binding_iter/sub_select.h"
#include "yb/mdb/query/executor/binding_iter/values.h"
#include "yb/mdb/query/executor/slice.h"
#include "query/parser/op/sparql/op_values.h"
#include "yb/util/test_macros.h"

namespace yb::mdb {

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
  ASSERT_EQ(result->object_language, "en");
  ASSERT_TRUE(result->object_type.empty());
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
  uint64_t id1 = ASSERT_RESULT(id_map.GetOrCreate("http://ex/s1"));
  uint64_t id2 = ASSERT_RESULT(id_map.GetOrCreate("http://ex/s1"));
  ASSERT_EQ(id1, id2);
  uint64_t id3 = ASSERT_RESULT(id_map.GetOrCreate("http://ex/s2"));
  ASSERT_NE(id1, id3);
}

TEST(IdMapTest, Lookup) {
  IdMap id_map;
  uint64_t id = ASSERT_RESULT(id_map.GetOrCreate("http://ex/foo"));
  ASSERT_EQ(id_map.Lookup(id), "http://ex/foo");
}

TEST(IdMapTest, Contains) {
  IdMap id_map;
  ASSERT_FALSE(id_map.Contains("http://ex/foo"));
  ASSERT_RESULT(id_map.GetOrCreate("http://ex/foo"));
  ASSERT_TRUE(id_map.Contains("http://ex/foo"));
}

TEST(IdMapTest, RestoreThenAppendPreservesIds) {
  IdMap ids;
  ASSERT_TRUE(ids.Restore(12, "http://example.org/last"));
  ASSERT_TRUE(ids.Restore(2, "http://example.org/first"));
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("http://example.org/first")), uint64_t{2});
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("http://example.org/new")), uint64_t{13});
  ASSERT_EQ(ids.Lookup(12), "http://example.org/last");
  ASSERT_EQ(ids.Lookup(13), "http://example.org/new");
}

TEST(IdMapTest, RejectConflictingRestoredIds) {
  IdMap ids;
  ASSERT_TRUE(ids.Restore(3, "http://example.org/a"));
  ASSERT_TRUE(ids.Restore(3, "http://example.org/a"));
  ASSERT_FALSE(ids.Restore(3, "http://example.org/b"));
  ASSERT_FALSE(ids.Restore(4, "http://example.org/a"));
  ASSERT_FALSE(ids.Restore(0, "http://example.org/zero"));
  ASSERT_EQ(ids.Lookup(3), "http://example.org/a");
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("http://example.org/next")), uint64_t{4});
}


TEST(IdMapTest, TermKindsPreserveSameLexeme) {
  IdMap ids;
  const std::string text = "https://example.org/value";
  const auto iri = ASSERT_RESULT(ids.GetOrCreate(text));
  const auto literal = ASSERT_RESULT(ids.GetOrCreate(text, TripleTermType::LITERAL));
  const auto blank = ASSERT_RESULT(ids.GetOrCreate(text, TripleTermType::BLANK));
  ASSERT_NE(iri, literal);
  ASSERT_NE(iri, blank);
  ASSERT_NE(literal, blank);
  ASSERT_EQ(ids.Get(text), iri);
  ASSERT_EQ(ids.Get(text, TripleTermType::LITERAL), literal);
  ASSERT_EQ(ids.Lookup(literal), text);
  IdMap restored;
  ASSERT_TRUE(restored.Restore(literal, text, TripleTermType::LITERAL));
  ASSERT_TRUE(restored.Restore(iri, text));
  ASSERT_EQ(ASSERT_RESULT(restored.GetOrCreate(text)), iri);
  ASSERT_EQ(ASSERT_RESULT(restored.GetOrCreate(text, TripleTermType::LITERAL)), literal);
  ASSERT_FALSE(restored.Restore(iri, text, TripleTermType::LITERAL));
}

TEST(IdMapTest, EmptyLiteralSurvivesRestoreAndConflicts) {
  IdMap ids;
  ASSERT_TRUE(ids.Restore(4, "", TripleTermType::LITERAL));
  ASSERT_TRUE(ids.Contains("", TripleTermType::LITERAL));
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("", TripleTermType::LITERAL)), uint64_t{4});
  ASSERT_FALSE(ids.Restore(4, "other", TripleTermType::LITERAL));
  ASSERT_FALSE(ids.Restore(5, "", TripleTermType::LITERAL));
  ASSERT_FALSE(ids.Restore(6, "", TripleTermType::URI));
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("https://example.org/next")), uint64_t{5});
}


TEST(IdMapTest, RejectOutOfRangeRestoredIdWithoutThrowing) {
  IdMap ids;
  ASSERT_FALSE(ids.Restore(UINT64_MAX, "https://example.org/invalid"));
  ASSERT_EQ(ids.Size(), size_t{0});
  ASSERT_FALSE(ids.Contains("https://example.org/invalid"));
}


TEST(IdMapTest, SparseRestoreUsesOnlyAssignedEntries) {
  IdMap ids;
  constexpr uint64_t sparse = uint64_t{1} << 40;
  ASSERT_TRUE(ids.Restore(sparse, "https://example.org/sparse"));
  ASSERT_TRUE(ids.Restore(2, "https://example.org/low"));
  ASSERT_EQ(ids.Size(), size_t{2});
  ASSERT_EQ(ids.Entries().size(), size_t{2});
  ASSERT_EQ(ids.Lookup(sparse), "https://example.org/sparse");
  ASSERT_TRUE(ids.Lookup(sparse - 1).empty());
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("https://example.org/new")), sparse + 1);
  ASSERT_EQ(ids.Size(), size_t{3});
}

TEST(IdMapTest, ExhaustionPreservesExistingIdsAndNeverWraps) {
  IdMap ids;
  ASSERT_TRUE(ids.Restore(IdMap::kMaxId - 1, "https://example.org/penultimate"));
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("https://example.org/last")), IdMap::kMaxId);
  const auto failed = ids.GetOrCreate("https://example.org/overflow");
  ASSERT_FALSE(failed.ok());
  ASSERT_TRUE(failed.status().IsIllegalState()) << failed.status();
  ASSERT_EQ(ids.Size(), size_t{2});
  ASSERT_FALSE(ids.Contains("https://example.org/overflow"));
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("https://example.org/last")), IdMap::kMaxId);
}

TEST(IdMapTest, InvalidRestoreDoesNotAdvanceAllocation) {
  IdMap ids;
  ASSERT_TRUE(ids.Restore(7, "", TripleTermType::LITERAL));
  ASSERT_FALSE(ids.Restore(IdMap::kMaxId + 1, "https://example.org/out-of-range"));
  ASSERT_FALSE(ids.Restore(7, "https://example.org/conflict"));
  ASSERT_FALSE(ids.Restore(0, "https://example.org/zero"));
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate("https://example.org/next")), uint64_t{8});
  ASSERT_EQ(ids.Size(), size_t{2});
  ASSERT_EQ(ids.Get("", TripleTermType::LITERAL), uint64_t{7});
}

class FailingBindingIter final : public BindingIter {
 public:
  void _begin(Binding&) override { set_status(STATUS(IllegalState, "synthetic iterator failure")); }
  bool _next() override { return false; }
  void _reset() override { set_status(STATUS(IllegalState, "synthetic iterator failure")); }
  void assign_nulls() override {}
  void print(std::ostream&, int, bool) const override {}
};


class LateFailingBindingIter final : public BindingIter {
 public:
  void _begin(Binding& binding) override { binding_ = &binding; first_ = true; }
  bool _next() override {
    if (first_) {
      first_ = false;
      binding_->add(VarId(0), ::ObjectId(42));
      return true;
    }
    set_status(STATUS(IOError, "synthetic late read failure"));
    return false;
  }
  void _reset() override { first_ = true; }
  void assign_nulls() override {}
  void print(std::ostream&, int, bool) const override {}

 private:
  Binding* binding_ = nullptr;
  bool first_ = true;
};

TEST(BindingIterTest, OrderByRejectsPartialRowsOnLateFailure) {
  OrderBy order(std::make_unique<LateFailingBindingIter>(), {VarId(0)},
                {VarId(0)}, {true}, OrderBy::internal_compare);
  Binding binding(1);
  order.begin(binding);
  ASSERT_TRUE(order.status().IsIOError()) << order.status();
  ASSERT_FALSE(order.next());
}

TEST(BindingIterTest, SubSelectPreservesLateChildFailure) {
  SubSelect select(std::make_unique<LateFailingBindingIter>(), {VarId(0)}, {});
  Binding binding(1);
  select.begin(binding);
  ASSERT_TRUE(select.next());
  ASSERT_FALSE(select.next());
  ASSERT_TRUE(select.status().IsIOError()) << select.status();
}


TEST(BindingIterTest, OrderByMissingIriDictionaryIsAnErrorEvenForOneRow) {
  auto values = std::make_unique<Values>(
      std::vector<std::pair<VarId, bool>>{{VarId(0), false}},
      std::vector<::ObjectId>{::ObjectId(0x3000FFFFFFFFFFFEULL)});
  OrderBy order(std::move(values), {VarId(0)}, {VarId(0)}, {true}, OrderBy::rdf_compare);
  Binding binding(1);
  order.begin(binding);
  ASSERT_TRUE(order.status().IsCorruption()) << order.status();
  ASSERT_FALSE(order.next());
}

TEST(BindingIterTest, OrderByResetRebuildsResults) {
  auto values = std::make_unique<Values>(
      std::vector<std::pair<VarId, bool>>{{VarId(0), false}},
      std::vector<::ObjectId>{::ObjectId(42), ::ObjectId(1)});
  OrderBy order(std::move(values), {VarId(0)}, {VarId(0)}, {true}, OrderBy::internal_compare);
  Binding binding(1);
  order.begin(binding);
  for (int pass = 0; pass < 2; ++pass) {
    ASSERT_OK(order.status());
    ASSERT_TRUE(order.next());
    ASSERT_EQ(binding[VarId(0)].id, uint64_t{1});
    ASSERT_TRUE(order.next());
    ASSERT_EQ(binding[VarId(0)].id, uint64_t{42});
    ASSERT_FALSE(order.next());
    ASSERT_OK(order.status());
    if (pass == 0) order.reset();
  }
}


TEST(BindingIterTest, ValuesRespectAssignedBindingsAndReset) {
  Values values(std::vector<std::pair<VarId, bool>>{{VarId(0), true}},
                std::vector<::ObjectId>{::ObjectId(1), ::ObjectId(2), ::ObjectId::get_null()},
                {VarId(0)});
  Binding binding(1);
  binding.add(VarId(0), ::ObjectId(2));
  values.begin(binding);
  ASSERT_TRUE(values.next());
  ASSERT_EQ(binding[VarId(0)].id, uint64_t{2});
  ASSERT_TRUE(values.next());
  ASSERT_EQ(binding[VarId(0)].id, uint64_t{2});
  ASSERT_FALSE(values.next());
  binding.add(VarId(0), ::ObjectId(1));
  values.reset();
  ASSERT_TRUE(values.next());
  ASSERT_EQ(binding[VarId(0)].id, uint64_t{1});
  ASSERT_TRUE(values.next());
  ASSERT_EQ(binding[VarId(0)].id, uint64_t{1});
  ASSERT_FALSE(values.next());
}

TEST(BindingIterTest, IndexNestedLoopJoinPropagatesChildFailure) {
  IndexNestedLoopJoin join(
      std::make_unique<FailingBindingIter>(),
      std::make_unique<EmptyBindingIter>());
  Binding binding(1);

  join.begin(binding);

  ASSERT_FALSE(join.status().ok());
  ASSERT_FALSE(join.next());
  ASSERT_FALSE(join.status().ok());
}

TEST(BindingIterTest, SlicePropagatesDeferredChildFailures) {
  class DeferredFailure final : public BindingIter {
   public:
    explicit DeferredFailure(bool fail_on_reset) : fail_on_reset_(fail_on_reset) {}
    void _begin(Binding&) override {}
    bool _next() override {
      if (fail_on_reset_) return true;
      set_status(STATUS(IllegalState, "synthetic next failure"));
      return false;
    }
    void _reset() override {
      set_status(STATUS(IllegalState, "synthetic reset failure"));
    }
    void assign_nulls() override {}
    void print(std::ostream&, int, bool) const override {}
   private:
    bool fail_on_reset_;
  };

  Binding binding(1);
  for (uint64_t offset : {0, 1}) {
    ::Slice slice(std::make_unique<DeferredFailure>(false), offset, 10);
    slice.begin(binding);
    ASSERT_TRUE(slice.status().ok());
    ASSERT_FALSE(slice.next());
    ASSERT_TRUE(slice.status().IsIllegalState());
    ASSERT_NE(slice.status().ToString().find("synthetic next failure"), std::string::npos);
    // A later reset failure must not replace the original next failure.
    const auto first_error = slice.status().ToString();
    slice.reset();
    ASSERT_EQ(slice.status().ToString(), first_error);
    ASSERT_FALSE(slice.next());
  }

  ::Slice slice(std::make_unique<DeferredFailure>(true), 0, 10);
  slice.begin(binding);
  ASSERT_TRUE(slice.next());
  slice.reset();
  ASSERT_TRUE(slice.status().IsIllegalState());
  ASSERT_NE(slice.status().ToString().find("synthetic reset failure"), std::string::npos);
  ASSERT_FALSE(slice.next());
}

TEST(RdfYBModelTest, LoadFromDocDBFailureLeavesCacheUnloaded) {
  auto& model = RdfYBModel::Instance();
  model.ClearAll();

  const auto status = model.LoadFromDocDB(0);

  ASSERT_FALSE(status.ok());
  ASSERT_FALSE(model.IsLoaded(0));
  ASSERT_EQ(model.GetCache(0).Size(), size_t{0});
}


TEST(TermIdentityTest, LanguageAndDatatypePackedIdsPreserveMetadata) {
  using SPARQL::Conversions;
  ASSERT_NE(Conversions::pack_string_lang("en", "same").id,
            Conversions::pack_string_lang("fr", "same").id);
  ASSERT_NE(Conversions::pack_string_datatype("urn:example:type:a", "same").id,
            Conversions::pack_string_datatype("urn:example:type:b", "same").id);
}


TEST(TermIdentityTest, CanonicalKeysRoundTripWithoutDelimiterCollisions) {
  std::vector<RdfTerm> terms{
      {TripleTermType::URI, "urn:example:x", "", ""},
      {TripleTermType::LITERAL, "urn:example:x", "", ""},
      {TripleTermType::BLANK, "urn:example:x", "", ""},
      {TripleTermType::LITERAL, "value|en", "", ""},
      {TripleTermType::LITERAL, "value", "en", ""},
      {TripleTermType::LITERAL, "value", "", "urn:example:en"},
      {TripleTermType::LITERAL, "", "en", ""},
      {TripleTermType::LITERAL, std::string("a\0b", 3), "", ""}};
  std::set<std::string> unique;
  IdMap ids;
  IdMap restored;
  for (const auto& term : terms) {
    const auto key = ASSERT_RESULT(term.Key());
    ASSERT_TRUE(unique.insert(key).second);
    const auto roundtrip = ASSERT_RESULT(RdfTerm::FromKey(key));
    ASSERT_EQ(ASSERT_RESULT(roundtrip.Key()), key);
    const auto id = ASSERT_RESULT(ids.GetOrCreate(term));
    ASSERT_TRUE(restored.Restore(id, roundtrip));
    ASSERT_EQ(ASSERT_RESULT(restored.GetOrCreate(term)), id);
    ASSERT_EQ(restored.Lookup(id), term.lexical);
  }
  ASSERT_EQ(ids.Size(), terms.size());
}

TEST(TermIdentityTest, LanguageCaseAndXsdStringCanonicalize) {
  IdMap ids;
  const RdfTerm en{TripleTermType::LITERAL, "value", "en", ""};
  auto upper = en;
  upper.language = "EN";
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate(en)), ASSERT_RESULT(ids.GetOrCreate(upper)));
  const RdfTerm plain{TripleTermType::LITERAL, "value", "", ""};
  auto xsd = plain;
  xsd.datatype = "http://www.w3.org/2001/XMLSchema#string";
  ASSERT_EQ(ASSERT_RESULT(ids.GetOrCreate(plain)), ASSERT_RESULT(ids.GetOrCreate(xsd)));
  ASSERT_EQ(SPARQL::Conversions::pack_string_lang("EN", "value").id,
            SPARQL::Conversions::pack_string_lang("en", "value").id);
  ASSERT_EQ(SPARQL::Conversions::try_pack_string_datatype(xsd.datatype, "value").id,
            SPARQL::Conversions::pack_string_simple("value").id);
  const auto packed = SPARQL::Conversions::pack_string_lang("EN", "value");
  ASSERT_EQ(SPARQL::Conversions::unpack_string_lang(packed), std::make_pair(
      std::string("en"), std::string("value")));
  const auto typed = SPARQL::Conversions::pack_string_datatype("urn:example:type", "long value");
  ASSERT_EQ(SPARQL::Conversions::unpack_string_datatype(typed), std::make_pair(
      std::string("urn:example:type"), std::string("long value")));
  ASSERT_NE(SPARQL::Conversions::pack_string_simple(std::string("a\0b", 3)).id,
            SPARQL::Conversions::pack_string_simple("a").id);
}

TEST(TermIdentityTest, InvalidAndNonCanonicalKeysAreRejected) {
  for (const auto& key : {"", "T9", "T29", "T221:truncated", "T22999999999999999999999999999:x",
                          "T220:0:0:trailing", "T2200:0:0:"}) {
    ASSERT_FALSE(RdfTerm::FromKey(key).ok()) << key;
  }
  const RdfTerm invalid{TripleTermType::URI, "urn:example:x", "en", ""};
  ASSERT_FALSE(invalid.Key().ok());
  const RdfTerm both{TripleTermType::LITERAL, "x", "en", "urn:example:type"};
  ASSERT_FALSE(both.Key().ok());
  IdMap ids;
  ASSERT_FALSE(ids.GetOrCreate(both).ok());
  ASSERT_EQ(ids.Size(), size_t{0});
}


TEST(NtParserTest, EscapesAndUnicodePreserveLexicalValue) {
  const auto triple = ASSERT_RESULT(NtParser::ParseLine(
      R"(<https://example.org/s> <urn:example:p> "a.\"quote\" \u0041" .)"));
  ASSERT_EQ(triple.obj, "a.\"quote\" A");
}
TEST(RdfXmlParserTest, LanguageEntitiesAndWhitespaceArePreserved) {
  const auto triples = ASSERT_RESULT(RdfXmlParser::ParseString(R"(
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="https://example.org/#" xml:lang="en">
 <rdf:Description rdf:about="urn:example:s">
  <ex:p>  A &amp; B  </ex:p>
 </rdf:Description>
</rdf:RDF>)"));
  ASSERT_EQ(triples.size(), size_t{1});
  ASSERT_EQ(triples[0].object_language, "en");
  ASSERT_EQ(triples[0].obj, "  A & B  ");
  ASSERT_EQ(triples[0].pred, "https://example.org/#p");
}


TEST(NtParserTest, StrictTermBoundariesAndMalformedEscapes) {
  const auto valid = ASSERT_RESULT(NtParser::ParseLine(
      R"(  <https://example.org/s\u0031> <urn:example:p> "a\\b\t\n\U0001F600"@en-US. # tail)"));
  ASSERT_EQ(valid.sub, "https://example.org/s1");
  ASSERT_EQ(valid.obj, "a\\b\t\n\xF0\x9F\x98\x80");
  ASSERT_EQ(valid.object_language, "en-US");
  const auto blank = ASSERT_RESULT(NtParser::ParseLine("_:a.b <urn:example:p> _:c.d."));
  ASSERT_EQ(blank.sub, "_:a.b");
  ASSERT_EQ(blank.obj, "_:c.d");
  for (const auto& line : {
      R"(<urn:example:s> <urn:example:p> "bad\q" .)",
      R"(<urn:example:s> <urn:example:p> "bad\uD800" .)",
      R"(<urn:example:s> <urn:example:p> "bad\U00110000" .)",
      R"(<urn:example:s> <urn:example:p> "unterminated .)",
      R"(<urn:example:s> <urn:example:p> "x"@ .)",
      R"(<urn:example:s> <urn:example:p> "x"^^_:a .)",
      R"(<urn:example:s> <urn:example:p> "x" extra .)",
      R"(<urn:example:s> _:p "x" .)",
      R"(<relative> <urn:example:p> "x" .)",
      R"(<urn:example:s> <urn:example:p> "x" . garbage)"}) {
    ASSERT_FALSE(NtParser::ParseLine(line).ok()) << line;
  }
}

TEST(NtParserTest, SequencePreservesPeriodsBracesAndComments) {
  const auto triples = ASSERT_RESULT(NtParser::ParseSequence(R"(
    # Not a statement .
    <https://example.org/a> <urn:example:p> "a. } # quote\"" .
    <https://example.org/b> <urn:example:p> <https://example.org/c>.
  )"));
  ASSERT_EQ(triples.size(), size_t{2});
  ASSERT_EQ(triples[0].obj, "a. } # quote\"");
  ASSERT_EQ(triples[1].obj, "https://example.org/c");
  ASSERT_FALSE(NtParser::ParseSequence(
      "<urn:example:s> <urn:example:p> \"ok\" . garbage .").ok());
}

TEST(RdfXmlParserTest, NamespaceAliasesLanguageOverrideAndEmptyLiteral) {
  const auto triples = ASSERT_RESULT(RdfXmlParser::ParseString(R"(
<r:RDF xmlns:r="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
       xmlns="https://example.org/#" xml:lang="en" xml:base="https://example.org/base/">
 <r:Description r:about="s" xml:lang="fr">
  <p xml:lang="de">same</p><p xml:lang="">same</p><p/>
  <p r:datatype="urn:example:type">same</p>
  <p r:resource="o"/><p r:nodeID="b"/>
 </r:Description>
 <r:Description r:nodeID="b"><p><![CDATA[<xml> & text]]></p></r:Description>
</r:RDF>)"));
  ASSERT_EQ(triples.size(), size_t{7});
  ASSERT_EQ(triples[0].sub, "https://example.org/base/s");
  ASSERT_EQ(triples[0].pred, "https://example.org/#p");
  ASSERT_EQ(triples[0].object_language, "de");
  ASSERT_TRUE(triples[1].object_language.empty());
  ASSERT_TRUE(triples[2].obj.empty());
  ASSERT_EQ(triples[2].object_language, "fr");
  ASSERT_EQ(triples[3].object_type, "urn:example:type");
  ASSERT_TRUE(triples[3].object_language.empty());
  ASSERT_EQ(triples[4].obj, "https://example.org/base/o");
  ASSERT_EQ(triples[5].obj_type, TripleTermType::BLANK);
  ASSERT_EQ(triples[6].sub_type, TripleTermType::BLANK);
  ASSERT_EQ(triples[6].obj, "<xml> & text");
}

TEST(RdfXmlParserTest, RejectMalformedOrUnsupportedDocumentWithoutPartialResult) {
  for (const auto& body : {
      "<rdf:Description rdf:about='urn:example:s'><ex:p>x</rdf:Description>",
      "<rdf:Description rdf:about='urn:example:s'>"
          "<ex:p rdf:parseType='Resource'/></rdf:Description>",
      "<rdf:Description rdf:about='urn:example:s'><ex:p><ex:nested/></ex:p></rdf:Description>",
      "<rdf:Description rdf:about='urn:example:s' ex:p='value'/>",
      "<ex:TypedNode rdf:about='urn:example:s'/>",
      "<rdf:Description rdf:about='urn:example:s'><ex:p rdf:resource='urn:example:o'>x</ex:p>"
          "</rdf:Description>"}) {
    const std::string xml =
        "<rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#' "
        "xmlns:ex='https://example.org/#'>"
        "<rdf:Description rdf:about='urn:example:valid'><ex:p>valid</ex:p></rdf:Description>" +
        std::string(body) + "</rdf:RDF>";
    ASSERT_FALSE(RdfXmlParser::ParseString(xml).ok()) << body;
  }
  ASSERT_FALSE(RdfXmlParser::ParseString(
      "<!DOCTYPE rdf:RDF [<!ENTITY x 'value'>]>"
      "<rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'/>").ok());
}


TEST(NumericConversionsTest, SignedInt64RoundTrip) {
  const std::array<int64_t, 10> values{
      -1, 0, 1, -(int64_t{1} << 55), (int64_t{1} << 55) - 1,
      -(int64_t{1} << 55) - 1, int64_t{1} << 55, (int64_t{1} << 48) + 1,
      std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()};
  for (auto value : values) {
    SCOPED_TRACE(value);
    const auto packed = SPARQL::Conversions::pack_int(value);
    ASSERT_EQ(RDF_OID::get_generic_subtype(packed), RDF_OID::GenericSubType::INTEGER);
    ASSERT_EQ(SPARQL::Conversions::unpack_int(packed), value);
    ASSERT_EQ(SPARQL::Conversions::to_lexical_str(packed), std::to_string(value));
    ASSERT_EQ(SPARQL::Conversions::pack_int(value), packed);
  }
}

TEST(NumericConversionsTest, FloatBitsRoundTrip) {
  for (uint32_t bits : {0U, 0x80000000U, 0x3fc00000U, 0xbf800000U,
                        1U, 0x7f7fffffU, 0x7f800000U, 0xff800000U, 0x7fc00001U}) {
    const auto packed = SPARQL::Conversions::pack_float(std::bit_cast<float>(bits));
    ASSERT_EQ(packed.get_type(), ::ObjectId::MASK_FLOAT);
    ASSERT_EQ(packed.get_value() >> 32, 0);
    ASSERT_EQ(std::bit_cast<uint32_t>(SPARQL::Conversions::unpack_float(packed)), bits);
  }
}

TEST(NumericConversionsTest, DoubleBitsRoundTrip) {
  for (uint64_t bits : {0ULL, 0x8000000000000000ULL, 0x3ff8000000000000ULL,
       0xbff0000000000000ULL, 1ULL, 0x7fefffffffffffffULL,
       0x7ff0000000000000ULL, 0xfff0000000000000ULL, 0x7ff8000000000001ULL}) {
    const auto packed = SPARQL::Conversions::pack_double(std::bit_cast<double>(bits));
    ASSERT_EQ(RDF_OID::get_generic_subtype(packed), RDF_OID::GenericSubType::DOUBLE);
    ASSERT_EQ(std::bit_cast<uint64_t>(SPARQL::Conversions::unpack_double(packed)), bits);
    ASSERT_EQ(SPARQL::Conversions::pack_double(std::bit_cast<double>(bits)), packed);
  }
}

TEST(NumericConversionsTest, DecimalPayloadRoundTrip) {
  for (const auto* text : {"-12.75", "0.0", "123456789012345678.1", "0.000000000000000001"}) {
    bool error = false;
    const Decimal original(text, &error);
    ASSERT_FALSE(error);
    ASSERT_EQ(original.to_string(), text);
    const auto packed = SPARQL::Conversions::pack_decimal(original);
    ASSERT_EQ(SPARQL::Conversions::unpack_decimal(packed).to_string(), text);
    ASSERT_EQ(SPARQL::Conversions::pack_decimal(original), packed);
  }
}

TEST(NumericConversionsTest, NumericPromotionAndComparison) {
  using C = SPARQL::Conversions;
  const auto f = C::pack_float(16777216.0f);
  const auto d = C::pack_double(16777217.0);
  ASSERT_EQ(C::calculate_optype(f, d), C::OpType::DOUBLE);
  bool error = true;
  ASSERT_LT(SPARQL::Comparisons::strict_compare(C::pack_int(-1), C::pack_int(0), &error), 0);
  ASSERT_FALSE(error);
  ASSERT_LT(SPARQL::Comparisons::compare(f, d), 0);
  ASSERT_LT(OrderBy::rdf_compare(C::pack_int(9007199254740992LL),
                                C::pack_int(9007199254740993LL)), 0);
  ASSERT_GT(OrderBy::rdf_compare(C::pack_int(10), C::pack_double(2.0)), 0);
}

TEST(NumericConversionsTest, NumericEffectiveBooleanValue) {
  using C = SPARQL::Conversions;
  ASSERT_TRUE(C::to_boolean(C::pack_int(-1)).is_true());
  ASSERT_TRUE(C::to_boolean(C::pack_float(0.5f)).is_true());
  ASSERT_TRUE(C::to_boolean(C::pack_double(-0.5)).is_true());
  ASSERT_TRUE(C::to_boolean(C::pack_int(0)).is_false());
  ASSERT_TRUE(C::to_boolean(C::pack_float(-0.0f)).is_false());
  ASSERT_TRUE(C::to_boolean(C::pack_double(std::numeric_limits<double>::quiet_NaN())).is_false());
}

TEST(NumericConversionsTest, DecimalComparisonKeepsLowDigits) {
  bool error = false;
  const Decimal left("1", &error);
  ASSERT_FALSE(error);
  const Decimal right("1.000000000000000001", &error);
  ASSERT_FALSE(error);
  ASSERT_LT(left, right);
  const Decimal minimum(std::numeric_limits<int64_t>::min());
  ASSERT_EQ(minimum.to_string(), "-9223372036854775808.0");
}

TEST(NumericConversionsTest, NumericIntegerCastsRejectOverflow) {
  using C = SPARQL::Conversions;
  ASSERT_THROW(C::to_integer(C::pack_double(std::numeric_limits<double>::infinity())),
               std::out_of_range);
  ASSERT_THROW(C::to_integer(C::pack_double(9223372036854775808.0)), std::out_of_range);
  ASSERT_EQ(C::to_integer(C::pack_double(-12.75)), -12);
}


TEST(NumericConversionsTest, NaNRelationsAndOrderAreConsistent) {
  using C = SPARQL::Conversions;
  const auto nan = C::pack_double(std::numeric_limits<double>::quiet_NaN());
  const auto zero = C::pack_int(0);
  const auto term = [](auto value) { return std::make_unique<BindingExprTerm>(value); };
  Binding binding(0);
  SPARQL::BindingExprLess less(term(nan), term(zero));
  SPARQL::BindingExprLessOrEqual le(term(nan), term(zero));
  SPARQL::BindingExprGreater greater(term(nan), term(zero));
  SPARQL::BindingExprGreaterOrEqual ge(term(nan), term(zero));
  ASSERT_TRUE(less.eval(binding).is_false());
  ASSERT_TRUE(le.eval(binding).is_false());
  ASSERT_TRUE(greater.eval(binding).is_false());
  ASSERT_TRUE(ge.eval(binding).is_false());
  ASSERT_LT(OrderBy::rdf_compare(nan, zero), 0);
  const std::vector<::ObjectId> values{
      C::pack_int(16777216), C::pack_float(16777216.0f), C::pack_int(16777217),
      C::pack_double(16777217.0), nan, C::pack_double(std::numeric_limits<double>::infinity())};
  for (auto a : values) for (auto b : values) for (auto c : values) {
    const auto ab = OrderBy::rdf_compare(a, b);
    const auto bc = OrderBy::rdf_compare(b, c);
    if (ab < 0 && bc < 0) ASSERT_LT(OrderBy::rdf_compare(a, c), 0);
    if (ab == 0 && bc == 0) ASSERT_EQ(OrderBy::rdf_compare(a, c), 0);
  }
}

TEST(NumericConversionsTest, MissingPayloadAndDecimalCastBoundaries) {
  using C = SPARQL::Conversions;
  ASSERT_THROW(C::unpack_double(::ObjectId(
      ::ObjectId::MASK_DOUBLE_TMP | ::ObjectId::MASK_EXTERNAL_ID)), std::runtime_error);
  ASSERT_EQ(C::to_decimal(C::pack_double(0.0)).to_string(), "0.0");
  ASSERT_EQ(C::to_decimal(C::pack_float(-0.0f)).to_string(), "0.0");
  ASSERT_THROW(C::to_decimal(C::pack_double(std::numeric_limits<double>::infinity())),
               std::out_of_range);
  bool error = false;
  const Decimal max("9223372036854775807", &error);
  ASSERT_FALSE(error);
  ASSERT_EQ(max.to_int(&error), std::numeric_limits<int64_t>::max());
  ASSERT_FALSE(error);
  const Decimal overflow("9223372036854775808", &error);
  ASSERT_FALSE(error);
  overflow.to_int(&error);
  ASSERT_TRUE(error);
}


TEST(NumericConversionsTest, TypedNumericLexicalValuesAndIdentity) {
  using C = SPARQL::Conversions;
  const std::string xsd = "http://www.w3.org/2001/XMLSchema#";
  const auto a = C::pack_string_datatype(xsd + "integer", "01");
  const auto b = C::pack_string_datatype(xsd + "integer", "1");
  ASSERT_NE(a, b);
  ASSERT_EQ(C::to_integer(a), 1);
  ASSERT_EQ(C::term_identity(C::pack_int(1)), b);
  ASSERT_EQ(C::to_lexical_str(a), "01");
  for (const auto& text : {"-9223372036854775808", "9223372036854775807", " \t+0001\r\n"}) {
    const auto value = C::numeric_operand(C::pack_string_datatype(xsd + "integer", text));
    ASSERT_EQ(RDF_OID::get_generic_type(value), RDF_OID::GenericType::NUMERIC) << text;
  }
  for (const auto& text : {".5", "+0.0", "-12.75", "1.000000000000000001"}) {
    const auto value = C::numeric_operand(C::pack_string_datatype(xsd + "decimal", text));
    ASSERT_EQ(RDF_OID::get_generic_type(value), RDF_OID::GenericType::NUMERIC) << text;
  }
  for (const auto& type : {"float", "double"}) {
    for (const auto& text : {"NaN", "INF", "-INF", "+1.25E+2", "-0", "1E-40"}) {
      const auto value = C::numeric_operand(C::pack_string_datatype(xsd + type, text));
      ASSERT_EQ(RDF_OID::get_generic_type(value), RDF_OID::GenericType::NUMERIC) << text;
    }
  }
}

TEST(NumericConversionsTest, TypedNumericRejectsInvalidOrUnsupportedValues) {
  using C = SPARQL::Conversions;
  const std::string xsd = "http://www.w3.org/2001/XMLSchema#";
  const std::vector<std::pair<std::string, std::string>> invalid{
      {"integer", ""}, {"integer", "1.0"}, {"integer", "1e0"}, {"integer", "1 2"},
      {"integer", "9223372036854775808"}, {"integer", "-9223372036854775809"},
      {"byte", "128"}, {"short", "-32769"}, {"int", "2147483648"},
      {"positiveInteger", "0"}, {"negativeInteger", "0"}, {"unsignedInt", "-1"},
      {"unsignedByte", "256"}, {"unsignedShort", "65536"},
      {"unsignedInt", "4294967296"}, {"decimal", "1.2.3"}, {"decimal", "."},
      {"decimal", "1e0"}, {"decimal", "1.1234567890123456789"},
      {"double", "nan"}, {"double", "infinity"}, {"double", "0x1p0"},
      {"double", "1E"}, {"float", "1e9999"}, {"double", "--1"}};
  Binding binding(0);
  for (const auto& [type, text] : invalid) {
    const auto original = C::pack_string_datatype(xsd + type, text);
    ASSERT_TRUE(C::numeric_operand(original).is_null()) << type << ": " << text;
    ASSERT_EQ(C::to_lexical_str(original), text);
    SPARQL::BindingExprIsNumeric numeric(std::make_unique<BindingExprTerm>(original));
    ASSERT_TRUE(numeric.eval(binding).is_false()) << text;
  }
  const auto plain = C::pack_string_simple("2");
  ASSERT_EQ(C::numeric_operand(plain), plain);
  ASSERT_EQ(C::calculate_optype(plain, C::pack_int(1)), C::OpType::INVALID);
  ASSERT_EQ(C::calculate_optype(C::pack_iri("urn:example:2"), C::pack_int(1)),
            C::OpType::INVALID);
}

TEST(NumericConversionsTest, TypedNumericComparisonAndNaNErrors) {
  using C = SPARQL::Conversions;
  const std::string xsd = "http://www.w3.org/2001/XMLSchema#";
  const auto term = [](auto value) { return std::make_unique<BindingExprTerm>(value); };
  Binding binding(0);
  const auto a = C::pack_string_datatype(xsd + "integer", "01");
  const auto b = C::pack_string_datatype(xsd + "integer", "1");
  SPARQL::BindingExprEquals equals(term(a), term(b));
  SPARQL::BindingExprSameTerm same(term(a), term(b));
  ASSERT_TRUE(equals.eval(binding).is_true());
  ASSERT_TRUE(same.eval(binding).is_false());
  const auto nan = C::pack_string_datatype(xsd + "double", "NaN");
  SPARQL::BindingExprEquals nan_equals(term(nan), term(nan));
  SPARQL::BindingExprLess less(term(nan), term(b));
  ASSERT_TRUE(nan_equals.eval(binding).is_false());
  ASSERT_TRUE(less.eval(binding).is_false());
  const auto invalid = C::pack_string_datatype(xsd + "integer", "oops");
  SPARQL::BindingExprEquals bad_equals(term(invalid), term(invalid));
  SPARQL::BindingExprLess bad_less(term(invalid), term(b));
  ASSERT_TRUE(bad_equals.eval(binding).is_null());
  ASSERT_TRUE(bad_less.eval(binding).is_null());
  bool error = false;
  SPARQL::Comparisons::strict_compare(b, C::pack_string_simple("1"), &error);
  ASSERT_TRUE(error);
}

TEST(NumericConversionsTest, TypedNumericIntegerFunctionsAndAggregates) {
  using C = SPARQL::Conversions;
  const auto term = [](auto value) { return std::make_unique<BindingExprTerm>(value); };
  Binding binding(1);
  const auto maximum = C::pack_int(std::numeric_limits<int64_t>::max());
  SPARQL::BindingExprCeil ceil(term(maximum));
  SPARQL::BindingExprFloor floor(term(maximum));
  SPARQL::BindingExprRound round(term(maximum));
  ASSERT_EQ(C::to_integer(ceil.eval(binding)), std::numeric_limits<int64_t>::max());
  ASSERT_EQ(C::to_integer(floor.eval(binding)), std::numeric_limits<int64_t>::max());
  ASSERT_EQ(C::to_integer(round.eval(binding)), std::numeric_limits<int64_t>::max());
  SPARQL::AggSum sum(VarId(0), std::make_unique<BindingExprVar>(VarId(0)));
  SPARQL::AggAvg avg(VarId(0), std::make_unique<BindingExprVar>(VarId(0)));
  sum.set_binding(binding);
  avg.set_binding(binding);
  sum.begin();
  avg.begin();
  for (const auto& lexical : {"01", "3"}) {
    binding.add(VarId(0), C::pack_string_datatype(
        "http://www.w3.org/2001/XMLSchema#integer", lexical));
    sum.process();
    avg.process();
  }
  ASSERT_EQ(C::to_integer(sum.get()), 4);
  ASSERT_EQ(C::to_lexical_str(avg.get()), "2.0");
  sum.begin();
  avg.begin();
  binding.add(VarId(0), maximum);
  sum.process();
  avg.process();
  binding.add(VarId(0), C::pack_int(1));
  sum.process();
  avg.process();
  ASSERT_TRUE(sum.get().is_null());
  ASSERT_TRUE(avg.get().is_null());
}


TEST(NumericConversionsTest, DecimalExactArithmeticKeepsLowDigits) {
  bool error = false;
  const Decimal large("999999999999999999", &error);
  ASSERT_FALSE(error);
  const Decimal one("1", &error);
  ASSERT_EQ((large + one).to_string(), "1000000000000000000.0");
  const Decimal tiny("0.000000000000000001", &error);
  ASSERT_EQ(((one + tiny) - one).to_string(), "0.000000000000000001");
  const Decimal a("123456789", &error);
  ASSERT_EQ((a * a).to_string(), "15241578750190521.0");
}

TEST(NumericConversionsTest, DecimalUnrepresentableArithmeticRejectsInsteadOfWrapping) {
  bool error = false;
  const Decimal large("9999999999999999999", &error);
  ASSERT_FALSE(error);
  const Decimal one("1", &error);
  const Decimal two("2", &error);
  ASSERT_THROW(large * two, std::out_of_range);
  const Decimal tiny("0.0000000000000000001", &error);
  ASSERT_THROW(one + tiny, std::out_of_range);
  const Decimal maximum("1" + std::string(127, '0'), &error);
  ASSERT_FALSE(error);
  ASSERT_THROW(maximum * Decimal(int64_t(10)), std::out_of_range);
}

TEST(NumericConversionsTest, DecimalDivisionHasNineteenSignificantDigits) {
  const Decimal one(int64_t(1));
  const Decimal seven(int64_t(7));
  ASSERT_EQ((one / seven).to_string(), "0.1428571428571428571");
  ASSERT_EQ((Decimal(int64_t(2)) / Decimal(int64_t(3))).to_string(), "0.6666666666666666667");
  ASSERT_EQ((Decimal(int64_t(1)) / Decimal(int64_t(8))).to_string(), "0.125");
}

TEST(NumericConversionsTest, DecimalMalformedLexicalReturnsError) {
  for (const auto& lexical : {"1.2.3", "..1", "1..", ".", "", "+", "--1",
                             "1.1234567890123456789"}) {
    bool error = false;
    const Decimal value(lexical, &error);
    ASSERT_TRUE(error) << lexical;
    ASSERT_TRUE(value.is_zero()) << lexical;
  }
}


TEST(NumericConversionsTest, DecimalExponentAndFloatingConversionBounds) {
  bool error = false;
  const Decimal maximum("1" + std::string(127, '0'), &error);
  ASSERT_FALSE(error);
  const Decimal minimum("0." + std::string(127, '0') + "1", &error);
  ASSERT_FALSE(error);
  const Decimal ten(int64_t(10));
  ASSERT_THROW(maximum * ten, std::out_of_range);
  ASSERT_THROW(minimum / ten, std::out_of_range);
  ASSERT_TRUE((maximum - maximum).is_zero());
  ASSERT_TRUE((Decimal() / minimum).is_zero());
  ASSERT_THROW(Decimal(1e200), std::out_of_range);
  ASSERT_THROW(Decimal(1e-200), std::out_of_range);
  ASSERT_EQ(Decimal(0.0).to_string(), "0.0");
  ASSERT_EQ(Decimal(1e127).to_string(), "1" + std::string(127, '0') + ".0");
  const Decimal odd("9999999999999999999", &error);
  const Decimal even("9999999999999999997", &error);
  const Decimal two(int64_t(2));
  ASSERT_EQ((odd / two).to_string(), "5000000000000000000.0");
  ASSERT_EQ((even / two).to_string(), "4999999999999999998.0");
}

TEST(NumericConversionsTest, DecimalArithmeticReferenceIdentities) {
  for (int64_t left : {-123456789LL, -7LL, 0LL, 1LL, 123456789LL}) {
    for (int64_t right : {-987654321LL, -3LL, 0LL, 2LL, 987654321LL}) {
      const Decimal a(left), b(right);
      ASSERT_EQ(a + b, Decimal(left + right));
      ASSERT_EQ(a - b, Decimal(left - right));
      ASSERT_EQ(a * b, Decimal(left * right));
    }
  }
  using C = SPARQL::Conversions;
  ASSERT_TRUE(C::to_boolean(::ObjectId::get_null()).is_null());
  ASSERT_TRUE(C::to_boolean(C::pack_iri("urn:example:ebv-error")).is_null());
  ASSERT_TRUE(C::to_boolean(C::pack_string_xsd("")).is_false());
}


TEST(AggregateTest, DistinctCountsTermsAndResets) {
  using C = SPARQL::Conversions;
  Binding binding(1);
  SPARQL::AggCountDistinct count(VarId(0), std::make_unique<BindingExprVar>(VarId(0)));
  count.set_binding(binding);
  count.begin();
  for (const auto value : {1, 1, 2}) {
    binding.add(VarId(0), C::pack_int(value));
    count.process();
  }
  ASSERT_EQ(C::to_lexical_str(count.get()), "2");
  count.begin();
  ASSERT_EQ(C::to_lexical_str(count.get()), "0");
}

TEST(AggregateTest, EmptyGlobalResetsToOneResult) {
  std::map<VarId, std::unique_ptr<Agg>> aggs;
  aggs.emplace(VarId(0), std::make_unique<SPARQL::AggCountAll>(VarId(0), nullptr));
  Aggregation aggregate(std::make_unique<EmptyBindingIter>(), std::move(aggs), {});
  Binding binding(1);
  aggregate.begin(binding);
  ASSERT_TRUE(aggregate.next());
  ASSERT_EQ(SPARQL::Conversions::to_integer(binding[VarId(0)]), 0);
  ASSERT_FALSE(aggregate.next());
  aggregate.reset();
  ASSERT_TRUE(aggregate.next());
  ASSERT_EQ(SPARQL::Conversions::to_integer(binding[VarId(0)]), 0);
  ASSERT_FALSE(aggregate.next());
}

TEST(AggregateTest, LateChildFailureDoesNotPublishPartialAggregate) {
  std::map<VarId, std::unique_ptr<Agg>> aggs;
  aggs.emplace(VarId(0), std::make_unique<SPARQL::AggCountAll>(VarId(0), nullptr));
  Aggregation aggregate(std::make_unique<LateFailingBindingIter>(), std::move(aggs), {});
  Binding binding(1);
  aggregate.begin(binding);
  ASSERT_FALSE(aggregate.next());
  ASSERT_TRUE(aggregate.status().IsIOError()) << aggregate.status();
}

TEST(NumericConversionsTest, TypedBooleanViewsPreserveIdentity) {
  using C = SPARQL::Conversions;
  const std::string type = "http://www.w3.org/2001/XMLSchema#boolean";
  const auto one = C::pack_string_datatype(type, "1");
  const auto yes = C::pack_string_datatype(type, "true");
  ASSERT_TRUE(C::to_boolean(one).is_true());
  ASSERT_TRUE(C::to_boolean(yes).is_true());
  ASSERT_NE(C::term_identity(one), C::term_identity(yes));
  ASSERT_EQ(C::to_lexical_str(one), "1");
  ASSERT_FALSE(C::to_boolean(C::pack_string_datatype(type, "bad")).is_true());
}


class CountingAggregateExpr : public BindingExpr {
 public:
  explicit CountingAggregateExpr(int* calls) : calls_(calls) {}
  ::ObjectId eval(const Binding& binding) override {
    ++*calls_;
    return binding[VarId(0)];
  }
  void accept_visitor(BindingExprVisitor&) override {}
  void print(std::ostream& os, std::vector<BindingIter*>&) const override { os << "counted"; }
 private:
  int* calls_;
};

TEST(AggregateTest, DistinctArgumentIsEvaluatedOnce) {
  using C = SPARQL::Conversions;
  Binding binding(1);
  int calls = 0;
  SPARQL::AggSumDistinct sum(VarId(0), std::make_unique<CountingAggregateExpr>(&calls));
  sum.set_binding(binding);
  sum.begin();
  for (const auto& text : {"01", "1", "1"}) {
    binding.add(VarId(0), C::pack_string_datatype(
        "http://www.w3.org/2001/XMLSchema#integer", text));
    sum.process();
  }
  ASSERT_EQ(calls, 3);
  ASSERT_EQ(C::to_integer(sum.get()), 2);
  sum.begin();
  ASSERT_EQ(C::to_integer(sum.get()), 0);
}

TEST(AggregateTest, GroupConcatProducesPlainString) {
  using C = SPARQL::Conversions;
  Binding binding(1);
  SPARQL::AggGroupConcatDistinct concat(
      VarId(0), std::make_unique<BindingExprVar>(VarId(0)), "|");
  concat.set_binding(binding);
  concat.begin();
  for (const auto& text : {"a", "a", "b"}) {
    binding.add(VarId(0), C::pack_string_lang("en", text));
    concat.process();
  }
  ASSERT_EQ(C::to_lexical_str(concat.get()), "a|b");
  ASSERT_EQ(RDF_OID::get_generic_subtype(concat.get()), RDF_OID::GenericSubType::STRING_SIMPLE);
}

TEST(AggregateTest, ErrorsSurviveProjectionAndHavingWrappers) {
  const auto child = [] {
    std::map<VarId, std::unique_ptr<Agg>> aggs;
    aggs.emplace(VarId(0), std::make_unique<SPARQL::AggCountAll>(VarId(0), nullptr));
    return std::make_unique<Aggregation>(
        std::make_unique<LateFailingBindingIter>(), std::move(aggs), std::set<VarId>());
  };
  Binding binding(2);
  std::vector<std::pair<VarId, std::unique_ptr<BindingExpr>>> expressions;
  expressions.emplace_back(VarId(1), std::make_unique<BindingExprVar>(VarId(0)));
  ExprEvaluator projection(child(), std::move(expressions));
  projection.begin(binding);
  ASSERT_FALSE(projection.next());
  ASSERT_TRUE(projection.status().IsIOError()) << projection.status();
  std::vector<std::unique_ptr<BindingExpr>> filters;
  filters.push_back(std::make_unique<BindingExprTerm>(SPARQL::Conversions::pack_bool(true)));
  Filter having(SPARQL::Conversions::to_boolean, child(), std::move(filters));
  having.begin(binding);
  ASSERT_FALSE(having.next());
  ASSERT_TRUE(having.status().IsIOError()) << having.status();
}

TEST(NumericConversionsTest, TypedBooleanLexicalAndValueRules) {
  using C = SPARQL::Conversions;
  const std::string type = "http://www.w3.org/2001/XMLSchema#boolean";
  for (const auto& text : {"true", "1", " \ttrue\r\n"}) {
    const auto value = C::pack_string_datatype(type, text);
    ASSERT_TRUE(C::to_boolean(value).is_true());
    ASSERT_TRUE(SPARQL::BindingExprEquals::eval_values(value, C::pack_bool(true)).is_true());
  }
  for (const auto& text : {"false", "0", " false "}) {
    ASSERT_EQ(C::boolean_operand(C::pack_string_datatype(type, text)), C::pack_bool(false));
  }
  for (const auto& text : {"", "TRUE", "yes", "1 0", "2"}) {
    const auto value = C::pack_string_datatype(type, text);
    ASSERT_TRUE(C::boolean_operand(value).is_null());
    ASSERT_EQ(C::to_boolean(value), C::pack_bool(false));
  }
  ASSERT_EQ(C::term_identity(C::pack_bool(true)), C::pack_string_datatype(type, "true"));
  bool error = false;
  ASSERT_LT(SPARQL::Comparisons::strict_compare(C::pack_bool(false),
      C::pack_string_datatype(type, "1"), &error), 0);
  ASSERT_FALSE(error);
  SPARQL::Comparisons::strict_compare(C::pack_bool(true), C::pack_iri("urn:example:x"), &error);
  ASSERT_TRUE(error);
}


TEST(AggregateTest, GroupResetAndAssignNullsClearProducedVariables) {
  using C = SPARQL::Conversions;
  std::vector<std::pair<VarId, bool>> vars{{VarId(0), false}};
  std::vector<::ObjectId> rows{C::pack_int(1), C::pack_int(1), C::pack_int(2)};
  std::map<VarId, std::unique_ptr<Agg>> aggs;
  aggs.emplace(VarId(1), std::make_unique<SPARQL::AggCountAll>(VarId(1), nullptr));
  Aggregation aggregate(std::make_unique<Values>(std::move(vars), std::move(rows)),
      std::move(aggs), {VarId(0)});
  Binding binding(2);
  aggregate.begin(binding);
  ASSERT_TRUE(aggregate.next());
  ASSERT_EQ(C::to_integer(binding[VarId(1)]), 2);
  ASSERT_TRUE(aggregate.next());
  ASSERT_EQ(C::to_integer(binding[VarId(1)]), 1);
  ASSERT_FALSE(aggregate.next());
  aggregate.reset();
  ASSERT_TRUE(aggregate.next());
  ASSERT_EQ(C::to_integer(binding[VarId(0)]), 1);
  ASSERT_EQ(C::to_integer(binding[VarId(1)]), 2);
  aggregate.assign_nulls();
  ASSERT_TRUE(binding[VarId(0)].is_null());
  ASSERT_TRUE(binding[VarId(1)].is_null());
}

TEST(AggregateTest, ZeroColumnValuesCloneAndResetKeepMultiplicity) {
  SPARQL::OpValues op({}, {}, 2);
  auto clone = op.clone();
  const auto* values_op = dynamic_cast<const SPARQL::OpValues*>(clone.get());
  ASSERT_NE(values_op, nullptr);
  ASSERT_EQ(values_op->num_rows, 2);
  Values values({}, {}, {}, values_op->num_rows);
  Binding binding(0);
  values.begin(binding);
  ASSERT_TRUE(values.next());
  ASSERT_TRUE(values.next());
  ASSERT_FALSE(values.next());
  values.reset();
  ASSERT_TRUE(values.next());
  ASSERT_TRUE(values.next());
  ASSERT_FALSE(values.next());
  std::ostringstream printed;
  values.print(printed, 0, false);
  ASSERT_NE(printed.str().find("()"), std::string::npos);
}

}  // namespace yb::mdb
