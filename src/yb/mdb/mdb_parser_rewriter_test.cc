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

#include "query/parser/sparql_query_parser.h"

namespace yb::mdb {

TEST(ParserRewriterTest, BasicBGP) {
    SPARQL::QueryParser parser("SELECT ?s WHERE { ?s ?p ?o }");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
}

TEST(ParserRewriterTest, FixedTriple) {
    SPARQL::QueryParser parser(
        "SELECT ?s WHERE { ?s <http://a/p> <http://a/o> }");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
}

TEST(ParserRewriterTest, DoubleTripleJoin) {
    SPARQL::QueryParser parser(
        "SELECT ?s ?o WHERE { ?s <http://a/p1> ?o . ?s <http://a/p2> ?o2 }");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
}

TEST(ParserRewriterTest, FilterPushdown) {
    SPARQL::QueryParser parser(
        "SELECT ?s WHERE { ?s <http://a/p> ?o . FILTER(?o > 3000) }");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
}

TEST(ParserRewriterTest, OptionalPattern) {
    SPARQL::QueryParser parser(
        "SELECT ?s WHERE { ?s <http://a/p1> ?o . OPTIONAL { ?s <http://a/p2> ?o2 } }");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
}

TEST(ParserRewriterTest, UnionPattern) {
    SPARQL::QueryParser parser(
        "SELECT ?s WHERE { "
        "  { ?s <http://a/p1> <http://a/o1> } UNION "
        "  { ?s <http://a/p2> <http://a/o2> } "
        "}");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
}

TEST(ParserRewriterTest, SyntaxError) {
    EXPECT_THROW(
        {
            SPARQL::QueryParser parser("SELECT ?s WHERE { ?s ?p");
            parser.get_query_plan({});
        },
        QueryParsingException);
}

TEST(ParserRewriterTest, WithPrefix) {
    SPARQL::QueryParser parser(
        "PREFIX ex: <http://example.org/> "
        "SELECT ?s WHERE { ?s ex:p ex:o }");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
}

}  // namespace yb::mdb
