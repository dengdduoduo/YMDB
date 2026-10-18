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

TEST(ParserTest, ParseSimpleBGP) {
    SPARQL::QueryParser qp(
        "SELECT ?s WHERE { ?s <http://example.org/p> <http://example.org/o> }");
    ASSERT_NE(qp.root, nullptr);
    ASSERT_EQ(qp.parser.getNumberOfSyntaxErrors(), 0);
}

TEST(ParserTest, ParseJoin) {
    SPARQL::QueryParser qp(
        "SELECT ?s WHERE { ?s <http://x/p1> ?x . ?x <http://x/p2> ?y }");
    ASSERT_NE(qp.root, nullptr);
    ASSERT_EQ(qp.parser.getNumberOfSyntaxErrors(), 0);
}

TEST(ParserTest, ParseWithFilter) {
    SPARQL::QueryParser qp(
        "SELECT ?s WHERE { ?s <http://x/p> ?o . FILTER(?o > 10) }");
    ASSERT_NE(qp.root, nullptr);
    ASSERT_EQ(qp.parser.getNumberOfSyntaxErrors(), 0);
}

TEST(ParserTest, ParseUnion) {
    SPARQL::QueryParser qp(
        "SELECT ?s WHERE { "
        "  { ?s <http://x/p1> <http://x/o1> } UNION "
        "  { ?s <http://x/p2> <http://x/o2> } "
        "}");
    ASSERT_NE(qp.root, nullptr);
    ASSERT_EQ(qp.parser.getNumberOfSyntaxErrors(), 0);
}

TEST(ParserTest, ParseOrderByLimit) {
    SPARQL::QueryParser qp(
        "SELECT ?s WHERE { ?s <http://x/p> ?o } ORDER BY ?o LIMIT 5");
    ASSERT_NE(qp.root, nullptr);
    ASSERT_EQ(qp.parser.getNumberOfSyntaxErrors(), 0);
}

TEST(ParserTest, ParseInvalidDoesNotCrash) {
    SPARQL::QueryParser qp("SELECT * FROM");
    (void)qp.root;
    SUCCEED();
}

TEST(ParserTest, ParseWithPrefix) {
    SPARQL::QueryParser qp(
        "PREFIX ex: <http://example.org/> "
        "SELECT ?s WHERE { ?s ex:p ex:o }");
    ASSERT_NE(qp.root, nullptr);
    ASSERT_EQ(qp.parser.getNumberOfSyntaxErrors(), 0);
}

}  // namespace yb::mdb
