#pragma once

#include <memory>

#include "query/parser/op/sparql/op_visitor.h"
#include "query/parser/op/sparql/ops.h"

namespace SPARQL {

class ExecutorConstructor : public OpVisitor {
public:
    ExecutorConstructor();

    void visit(OpSelect&) override;
    void visit(OpAsk&) override;
    void visit(OpDescribe&) override;
    void visit(OpConstruct&) override;
    void visit(OpShow&) override;

    void visit(OpBasicGraphPattern&) override { }
    void visit(OpBind&) override { }
    void visit(OpEmpty&) override { }
    void visit(OpFrom&) override { }
    void visit(OpFilter&) override { }
    void visit(OpGraph&) override { }
    void visit(OpGroupBy&) override { }
    void visit(OpHaving&) override { }
    void visit(OpJoin&) override { }
    void visit(OpMinus&) override { }
    void visit(OpNotExists&) override { }
    void visit(OpOptional&) override { }
    void visit(OpOrderBy&) override { }
    void visit(OpPath&) override { }
    void visit(OpProcedure&) override { }
    void visit(OpSequence&) override { }
    void visit(OpSemiJoin&) override { }
    void visit(OpService&) override { }
    void visit(OpTriple&) override { }
    void visit(OpUnion&) override { }
    void visit(OpUnitTable&) override { }
    void visit(OpValues&) override { }

    void visit(OpCreateHNSWIndex&) override { }
    void visit(OpCreateTextIndex&) override { }
    void visit(OpDeleteData&) override { }
    void visit(OpInsertData&) override { }
    void visit(OpUpdate&) override { }
};

} // namespace SPARQL
