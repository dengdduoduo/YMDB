#include "executor_constructor.h"

#include "query/exceptions.h"

using namespace SPARQL;

ExecutorConstructor::ExecutorConstructor() { }

void ExecutorConstructor::visit(OpSelect&) {
    throw NotSupportedException("ExecutorConstructor::visit(OpSelect) not yet implemented");
}

void ExecutorConstructor::visit(OpAsk&) {
    throw NotSupportedException("ExecutorConstructor::visit(OpAsk) not yet implemented");
}

void ExecutorConstructor::visit(OpDescribe&) {
    throw NotSupportedException("ExecutorConstructor::visit(OpDescribe) not yet implemented");
}

void ExecutorConstructor::visit(OpConstruct&) {
    throw NotSupportedException("ExecutorConstructor::visit(OpConstruct) not yet implemented");
}

void ExecutorConstructor::visit(OpShow&) {
    throw NotSupportedException("ExecutorConstructor::visit(OpShow) not yet implemented");
}
