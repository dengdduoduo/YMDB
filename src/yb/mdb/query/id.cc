#include "query/id.h"

#include <ostream>

#include "query/query_context.h"

std::ostream& operator<<(std::ostream& os, ObjectId oid) {
    return os << oid.id;
}

std::ostream& operator<<(std::ostream& os, VarId var) {
    return os << '?' << get_query_ctx().get_var_name(var.id);
}

std::ostream& operator<<(std::ostream& os, Id id) {
    if (id.is_OID()) {
        return os << id.get_OID();
    } else {
        return os << '?' << get_query_ctx().get_var_name(id.get_var().id);
    }
}
