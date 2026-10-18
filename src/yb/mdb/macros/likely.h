#pragma once
#define MDB_likely(x)   __builtin_expect(!!(x), 1)
#define MDB_unlikely(x) __builtin_expect(!!(x), 0)
