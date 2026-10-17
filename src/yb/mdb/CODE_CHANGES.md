# MDB 源码修改说明

初始记录日期：2026 年 10 月 12 日。补充记录日期：2026 年 10 月 16 日。

## 最新修复与验证状态（2026-10-16）

Gitee `ymdb` 同步说明：以原分支提交 `791069a2ac6d1d541ef1c98255bb06309ec15d94` 为基础移植 MDB 内容与构建入口，保留该分支的 YugabyteDB 基线和既有文件权限。文中 28 项通过结果来自虚拟机的 `codex/mdb-integration` 基线；本次同步进行了内容与接口检查，未在 `ymdb` 的较新基线上重新完整编译或运行测试。

此前虚拟机正式构建成功，19 项单元测试通过，8 项集成测试中 4 项字典测试通过、4 项导入查询测试失败。临时测试副本补刷新并修正表主键后，4 项导入查询测试通过。以下修复已应用到 Windows 与虚拟机正式源码；虚拟机恢复连接后增量编译成功，19 项单元测试与 9 项集成测试全部通过（28/28）。修改前文件、构建日志、测试日志及 XML 结果保存在 `/home/wfgd/mdb-agent-session/import-fix-20261016/`。

| 文件 | 本轮修改 | 用途 |
|---|---|---|
| `import/import_pipeline.cc` | 每批写入后调用 `Flush()`，传播错误；刷新成功后累计 `triples_written` | 成功返回前完成本批缓冲写入，避免把仅入队的数据算作已写入 |
| `mdb_import_integration_test.cc` | 测试表 `c2` 增加 `PrimaryKey()` | 与正式三元组写入的复合主键一致 |
| `mdb_import_integration_test.cc` | 新增 `ImportFilesAreVisibleWhenImportReturns` 及文件流、utility 头文件 | 直接覆盖三种文件导入接口和六张索引的可见性 |
| `IMPROVEMENTS.md` | 更新第一项验证结果及第九项修复进度 | 区分历史测试结果与本轮待验证状态 |

批量写入的核心差异：

```diff
     RETURN_NOT_OK(model.WriteTripleToDocDB(subj_id, pred_id, obj_id));
-    stats_.triples_written++;
   }
+  // Complete buffered writes before reporting this batch as written.
+  RETURN_NOT_OK(model.Flush());
+  stats_.triples_written += static_cast<int64_t>(triples.size());
   return Status::OK();
```

测试建表差异：

```diff
-    builder.AddColumn("c2")->Type(DataType::INT64)->NotNull();
+    builder.AddColumn("c2")->Type(DataType::INT64)->PrimaryKey()->NotNull();
```

新增回归测试依次生成 NT、带表头 CSV、RDF/XML 文件，每个文件含两个不同主体的三元组，使用同一个 `ImportPipeline` 调用公开 `ImportFile()` 接口。每次断言解析数、写入数均为 2，立即从数据库加载六张索引并检查累计行数为 2、4、6；测试读取前不调用 `Flush()`，避免掩盖导入接口漏刷新的问题。文件位于测试临时目录。

本轮源码空白字符检查通过，已清理修改记录中历史差异快照的行尾空白。正式增量编译成功；19 项单元测试、9 项集成测试（新增 1 项）全部通过。原来失败的四项导入查询分别读回 6、8、6、2 行。既有测试仅在单 TabletServer 的 MiniCluster 中运行，字典重建是对象重建，并未覆盖真实进程重启、多节点或故障恢复。`Flush()` 也不提供整批六张索引的事务原子性；本轮未修改这一语义。

新增测试完整内容：

```cpp
TEST_F(MdbImportIntegrationTest, ImportFilesAreVisibleWhenImportReturns) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());

  const std::pair<const char*, const char*> files[] = {
      {"visible.nt",
       "<http://example.org/nt1> <http://example.org/p> <http://example.org/o> .\n"
       "<http://example.org/nt2> <http://example.org/p> <http://example.org/o> .\n"},
      {"visible.csv",
       "subject,predicate,object\n"
       "http://example.org/csv1,http://example.org/p,http://example.org/o\n"
       "http://example.org/csv2,http://example.org/p,http://example.org/o\n"},
      {"visible.rdf", R"(<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/xml1">
    <ex:p rdf:resource="http://example.org/o"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/xml2">
    <ex:p rdf:resource="http://example.org/o"/>
  </rdf:Description>
</rdf:RDF>)"}};

  ImportPipeline pipeline;
  size_t expected_rows = 0;
  for (const auto& [name, data] : files) {
    SCOPED_TRACE(name);
    auto path = GetTestPath(name);
    {
      std::ofstream file(path);
      ASSERT_TRUE(file.is_open());
      file << data;
      file.close();
      ASSERT_FALSE(file.fail());
    }
    auto stats = ASSERT_RESULT(pipeline.ImportFile(path));
    ASSERT_EQ(stats.triples_parsed, 2);
    ASSERT_EQ(stats.triples_written, 2);
    expected_rows += 2;

    // Do not flush here: successful import must make its own writes visible.
    ASSERT_NO_FATALS(LoadFromDocDB());
    for (int perm = 0; perm < 6; ++perm) {
      ASSERT_EQ(RdfYBModel::Instance().GetCache(perm).Size(), expected_rows);
    }
  }
}
```

下文为 2026-10-12 的实现说明和代码快照；涉及本轮改动的旧片段以此处补充及当前源文件为准。

本文记录本次对 `mdb` 模块所做的全部代码修改，包含文件清单、实现说明、新增测试、验证状态，以及完整代码差异和新增文件内容。当前只实施了第一项“词项字典持久化与唯一 ID 分配”；其余改进项的状态见 `IMPROVEMENTS.md`。

## 一、修改目标

原来的 `IdMap` 按本地数组长度从 1 开始分配 ID，恢复数据库映射时没有恢复分配器。进程重新启动或多个客户端分别导入时，可能为不同词项分配相同 ID。

本次增加数据库中的持久化词项字典，通过条件写入协调 ID 分配，并恢复旧字典中的原有 ID。

## 二、文件清单

以下路径均相对于 `src/yb/mdb/`。

| 文件 | 类型 | 修改内容 |
|---|---|---|
| `CMakeLists.txt` | 修改 | 将持久化字典实现加入 MDB 导入源码列表 |
| `import/persistent_id_map.h` | 新增 | 定义持久化字典类及接口 |
| `import/persistent_id_map.cc` | 新增 | 实现建表、读取、条件写入、计数器分配及旧字典迁移 |
| `import/import.h` | 修改 | 为本地字典增加恢复、最大 ID 和遍历接口，支持不连续 ID |
| `import/import.cc` | 修改 | 实现映射恢复、冲突检测及 ID 溢出检查 |
| `model/rdf_yb_model.h` | 修改 | 增加持久化字典成员和统一词项解析接口 |
| `model/rdf_yb_model.cc` | 修改 | 初始化和重置字典，统一分配 ID，传播打开表失败 |
| `import/import_pipeline.cc` | 修改 | 批量导入改用持久化 ID 分配 |
| `import/insert_executor.cc` | 修改 | INSERT DATA 改用统一 ID 分配，增加事务退出清理 |
| `adapter/mdb_adapter.cc` | 修改 | 恢复本地字典、接入分配器、保留重复导入映射、传播字典错误 |
| `mdb_import_test.cc` | 修改 | 新增 3 个本地字典测试 |
| `mdb_import_integration_test.cc` | 修改 | 新增 4 个持久化与并发分配集成测试 |
| `IMPROVEMENTS.md` | 新增并翻译为中文 | 记录全部改进项及当前验证状态 |
| `CODE_CHANGES.md` | 新增 | 本修改说明及代码快照 |

## 三、具体代码修改

### 1. 持久化字典：PersistentIdMap

新增 `rdf_terms` 表，包含以下字段：

| 字段 | 类型 | 用途 |
|---|---|---|
| `term_key` | STRING，哈希主键 | 保存计数器键或词项键 |
| `term_id` | INT64，非空 | 保存已分配的最大 ID 或词项 ID |

计数器键为 `counter`，词项键为 `term:` 加词项字符串。新表初始创建 1 个 Tablet。

| 接口或函数 | 实现内容 |
|---|---|
| `Init()` | 检查客户端，创建或打开表，迁移旧字典，初始化计数器，成功后标记已初始化 |
| `Read()` | 读取指定键的 ID，检查响应、行数、空值和负值 |
| `CompareExchange()` | 通过值相等条件更新，或通过记录不存在条件插入；解析 `[applied]` 判断是否成功 |
| `EnsureMinimumId()` | 保证计数器不小于指定值，不降低已有计数器 |
| `MigrateLegacy()` | 读取 `rdf_idmap` 的 `gmap_id` 与 `uri`，保留原 ID 并建立新字典记录 |
| `GetOrCreate()` | 已存在词项直接复用 ID；新词项先预留 ID，再尝试认领词项记录 |

分配流程：

1. 查询词项是否已经存在，存在则返回原 ID。
2. 读取共享计数器，以条件更新方式将其加 1。
3. 通过“记录不存在时才插入”尝试保存词项与新 ID。
4. 如果另一客户端已经保存该词项，则读取并返回对方保存的 ID。

竞争失败或操作中断允许留下未使用的 ID，因此 ID 不保证连续。计数器竞争最多尝试 128 次，超过后返回 `TryAgain`；计数器达到 `INT64_MAX` 时返回错误。数据库读取和写入使用 30 秒 session 超时。

### 2. 本地字典：IdMap

- `id_to_uri_` 从数组改为以 ID 为键的哈希表，支持不连续 ID。
- 新增 `max_id_`，本地 `GetOrCreate()` 从最大已有 ID 后继续分配。
- 新增 `Restore(id, uri)`，恢复指定 ID；拒绝 ID 为 0、同词项对应不同 ID，以及同 ID 对应不同词项。
- 相同映射可重复恢复；冲突返回 `false`，不修改映射。
- 新增 `MaxId()` 与 `Entries()`，用于迁移和遍历实际存在的记录。
- `Lookup()` 改为按 ID 查找，不存在时返回空字符串。
- 本地分配达到 `UINT64_MAX` 时抛出溢出异常。

本地 `GetOrCreate()` 保留供本地字典使用；本次接入的数据库写入路径使用持久化分配器。

### 3. 存储模型：RdfYBModel

- 新增 `term_dictionary_` 成员。
- `Init()` 在打开六张索引表后初始化持久化字典。
- 打开索引表失败时返回真实错误，不再标记初始化成功。
- 新增 `ResolveTerm(local_map, term)`：检查连接状态，优先复用本地映射，否则取得持久化 ID 并恢复到本地字典。
- 本地与持久化映射冲突时返回 `Corruption`。
- `ClearAll()` 同时重置持久化字典对象。

### 4. 导入管线：ImportPipeline

`BatchWrite()` 中主语、谓语及宾语的 ID 分配，均由本地 `GetOrCreate()` 改为 `RdfYBModel::ResolveTerm()`，分配失败通过 `VERIFY_RESULT` 返回。

原有带类型宾语的字符串拼接方式保持不变，RDF 类型编码问题尚未在本项解决。

### 5. INSERT DATA：InsertDataExecutor

- 主语、谓语和宾语改用统一持久化 ID 分配。
- 删除空白节点宾语与其他宾语执行相同逻辑的重复分支。
- 增加 `ScopeExit`：开启事务后，离开函数时中止仍存在的事务，覆盖 ID 分配失败等提前返回路径。
- 新增作用域清理所需头文件。

原有句点切分解析和每 50 条提交方式尚未修改；本次没有解决整条更新语句的原子性问题。

### 6. 适配层：mdb_adapter.cc

| 函数 | 修改内容 |
|---|---|
| `CreateIdMapTable()` | 只容忍表已存在，其他建表失败返回错误 |
| `SaveIdMapToDocDB()` | 从本地字典实际记录取得词项内容，支持保存空字符串；缺少词项或 Flush 失败时返回错误 |
| `LoadIdMapFromDocDB()` | 增加扫描错误收集；检查缺失词项、非法 ID 和映射冲突；恢复 `g_id_map` |
| `ImportNtFileWithIdMap()` | 三个位置均使用 `ResolveTerm()`；增加事务作用域清理 |
| `YBCMdbInitCluster()` | 检查字典建表和恢复结果，失败时记录错误并返回失败 |
| `YBCMdbImportFile()` | 不再清空已有 `g_pack_to_gmap`；按实际字典记录遍历；检查字典保存结果 |

### 7. 构建配置

在 `MDB_IMPORT_SRCS` 中加入 `import/persistent_id_map.cc`，使新增实现参与 `yb_mdb` 编译。

## 四、新增测试

### 本地字典测试

| 测试名称 | 验证内容 |
|---|---|
| `RestoreOutOfOrderAndContinueAllocation` | 乱序恢复 1000、7 等不连续 ID，新词项从 1001 继续分配 |
| `RejectConflictingRestoredMappings` | 拒绝冲突映射和 ID 0，相同映射可重复恢复 |
| `RestoreEmptyTermAndDetectExhaustion` | 恢复空字符串词项，检测 ID 空间耗尽 |

### 集成测试

| 测试名称 | 验证内容 |
|---|---|
| `PersistentTermIdsSurviveNewDictionaryInstance` | 新建字典对象后复用已有 ID，新 ID 继续增长，空字符串可复用 |
| `ConcurrentDictionaryClientsClaimUniqueIds` | 8 个字典客户端同时认领同一词项时返回同一 ID，不同词项获得不同 ID |
| `ModelUsesPersistedIdsWithFreshLocalMap` | 模型清理并重新初始化后，空本地字典取得原有持久化 ID |
| `MigrateLegacyDictionaryWithoutRenumbering` | 迁移旧字典中的 ID 1000，后续分配大于 1000，重复初始化保留旧 ID |

这些测试已写入源码，但尚未运行。重新连接和模型重新初始化测试不等同于真正的独立进程重启测试。

## 五、验证状态

| 验证项 | 状态 |
|---|---|
| 代码差异空白字符检查 | 通过 |
| 新增持久化字典文件的 clang-format 检查 | 通过 |
| 独立字典源码编译尝试 | 未成功：当前 C++ 工具链找不到标准库头文件 `cstdint` |
| 完整 MDB 编译 | 未执行：缺少生成的 protobuf 头文件和支持的 YugabyteDB 构建环境 |
| 本地字典测试、集成测试 | 未运行 |

在支持的 Linux 构建环境中执行：

```bash
./yb_build.sh release --cxx-test mdb_import_test --sj --skip-pg-parquet --no-odyssey --no-ybc
./yb_build.sh release --cxx-test mdb_import_integration_test --sj --skip-pg-parquet --no-odyssey --no-ybc
```

## 六、兼容性和本项边界

- 旧 `rdf_idmap` 表保留，原有图 ID 不重新编号。
- 迁移遇到冲突或缺失元数据时返回错误，不能仅凭数字图 ID 重建词项内容。
- 新分配器启用后，旧版本程序不得继续向同一 keyspace 写入；旧版本仍会自行分配本地 ID。
- 持久化字典分配独立于图写入事务，分配后图写入失败可能留下未被三元组引用的词项或 ID 间隙。
- 本项的并发协调针对独立字典客户端；全局查询上下文、适配层映射和共享事务的进程内并发隔离仍待修改。
- 类型编码、IRI 输出、缓存一致性、完整查询快照及分布式执行尚未完成。
- 本文是当前修改快照；后续代码改动需要同步更新本文和 `IMPROVEMENTS.md`。

## 七、完整代码差异及新增文件

下方记录相对于修改前源码快照的完整差异，并附新增源码和进度文档全文，便于逐项核对。

### 7.1 已有源码的完整修改差异

```diff
diff --git a/src/yb/mdb/CMakeLists.txt b/src/yb/mdb/CMakeLists.txt
index a1bb126..1a1dfc0 100644
--- a/src/yb/mdb/CMakeLists.txt
+++ b/src/yb/mdb/CMakeLists.txt
@@ -38,6 +38,7 @@ set(MDB_STORAGE_SRCS

 set(MDB_IMPORT_SRCS
     import/import.cc
+    import/persistent_id_map.cc
     import/nt_parser.cc
     import/insert_executor.cc
     import/import_pipeline.cc
diff --git a/src/yb/mdb/adapter/mdb_adapter.cc b/src/yb/mdb/adapter/mdb_adapter.cc
index 1fc719e..59e0dd5 100644
--- a/src/yb/mdb/adapter/mdb_adapter.cc
+++ b/src/yb/mdb/adapter/mdb_adapter.cc
@@ -62,6 +62,7 @@
 #include "yb/server/hybrid_clock.h"
 #include "yb/util/logging.h"
 #include "yb/util/status.h"
+#include "yb/util/scope_exit.h"

 using yb::MonoDelta;
 using yb::Status;
@@ -124,10 +125,8 @@ Status CreateIdMapTable() {

   yb::client::TableHandle tmp;
   auto status = tmp.Create(table_name, 1, g_yb_client.get(), &builder);
-  if (!status.ok()) {
-    LOG(INFO) << "IdMap table: " << status;
-  }
-  return Status::OK();
+  if (status.IsAlreadyPresent()) return Status::OK();
+  return status;
 }

 Status SaveIdMapToDocDB() {
@@ -144,28 +143,15 @@ Status SaveIdMapToDocDB() {
     auto* req = insert->mutable_request();
     QLAddInt64HashValue(req, static_cast<int64_t>(pack_id));
     table.AddInt64ColumnValue(req, "gmap_id", static_cast<int64_t>(gmap_id));
-    auto uri_it = g_gmap_to_uri.find(gmap_id);
-    if (uri_it != g_gmap_to_uri.end()) {
-      table.AddStringColumnValue(req, "uri", uri_it->second);
-    } else {
-      ObjectId pack_oid(static_cast<uint64_t>(pack_id));
-      auto oid_type = pack_oid.get_type();
-      if (oid_type == ObjectId::MASK_STRING_SIMPLE_EXTERN ||
-          oid_type == ObjectId::MASK_STRING_XSD_EXTERN ||
-          oid_type == ObjectId::MASK_STRING_LANG_EXTERN) {
-        std::string str_val = Conversions::to_lexical_str(pack_oid);
-        if (!str_val.empty()) {
-          table.AddStringColumnValue(req, "uri", str_val);
-        }
-      }
+    auto term = g_id_map->Entries().find(gmap_id);
+    if (term == g_id_map->Entries().end()) {
+      return STATUS(Corruption, "Missing RDF term for persisted ID");
     }
+    table.AddStringColumnValue(req, "uri", term->second);
     session->Apply(insert);
   }

-  auto flush_status = session->TEST_Flush();
-  if (!flush_status.ok()) {
-    LOG(WARNING) << "Flush idmap failed: " << flush_status;
-  }
+  RETURN_NOT_OK(session->TEST_Flush());

   LOG(INFO) << "Saved " << g_pack_to_gmap.size() << " idmap entries";
   return Status::OK();
@@ -185,10 +171,18 @@ Status LoadIdMapFromDocDB() {

   yb::client::TableIteratorOptions options;
   options.columns = {"pack_id", "gmap_id", "uri"};
+  Status scan_status;
+  options.error_handler = [&scan_status](const Status& s) { scan_status = s; };
   for (const auto& row : yb::client::TableRange(table, options)) {
     int64_t pack_id = row.column(0).int64_value();
     int64_t gmap_id = row.column(1).int64_value();
+    if (row.column(2).IsNull()) {
+      return STATUS(Corruption, "Missing persisted RDF term");
+    }
     auto uri = row.column(2).string_value();
+    if (gmap_id <= 0 || !g_id_map->Restore(static_cast<uint64_t>(gmap_id), uri)) {
+      return STATUS(Corruption, "Conflicting persisted RDF term mapping");
+    }
     g_pack_to_gmap[static_cast<uint64_t>(pack_id)] = static_cast<uint64_t>(gmap_id);
     ObjectId pack_oid(static_cast<uint64_t>(pack_id));
     g_gmap_to_pack[static_cast<uint64_t>(gmap_id)] = pack_oid;
@@ -205,6 +199,7 @@ Status LoadIdMapFromDocDB() {
     }
   }

+  RETURN_NOT_OK(scan_status);
   LOG(INFO) << "Loaded " << g_pack_to_gmap.size() << " idmap entries";
   return Status::OK();
 }
@@ -479,6 +474,9 @@ Status ImportNtFileWithIdMap(const std::string& file_path,
   if (use_txn) {
     RETURN_NOT_OK(model.BeginTransaction());
   }
+  auto abort_on_exit = yb::ScopeExit([&model, use_txn] {
+    if (use_txn) (void)model.AbortTransaction();
+  });

   std::string line;
   int line_num = 0;
@@ -498,14 +496,14 @@ Status ImportNtFileWithIdMap(const std::string& file_path,

     auto& triple = *result;

-    uint64_t subj_id = id_map.GetOrCreate(triple.sub);
-    uint64_t pred_id = id_map.GetOrCreate(triple.pred);
+    uint64_t subj_id = VERIFY_RESULT(model.ResolveTerm(id_map, triple.sub));
+    uint64_t pred_id = VERIFY_RESULT(model.ResolveTerm(id_map, triple.pred));

     std::string obj_key = triple.obj;
     if (triple.obj_type == yb::mdb::TripleTermType::LITERAL && !triple.object_type.empty()) {
       obj_key = triple.obj + "|" + triple.object_type;
     }
-    uint64_t obj_id = id_map.GetOrCreate(obj_key);
+    uint64_t obj_id = VERIFY_RESULT(model.ResolveTerm(id_map, obj_key));

     ObjectId subj_pack = Conversions::pack_iri(triple.sub);
     ObjectId pred_pack = Conversions::pack_iri(triple.pred);
@@ -638,7 +636,11 @@ int YBCMdbInitCluster(const char* master_addrs, const char* keyspace) {
     }
   }

-  (void)CreateIdMapTable();
+  auto create_idmap_status = CreateIdMapTable();
+  if (!create_idmap_status.ok()) {
+    LOG(WARNING) << "Failed to create RDF dictionary: " << create_idmap_status;
+    return 1;
+  }

   auto init_status = yb::mdb::RdfYBModel::Instance().Init(
       g_yb_client.get(), g_keyspace);
@@ -649,7 +651,11 @@ int YBCMdbInitCluster(const char* master_addrs, const char* keyspace) {

   g_id_map = std::make_unique<yb::mdb::IdMap>();

-  (void)LoadIdMapFromDocDB();
+  auto load_idmap_status = LoadIdMapFromDocDB();
+  if (!load_idmap_status.ok()) {
+    LOG(WARNING) << "Failed to restore RDF dictionary: " << load_idmap_status;
+    return 1;
+  }

   g_cluster_initialized = true;
   LOG(INFO) << "Cluster initialized successfully, keyspace=" << g_keyspace;
@@ -670,8 +676,6 @@ int YBCMdbImportFile(const char* file_path) {
     g_id_map = std::make_unique<yb::mdb::IdMap>();
   }

-  g_pack_to_gmap.clear();
-
   int64_t parsed = 0, written = 0, errors = 0;
   auto status = ImportNtFileWithIdMap(std::string(file_path), *g_id_map,
                                       parsed, written, errors);
@@ -685,14 +689,17 @@ int YBCMdbImportFile(const char* file_path) {
             << " errors=" << errors;

   g_gmap_to_uri.clear();
-  for (uint64_t i = 1; i <= g_id_map->Size(); i++) {
-    auto& uri = g_id_map->Lookup(i);
+  for (const auto& [i, uri] : g_id_map->Entries()) {
     if (!uri.empty()) {
       g_gmap_to_uri[i] = uri;
     }
   }

-  (void)SaveIdMapToDocDB();
+  auto save_idmap_status = SaveIdMapToDocDB();
+  if (!save_idmap_status.ok()) {
+    LOG(WARNING) << "Failed to save RDF dictionary: " << save_idmap_status;
+    return 1;
+  }

   g_gmap_to_pack.clear();
   for (auto& [pack_id, gmap_id] : g_pack_to_gmap) {
diff --git a/src/yb/mdb/import/import.cc b/src/yb/mdb/import/import.cc
index 5443ab8..bfaeab4 100644
--- a/src/yb/mdb/import/import.cc
+++ b/src/yb/mdb/import/import.cc
@@ -1,5 +1,9 @@
 #include "yb/mdb/import/import.h"

+#include <limits>
+#include <stdexcept>
+#include <utility>
+
 namespace yb::mdb {

 uint64_t IdMap::GetOrCreate(std::string_view uri) {
@@ -7,19 +11,24 @@ uint64_t IdMap::GetOrCreate(std::string_view uri) {
   if (it != uri_to_id_.end()) {
     return it->second;
   }
-  uint64_t id = static_cast<uint64_t>(id_to_uri_.size()) + 1;
+  if (max_id_ == std::numeric_limits<uint64_t>::max()) {
+    throw std::overflow_error("RDF term ID space exhausted");
+  }
+  uint64_t id = max_id_ + 1;
   std::string uri_str(uri);
   uri_to_id_[uri_str] = id;
-  id_to_uri_.push_back(std::move(uri_str));
+  id_to_uri_.emplace(id, std::move(uri_str));
+  max_id_ = id;
   return id;
 }

 const std::string& IdMap::Lookup(uint64_t id) const {
-  if (id == 0 || id > id_to_uri_.size()) {
+  auto it = id_to_uri_.find(id);
+  if (it == id_to_uri_.end()) {
     static const std::string k_empty;
     return k_empty;
   }
-  return id_to_uri_[id - 1];
+  return it->second;
 }

 bool IdMap::Contains(std::string_view uri) const {
@@ -38,4 +47,17 @@ size_t IdMap::Size() const {
   return id_to_uri_.size();
 }

+bool IdMap::Restore(uint64_t id, std::string_view uri) {
+  if (id == 0) return false;
+  std::string term(uri);
+  auto by_term = uri_to_id_.find(term);
+  if (by_term != uri_to_id_.end() && by_term->second != id) return false;
+  auto by_id = id_to_uri_.find(id);
+  if (by_id != id_to_uri_.end() && by_id->second != term) return false;
+  uri_to_id_.emplace(term, id);
+  id_to_uri_.emplace(id, std::move(term));
+  if (id > max_id_) max_id_ = id;
+  return true;
+}
+
 }  // namespace yb::mdb
diff --git a/src/yb/mdb/import/import.h b/src/yb/mdb/import/import.h
index aba208e..2584310 100644
--- a/src/yb/mdb/import/import.h
+++ b/src/yb/mdb/import/import.h
@@ -45,10 +45,16 @@ class IdMap {
   bool Contains(std::string_view uri) const;
   uint64_t Get(std::string_view uri) const;
   size_t Size() const;
+  // Restore an explicit persisted ID without renumbering existing terms.
+  // Conflicting mappings and ID zero are rejected without changing the map.
+  bool Restore(uint64_t id, std::string_view uri);
+  uint64_t MaxId() const { return max_id_; }
+  const std::unordered_map<uint64_t, std::string>& Entries() const { return id_to_uri_; }

  private:
   std::unordered_map<std::string, uint64_t> uri_to_id_;
-  std::vector<std::string> id_to_uri_;
+  std::unordered_map<uint64_t, std::string> id_to_uri_;
+  uint64_t max_id_ = 0;
 };

 }  // namespace yb::mdb
diff --git a/src/yb/mdb/import/import_pipeline.cc b/src/yb/mdb/import/import_pipeline.cc
index ca37108..ad35896 100644
--- a/src/yb/mdb/import/import_pipeline.cc
+++ b/src/yb/mdb/import/import_pipeline.cc
@@ -15,14 +15,14 @@ Status ImportPipeline::BatchWrite(const std::vector<Triple>& triples) {
   auto& model = RdfYBModel::Instance();

   for (const auto& triple : triples) {
-    uint64_t subj_id = id_map_.GetOrCreate(triple.sub);
-    uint64_t pred_id = id_map_.GetOrCreate(triple.pred);
+    uint64_t subj_id = VERIFY_RESULT(model.ResolveTerm(id_map_, triple.sub));
+    uint64_t pred_id = VERIFY_RESULT(model.ResolveTerm(id_map_, triple.pred));

     std::string obj_key = triple.obj;
     if (triple.obj_type == TripleTermType::LITERAL && !triple.object_type.empty()) {
       obj_key = triple.obj + "|" + triple.object_type;
     }
-    uint64_t obj_id = id_map_.GetOrCreate(obj_key);
+    uint64_t obj_id = VERIFY_RESULT(model.ResolveTerm(id_map_, obj_key));

     RETURN_NOT_OK(model.WriteTripleToDocDB(subj_id, pred_id, obj_id));
     stats_.triples_written++;
diff --git a/src/yb/mdb/import/insert_executor.cc b/src/yb/mdb/import/insert_executor.cc
index f7ed630..bc4b736 100644
--- a/src/yb/mdb/import/insert_executor.cc
+++ b/src/yb/mdb/import/insert_executor.cc
@@ -5,6 +5,7 @@
 #include "yb/mdb/import/nt_parser.h"
 #include "yb/mdb/model/rdf_yb_model.h"
 #include "yb/util/logging.h"
+#include "yb/util/scope_exit.h"

 namespace yb::mdb {

@@ -55,17 +56,14 @@ Status InsertDataExecutor::Execute(const std::string& insert_data_query,
   if (use_txn) {
     RETURN_NOT_OK(model.BeginTransaction());
   }
+  auto abort_on_exit = ScopeExit([&model, use_txn] {
+    if (use_txn) (void)model.AbortTransaction();
+  });

   for (const auto& triple : triples) {
-    uint64_t subj_id = id_map.GetOrCreate(triple.sub);
-    uint64_t pred_id = id_map.GetOrCreate(triple.pred);
-    uint64_t obj_id;
-
-    if (triple.obj_type == TripleTermType::BLANK) {
-      obj_id = id_map.GetOrCreate(triple.obj);
-    } else {
-      obj_id = id_map.GetOrCreate(triple.obj);
-    }
+    uint64_t subj_id = VERIFY_RESULT(model.ResolveTerm(id_map, triple.sub));
+    uint64_t pred_id = VERIFY_RESULT(model.ResolveTerm(id_map, triple.pred));
+    uint64_t obj_id = VERIFY_RESULT(model.ResolveTerm(id_map, triple.obj));

     auto status = model.WriteTripleToDocDB(subj_id, pred_id, obj_id);
     if (!status.ok()) {
diff --git a/src/yb/mdb/mdb_import_integration_test.cc b/src/yb/mdb/mdb_import_integration_test.cc
index ef8ffbe..79ad535 100644
--- a/src/yb/mdb/mdb_import_integration_test.cc
+++ b/src/yb/mdb/mdb_import_integration_test.cc
@@ -13,6 +13,8 @@

 #include <gtest/gtest.h>

+#include <future>
+#include <memory>
 #include <set>
 #include <sstream>
 #include <string>
@@ -23,14 +25,18 @@
 #include "query/query_context.h"

 #include "yb/client/schema.h"
+#include "yb/client/session.h"
 #include "yb/client/table_handle.h"
+#include "yb/client/yb_op.h"
 #include "yb/client/yb_table_name.h"
 #include "yb/common/common.pb.h"
+#include "yb/common/ql_protocol_util.h"
 #include "yb/integration-tests/mini_cluster.h"
 #include "yb/integration-tests/yb_mini_cluster_test_base.h"
 #include "yb/mdb/import/csv_parser.h"
 #include "yb/mdb/import/import_pipeline.h"
 #include "yb/mdb/import/nt_parser.h"
+#include "yb/mdb/import/persistent_id_map.h"
 #include "yb/mdb/import/rdf_xml_parser.h"
 #include "yb/mdb/model/rdf_yb_model.h"
 #include "yb/util/status.h"
@@ -112,6 +118,101 @@ class MdbImportIntegrationTest : public MiniClusterTestWithClient<MiniCluster> {
   }
 };

+TEST_F(MdbImportIntegrationTest, PersistentTermIdsSurviveNewDictionaryInstance) {
+  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
+  uint64_t first_id;
+  {
+    PersistentIdMap dictionary;
+    ASSERT_OK(dictionary.Init(client_.get(), kNamespace));
+    first_id = ASSERT_RESULT(dictionary.GetOrCreate("http://ex/first"));
+  }
+  PersistentIdMap restarted;
+  ASSERT_OK(restarted.Init(client_.get(), kNamespace));
+  ASSERT_EQ(ASSERT_RESULT(restarted.GetOrCreate("http://ex/first")), first_id);
+  ASSERT_GT(ASSERT_RESULT(restarted.GetOrCreate("http://ex/second")), first_id);
+  auto empty_id = ASSERT_RESULT(restarted.GetOrCreate(""));
+  ASSERT_EQ(ASSERT_RESULT(restarted.GetOrCreate("")), empty_id);
+}
+
+TEST_F(MdbImportIntegrationTest, ConcurrentDictionaryClientsClaimUniqueIds) {
+  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
+  constexpr int kWorkers = 8;
+  std::vector<std::unique_ptr<PersistentIdMap>> dictionaries;
+  for (int i = 0; i < kWorkers; ++i) {
+    auto dictionary = std::make_unique<PersistentIdMap>();
+    ASSERT_OK(dictionary->Init(client_.get(), kNamespace));
+    dictionaries.push_back(std::move(dictionary));
+  }
+  std::promise<void> start;
+  auto ready = start.get_future().share();
+  std::vector<std::future<Result<uint64_t>>> shared_claims;
+  for (auto& dictionary : dictionaries) {
+    auto* ptr = dictionary.get();
+    shared_claims.push_back(std::async(std::launch::async, [ptr, ready] {
+      ready.wait();
+      return ptr->GetOrCreate("http://ex/shared");
+    }));
+  }
+  start.set_value();
+  std::set<uint64_t> shared_ids;
+  for (auto& claim : shared_claims) shared_ids.insert(ASSERT_RESULT(claim.get()));
+  ASSERT_EQ(shared_ids.size(), 1);
+
+  std::vector<std::future<Result<uint64_t>>> unique_claims;
+  for (int i = 0; i < kWorkers; ++i) {
+    auto* ptr = dictionaries[i].get();
+    unique_claims.push_back(std::async(std::launch::async, [ptr, i] {
+      return ptr->GetOrCreate("http://ex/unique/" + std::to_string(i));
+    }));
+  }
+  std::set<uint64_t> unique_ids = shared_ids;
+  for (auto& claim : unique_claims) {
+    ASSERT_TRUE(unique_ids.insert(ASSERT_RESULT(claim.get())).second);
+  }
+  ASSERT_EQ(unique_ids.size(), kWorkers + 1);
+}
+
+TEST_F(MdbImportIntegrationTest, ModelUsesPersistedIdsWithFreshLocalMap) {
+  ASSERT_NO_FATALS(CreateRdfTables());
+  ASSERT_NO_FATALS(InitModel());
+  IdMap first;
+  auto& model = RdfYBModel::Instance();
+  auto first_id = ASSERT_RESULT(model.ResolveTerm(first, "http://ex/first"));
+  auto second_id = ASSERT_RESULT(model.ResolveTerm(first, "http://ex/second"));
+  model.ClearAll();
+  ASSERT_NO_FATALS(InitModel());
+  IdMap restarted;
+  ASSERT_EQ(ASSERT_RESULT(model.ResolveTerm(restarted, "http://ex/second")), second_id);
+  ASSERT_EQ(ASSERT_RESULT(model.ResolveTerm(restarted, "http://ex/first")), first_id);
+  auto new_id = ASSERT_RESULT(model.ResolveTerm(restarted, "http://ex/new"));
+  ASSERT_GT(new_id, second_id);
+  ASSERT_EQ(restarted.Lookup(first_id), "http://ex/first");
+}
+
+TEST_F(MdbImportIntegrationTest, MigrateLegacyDictionaryWithoutRenumbering) {
+  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
+  client::YBSchemaBuilder builder;
+  builder.AddColumn("pack_id")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
+  builder.AddColumn("gmap_id")->Type(DataType::INT64)->NotNull();
+  builder.AddColumn("uri")->Type(DataType::STRING)->NotNull();
+  client::TableHandle legacy;
+  ASSERT_OK(legacy.Create(
+      client::YBTableName(YQL_DATABASE_CQL, kNamespace, "rdf_idmap"),
+      1, client_.get(), &builder));
+  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
+  auto op = legacy.NewInsertOp(session->arena());
+  QLAddInt64HashValue(op->mutable_request(), 123);
+  legacy.AddInt64ColumnValue(op->mutable_request(), "gmap_id", 1000);
+  legacy.AddStringColumnValue(op->mutable_request(), "uri", "http://ex/legacy");
+  ASSERT_OK(session->TEST_ApplyAndFlush(op));
+  PersistentIdMap dictionary;
+  ASSERT_OK(dictionary.Init(client_.get(), kNamespace));
+  ASSERT_EQ(ASSERT_RESULT(dictionary.GetOrCreate("http://ex/legacy")), 1000);
+  ASSERT_GT(ASSERT_RESULT(dictionary.GetOrCreate("http://ex/new")), 1000);
+  ASSERT_OK(dictionary.Init(client_.get(), kNamespace));
+  ASSERT_EQ(ASSERT_RESULT(dictionary.GetOrCreate("http://ex/legacy")), 1000);
+}
+
 TEST_F(MdbImportIntegrationTest, ImportNtFileAndQuery) {
   ASSERT_NO_FATALS(CreateRdfTables());
   ASSERT_NO_FATALS(InitModel());
diff --git a/src/yb/mdb/mdb_import_test.cc b/src/yb/mdb/mdb_import_test.cc
index ffaea96..fe0d3eb 100644
--- a/src/yb/mdb/mdb_import_test.cc
+++ b/src/yb/mdb/mdb_import_test.cc
@@ -12,6 +12,8 @@
 //

 #include <gtest/gtest.h>
+#include <limits>
+#include <stdexcept>

 #include "yb/mdb/import/csv_parser.h"
 #include "yb/mdb/import/import.h"
@@ -182,4 +184,36 @@ TEST(IdMapTest, Contains) {
   ASSERT_TRUE(id_map.Contains("http://ex/foo"));
 }

+TEST(IdMapTest, RestoreOutOfOrderAndContinueAllocation) {
+  IdMap id_map;
+  ASSERT_TRUE(id_map.Restore(1000, "http://ex/last"));
+  ASSERT_TRUE(id_map.Restore(7, "http://ex/first"));
+  ASSERT_EQ(id_map.GetOrCreate("http://ex/first"), 7);
+  ASSERT_EQ(id_map.GetOrCreate("http://ex/new"), 1001);
+  ASSERT_EQ(id_map.Lookup(1000), "http://ex/last");
+  ASSERT_EQ(id_map.Size(), 3);
+  ASSERT_TRUE(id_map.Lookup(8).empty());
+}
+
+TEST(IdMapTest, RejectConflictingRestoredMappings) {
+  IdMap id_map;
+  ASSERT_TRUE(id_map.Restore(7, "http://ex/a"));
+  ASSERT_TRUE(id_map.Restore(7, "http://ex/a"));
+  ASSERT_FALSE(id_map.Restore(7, "http://ex/b"));
+  ASSERT_FALSE(id_map.Restore(8, "http://ex/a"));
+  ASSERT_FALSE(id_map.Restore(0, "http://ex/zero"));
+  ASSERT_EQ(id_map.Size(), 1);
+  ASSERT_EQ(id_map.MaxId(), 7);
+  ASSERT_EQ(id_map.GetOrCreate("http://ex/b"), 8);
+}
+
+TEST(IdMapTest, RestoreEmptyTermAndDetectExhaustion) {
+  IdMap id_map;
+  ASSERT_TRUE(id_map.Restore(5, ""));
+  ASSERT_TRUE(id_map.Contains(""));
+  ASSERT_EQ(id_map.GetOrCreate(""), 5);
+  ASSERT_TRUE(id_map.Restore(std::numeric_limits<uint64_t>::max(), "http://ex/max"));
+  ASSERT_THROW(id_map.GetOrCreate("http://ex/overflow"), std::overflow_error);
+}
+
 }  // namespace yb::mdb
diff --git a/src/yb/mdb/model/rdf_yb_model.cc b/src/yb/mdb/model/rdf_yb_model.cc
index df27b32..31b44e5 100644
--- a/src/yb/mdb/model/rdf_yb_model.cc
+++ b/src/yb/mdb/model/rdf_yb_model.cc
@@ -12,6 +12,7 @@
 //

 #include "yb/mdb/model/rdf_yb_model.h"
+#include "yb/mdb/import/import.h"

 #include <future>

@@ -56,11 +57,11 @@ Status RdfYBModel::Init(yb::client::YBClient* client, const std::string& namespa
     YBTableName table_name(YQL_DATABASE_CQL, namespace_name, kTableNames[i]);
     Status s = tables_[i].Open(table_name, client);
     if (!s.ok()) {
-      initialized_ = true;
-      return Status::OK();
+      return s;
     }
   }

+  RETURN_NOT_OK(term_dictionary_.Init(client, namespace_name));
   write_session_ = client->NewSession(MonoDelta::FromSeconds(15));

   txn_manager_ = std::make_unique<client::TransactionManager>(
@@ -78,6 +79,16 @@ const yb::client::TableHandle* RdfYBModel::GetTable(int perm_index) const {
   return &tables_[perm_index];
 }

+Result<uint64_t> RdfYBModel::ResolveTerm(IdMap& local_map, std::string_view term) {
+  if (!connected_) return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
+  if (local_map.Contains(term)) return local_map.Get(term);
+  auto id = VERIFY_RESULT(term_dictionary_.GetOrCreate(term));
+  if (!local_map.Restore(id, term)) {
+    return STATUS(Corruption, "Persistent RDF dictionary conflicts with local mapping");
+  }
+  return id;
+}
+
 DocDBRangeCache& RdfYBModel::GetCache(int perm_index) {
   return caches_[perm_index];
 }
@@ -276,6 +287,7 @@ void RdfYBModel::ClearAll() {
   write_session_.reset();
   txn_manager_.reset();
   client_ = nullptr;
+  term_dictionary_ = PersistentIdMap();
   initialized_ = false;
   connected_ = false;
 }
diff --git a/src/yb/mdb/model/rdf_yb_model.h b/src/yb/mdb/model/rdf_yb_model.h
index 467941a..e3d44d6 100644
--- a/src/yb/mdb/model/rdf_yb_model.h
+++ b/src/yb/mdb/model/rdf_yb_model.h
@@ -25,6 +25,7 @@

 #include "yb/mdb/storage/docdb_key_encoder.h"
 #include "yb/mdb/storage/docdb_range_cache.h"
+#include "yb/mdb/import/persistent_id_map.h"
 #include "yb/common/read_hybrid_time.h"

 namespace yb::client {
@@ -34,6 +35,8 @@ class YBTransaction;

 namespace yb::mdb {

+class IdMap;
+
 class RdfYBModel {
  public:
   static RdfYBModel& Instance();
@@ -46,6 +49,8 @@ class RdfYBModel {

   bool IsConnected() const { return connected_; }

+  Result<uint64_t> ResolveTerm(IdMap& local_map, std::string_view term);
+
   const yb::client::TableHandle* GetTable(int perm_index) const;

   DocDBRangeCache& GetCache(int perm_index);
@@ -99,6 +104,7 @@ class RdfYBModel {
   yb::client::YBClient* client_ = nullptr;
   yb::client::TableHandle tables_[6];
   yb::client::YBSessionPtr write_session_;
+  PersistentIdMap term_dictionary_;
   std::unique_ptr<client::TransactionManager> txn_manager_;
   client::YBTransactionPtr current_txn_;
   std::shared_ptr<client::YBSession> read_session_;
```

### 7.2 新增文件全文

#### import/persistent_id_map.h

```cpp
#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "yb/client/table_handle.h"
#include "yb/util/result.h"

namespace yb::mdb {

// The counter and term claims use single-row conditional writes. Allocations
// may leave gaps after a crash or a competing claim, but never reuse an ID.
class PersistentIdMap {
 public:
  Status Init(client::YBClient* client, const std::string& namespace_name);
  Result<uint64_t> GetOrCreate(std::string_view term);
  static constexpr const char* kTableName = "rdf_terms";

 private:
  Result<std::optional<int64_t>> Read(const std::string& key);
  Result<bool> CompareExchange(
      const std::string& key, std::optional<int64_t> expected, int64_t value);
  Status EnsureMinimumId(int64_t minimum);
  Status MigrateLegacy(const std::string& namespace_name);

  client::YBClient* client_ = nullptr;
  client::TableHandle table_;
  bool initialized_ = false;
};

}  // namespace yb::mdb
```

#### import/persistent_id_map.cc

```cpp
#include "yb/mdb/import/persistent_id_map.h"

#include <limits>
#include <vector>

#include "yb/client/client.h"
#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/yb_op.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/common/schema.h"
#include "yb/common/schema_pbutil.h"
#include "yb/mdb/import/import.h"
#include "yb/qlexpr/ql_rowblock.h"

namespace yb::mdb {
namespace {
constexpr const char* kCounterKey = "counter";
constexpr int kMaxAttempts = 128;

std::string TermKey(std::string_view term) { return "term:" + std::string(term); }
}  // namespace

Status PersistentIdMap::Init(client::YBClient* client, const std::string& namespace_name) {
  initialized_ = false;
  if (!client) return STATUS(InvalidArgument, "YBClient is null");
  client_ = client;
  client::YBTableName name(YQL_DATABASE_CQL, namespace_name, kTableName);
  client::YBSchemaBuilder builder;
  builder.AddColumn("term_key")->Type(DataType::STRING)->HashPrimaryKey()->NotNull();
  builder.AddColumn("term_id")->Type(DataType::INT64)->NotNull();
  auto status = table_.Create(name, 1, client_, &builder);
  if (!status.ok() && !status.IsAlreadyPresent()) return status;
  RETURN_NOT_OK(table_.Open(name, client_));
  // Upgrade the old dictionary before accepting allocations in this client.
  RETURN_NOT_OK(MigrateLegacy(namespace_name));
  RETURN_NOT_OK(EnsureMinimumId(0));
  initialized_ = true;
  return Status::OK();
}

Result<std::optional<int64_t>> PersistentIdMap::Read(const std::string& key) {
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto op = table_.NewReadOp(session->arena());
  QLAddStringHashValue(op->mutable_request(), key);
  table_.AddColumns({"term_id"}, op->mutable_request());
  RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
  if (op->response().status() != QLResponsePB::YQL_STATUS_OK) {
    return STATUS(
        RuntimeError, "Read RDF dictionary: " + op->response().error_message().ToBuffer());
  }
  auto rows = VERIFY_RESULT(op->MakeRowBlock());
  if (rows.row_count() == 0) return std::optional<int64_t>();
  if (rows.row_count() != 1 || rows.row(0).column(0).IsNull()) {
    return STATUS(Corruption, "Invalid RDF dictionary row");
  }
  auto value = rows.row(0).column(0).int64_value();
  if (value < 0) return STATUS(Corruption, "Negative RDF term ID");
  return std::optional<int64_t>(value);
}

Result<bool> PersistentIdMap::CompareExchange(
    const std::string& key, std::optional<int64_t> expected, int64_t value) {
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto op = table_.NewInsertOp(session->arena());
  auto* req = op->mutable_request();
  QLAddStringHashValue(req, key);
  table_.AddInt64ColumnValue(req, "term_id", value);
  if (expected) {
    table_.SetInt64Condition(
        req->mutable_if_expr()->mutable_condition(), "term_id", QL_OP_EQUAL, *expected);
    req->mutable_column_refs()->add_ids(table_.ColumnId("term_id"));
  } else {
    req->mutable_if_expr()->mutable_condition()->set_op(QL_OP_NOT_EXISTS);
  }
  RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
  if (op->response().status() != QLResponsePB::YQL_STATUS_OK) {
    return STATUS(RuntimeError, "Claim RDF term ID: " + op->response().error_message().ToBuffer());
  }
  std::vector<ColumnSchema> columns;
  for (const auto& column : op->response().column_schemas()) {
    columns.push_back(ColumnSchemaFromPB(column));
  }
  if (columns.empty() || columns.front().name() != "[applied]") {
    return STATUS(Corruption, "Missing conditional-write result for RDF dictionary");
  }
  qlexpr::QLRowBlock rows{Schema(columns)};
  auto data = op->rows_data().AsSlice();
  RETURN_NOT_OK(rows.Deserialize(YQL_CLIENT_CQL, &data));
  if (rows.row_count() != 1 || rows.row(0).column(0).IsNull()) {
    return STATUS(Corruption, "Invalid conditional-write result for RDF dictionary");
  }
  return rows.row(0).column(0).bool_value();
}

Status PersistentIdMap::EnsureMinimumId(int64_t minimum) {
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    auto current = VERIFY_RESULT(Read(kCounterKey));
    if (current && *current >= minimum) return Status::OK();
    if (VERIFY_RESULT(CompareExchange(kCounterKey, current, minimum))) return Status::OK();
  }
  return STATUS(TryAgain, "RDF ID counter is busy");
}

Status PersistentIdMap::MigrateLegacy(const std::string& namespace_name) {
  client::TableHandle legacy;
  auto status =
      legacy.Open(client::YBTableName(YQL_DATABASE_CQL, namespace_name, "rdf_idmap"), client_);
  if (status.IsNotFound()) return Status::OK();
  RETURN_NOT_OK(status);
  IdMap restored;
  Status scan_status;
  client::TableIteratorOptions options;
  options.columns = {"gmap_id", "uri"};
  options.error_handler = [&scan_status](const Status& s) { scan_status = s; };
  for (const auto& row : client::TableRange(legacy, options)) {
    auto id = row.column(0).int64_value();
    if (id <= 0 || row.column(1).IsNull() ||
        !restored.Restore(static_cast<uint64_t>(id), row.column(1).string_value())) {
      return STATUS(Corruption, "Conflicting or incomplete legacy RDF dictionary");
    }
  }
  RETURN_NOT_OK(scan_status);
  RETURN_NOT_OK(EnsureMinimumId(static_cast<int64_t>(restored.MaxId())));
  for (const auto& [id, term] : restored.Entries()) {
    if (!VERIFY_RESULT(CompareExchange(TermKey(term), std::nullopt, id))) {
      auto existing = VERIFY_RESULT(Read(TermKey(term)));
      if (!existing || *existing != static_cast<int64_t>(id)) {
        return STATUS(Corruption, "Legacy RDF term conflicts with persistent dictionary");
      }
    }
  }
  return Status::OK();
}

Result<uint64_t> PersistentIdMap::GetOrCreate(std::string_view term) {
  if (!initialized_) return STATUS(IllegalState, "Persistent RDF dictionary is not initialized");
  const auto key = TermKey(term);
  if (auto existing = VERIFY_RESULT(Read(key))) {
    if (*existing == 0) return STATUS(Corruption, "RDF term ID zero is reserved");
    return static_cast<uint64_t>(*existing);
  }
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    auto current = VERIFY_RESULT(Read(kCounterKey));
    if (!current) return STATUS(Corruption, "Missing RDF ID counter");
    if (*current == std::numeric_limits<int64_t>::max()) {
      return STATUS(IllegalState, "RDF term ID space exhausted");
    }
    const auto next = *current + 1;
    if (!VERIFY_RESULT(CompareExchange(kCounterKey, current, next))) continue;
    if (VERIFY_RESULT(CompareExchange(key, std::nullopt, next))) {
      return static_cast<uint64_t>(next);
    }
    auto winner = VERIFY_RESULT(Read(key));
    if (!winner || *winner == 0) return STATUS(Corruption, "Missing claimed RDF term");
    return static_cast<uint64_t>(*winner);
  }
  return STATUS(TryAgain, "RDF ID counter is busy");
}

}  // namespace yb::mdb
```

#### IMPROVEMENTS.md

````markdown
# MDB 改进进度

按顺序逐项修改。“源码已修改”不代表集群测试已通过，验证状态单独记录。

| 序号 | 改进内容 | 状态 |
|---|---|---|
| 1 | 词项字典持久化与唯一 ID 分配 | 源码已修改，运行验证待完成 |
| 2 | IRI 编码与恢复 | 待修改 |
| 3 | RDF 词项类型与可逆数值编码 | 待修改 |
| 4 | 缓存失效机制与加载状态 | 待修改 |
| 5 | 查询共享读取快照 | 待修改 |
| 6 | 按 Tablet 范围执行分布式查询 | 待修改 |
| 7 | Join 分布属性与执行策略 | 待修改 |
| 8 | 全局排序、分页、聚合与去重 | 待修改 |
| 9 | 统一写入与提交流程 | 待修改 |
| 10 | INSERT DATA 解析与原子性 | 待修改 |
| 11 | OPTIONAL 与属性路径语义 | 待修改 |
| 12 | 结构化结果接口 | 待修改 |
| 13 | 错误传播、分页与资源释放 | 待修改 |
| 14 | 查询与事务上下文隔离 | 待修改 |
| 15 | 流式扫描与缓存优化 | 待修改 |
| 16 | 统计信息、索引选择与 Leapfrog 接入 | 待修改 |
| 17 | 重启、并发与分布式回归测试覆盖 | 进行中 |

## 第一项：词项字典持久化与唯一 ID 分配

- 新增 `rdf_terms` 持久化正向字典，使用共享 ID 计数器。
- 通过条件更新预留 ID，通过“记录不存在时才插入”确保不同客户端对同一词项复用同一 ID。
- 允许并发竞争或操作中断造成 ID 不连续。
- 迁移已有 `rdf_idmap` 记录，保留原图 ID；旧记录冲突或不完整时，初始化返回失败。
- 本地字典支持恢复不连续的 ID，并拒绝冲突映射。
- 文件导入、`ImportPipeline` 和 `INSERT DATA` 接入持久化 ID 分配。
- 传播字典保存与加载错误，重复导入保留已有映射。
- 补充本地恢复测试，以及重新连接、并发分配、模型重新初始化和旧字典迁移的集群测试。

所有写入程序需要一起升级：旧版本仍在本地分配 ID，新分配器启用后，旧版本不得继续写入同一 keyspace。缺失的旧词项元数据无法仅凭数字图 ID 重建。

验证状态：补丁空白字符检查通过。当前 Windows 环境只有源码，缺少生成的 protobuf 头文件、Linux YugabyteDB 构建产物及完整的 C++ 标准库，尚未运行运行时测试。

在支持的构建环境中执行以下测试：

```
.。release --cxx-test mdb_import_test --sj --skip-pg-parquet --no-odyssey --no-ybc
./yb_build.sh release --cxx-test mdb_import_integration_test --sj --skip-pg-parquet --no-odyssey --no-ybc
```
````
