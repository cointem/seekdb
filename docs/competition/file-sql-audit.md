# 文件 SQL 系统核对记录（2026-10-27）

本次按解析与绑定、Schema、文件发现、扫描与重扫、原生 SQL、计划缓存、取消和导出清理检查完整链路。以下记录的是已复现的问题和对应修复，不代表任意输入或任意平台都经过验证。

## 已复现并修复

| 级别 | 问题与具体触发 | 修复和证明 |
| --- | --- | --- |
| Blocker | 损坏 Parquet Footer 的未知 DOUBLE 字段截断会在依赖库中越界；损坏定义级别或未知页类型也会触发 panic。release 使用 panic=abort，可能终止服务器。 | 在依赖库前校验 Compact framing、字段读取类型、Schema、统计值和资源尺寸；固定已验证 Footer/页头；逐页检查实际解压数据的级别、字典索引及字符串长度。保留两个 panic 回归，增加变异输入测试。 |
| Blocker | 同一条 glob 查询先返回空集，新建匹配文件后仍返回空集；绑定文件身份和 Schema 也可能随计划缓存跨语句保留。 | 在全局 QueryCtx 标记文件 SQL，读取、glob、COPY 均不进入计划缓存。覆盖普通语句和预处理语句的重执行及文件变更。 |
| Major | CSV/JSONL 全量推断和 Parquet 扫描准备期间没有查询取消检查；宽表列匹配还会反复线性搜索，放大扫描成本。 | Rust 长循环回调 seekdb worker/session 状态，C++ 保留原超时/取消错误码；按名称建立列索引。覆盖推断中取消和原生 SQL 超时响应。 |
| Major | Parquet COUNT(*) 强制解码第一列；第一列损坏时，即便只需要 Footer 行数也会失败。 | 零物理列投影按 Footer 产生行数，其他投影只解码所需列。验证第 1/2 列损坏时未使用列查询和 COUNT(*) 仍可执行，实际读取坏列报错。 |
| Major | COPY 的临时文件名被替换后，可能发布替代内容，或在清理时删除替代文件；glob 库在结果上限生效前会收集完整目录。 | COPY 发布前后和清理时核对文件身份；glob 改用目录句柄逐项读取、去重和限额，支持取消。覆盖替代文件保留、10001 个符号链接去重和 10001 个唯一结果拒绝。 |

同时规范化 COPY 的 delim/delimiter 参数别名，拒绝冲突重复参数；缺失的允许目录返回 glob 空集。原有文件打开、目录替换、只读边界、坏行和类型边界回归一并运行。

## 覆盖范围

- Rust 功能回归共 28 个：CSV/JSONL、全部类型合并组合、边界与错误、文件发现、变化与重扫、取消、COPY、Parquet。
- Parquet 正常组合包括 PLAIN/字典、Snappy、Data Page V1/V2、两 Row Group、可空值、UTF-8、多列投影、DATE、微秒 DATETIME 和负 epoch。
- Parquet 损坏输入包括 256 个 Footer 变异、384 个数据页变异，以及 6 个针对 Schema/统计值/行数的构造用例。上述用例要求错误返回而非 panic。
- SQL 对照以 seekdb 普通表为预期结果，覆盖 16 个关系运算组合：表达式、分组/HAVING、聚合、DISTINCT、INNER/LEFT/CROSS JOIN、重复/NULL 键、子查询、CTE、UNION、列重排和重扫。
- 原生 SQL 还覆盖多列 Parquet 类型和精度、跨格式 JOIN、预处理语句、文件变更、开关、参数错误、COPY 读回/不覆盖/取消清理，以及坏 Footer 后服务器存活。
- 新增测试入口：`unittest/sql/engine/file_sql/test_file_sql_edges.py`；其 Parquet 样例为本地生成的 `data/matrix.parquet.b64`。

## 平台和资源边界

本轮运行平台为 macOS；Unix 使用目录句柄与禁止跟随替换链接的文件打开。非 Unix 文件入口明确返回不支持，在同等级的安全文件 API 实现之前不会回退到未经加固的路径操作。

| 对象 | 当前上限 |
| --- | --- |
| CSV/JSONL | 1024 列；16 MiB 原始记录/行 |
| glob | 10000 个规范化唯一结果；100000 个遍历项；128 个路径组件；不支持递归 `**` |
| Parquet Footer | 16 MiB；Compact 嵌套 64 层；1000000 个字段/容器值预算；仅扁平 Schema |
| Parquet 页头 | 每个 64 KiB；总缓存 16 MiB、最多 100000 个 |
| Parquet 数据页 | 压缩和解压尺寸各 64 MiB；每页最多 1000000 个值；字符串单值 16 MiB |
| Parquet 投影 | 一个 Row Group 的所需列最大页尺寸之和不超过 128 MiB |

超限返回错误。头部缓存只保存被验证的页头，不保存全文件行数据；实际数据按页读取、解压、校验和消费。COUNT(*) 可以只使用 Footer；未读取的列不作为损坏数据页校验对象。

## 验证入口

```sh
cargo test --offline --manifest-path rust/Cargo.toml -p file-sql
cargo clippy --offline --manifest-path rust/Cargo.toml -p file-sql --all-targets -- -D warnings
cmake --build build_release --target seekdb -j 4
```

SQL 两个脚本使用 `--client`、`--port`、`--data-dir`，在独立临时 seekdb 实例运行，结束后关闭实例并清理测试数据。构建和运行证据位于工作区外层 `validation/competition/file-sql-audit-*.log`。本记录不宣称运行了整个仓库的 mysqltest 或其他子系统的总回归。

## mysqltest 启动兼容性补验

收到评测日志 `unknown variable 'variable=FILE_SQL_DATA_DIR=...'` 后，使用仓库自带 mysqltest 复现。该客户端不支持 `--variable` 参数，会在连接数据库之前退出；修改数据库实现不能修正评测端的启动命令。

新增 `unittest/sql/engine/file_sql/run_mysqltest.py`，在测试文件中以 `let` 设置目录，复用 `file_sql_basic.test` 的全部 SQL 断言及原有 `.result` 文件。两种部署入口共享 `include/file_sql_queries.inc`，本地入口不依赖 SSH。运行说明见同目录 `README.md`。

在独立临时实例上连续两次执行真实 mysqltest，结果比较均通过；另以不存在的数据库验证失败码会向上传递。成功和失败后都核对了临时目录清理及既有文件保留。证据为工作区外层 `validation/competition/file-sql-mysqltest-validation.log`。此补验不代表赛题方私有评测已经通过；赛题方仍需移除不支持的参数，改用测试文件赋值或兼容入口后重新评测。
