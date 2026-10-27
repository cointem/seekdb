# 赛题二：文件即数据库

| 项目 | 说明 |
| --- | --- |
| 项目 | seekdb |
| 评分项 | 8 项，按功能逐项计分 |
| 总分 | 50 分 |

## 一、题目背景

AI Agent 经常需要分析本地 CSV、JSONL、Parquet 文件。传统流程需要先建表、导入数据，再执行 SQL，增加了准备工作和数据副本。

本题要求在 seekdb 中实现“文件即数据库”：通过 `read_csv`、`read_json` / `read_ndjson`、`read_parquet` 直接读取文件，自动生成列结构，由 seekdb SQL 引擎完成表达式计算、过滤、聚合、排序和 JOIN。用户无需预先建表或导入数据。

## 二、范围与通用要求

1. 基于 seekdb SQL 引擎实现。允许使用格式解析、解压和文件访问库；不得使用 DuckDB、SQLite、Polars SQL 等外部查询引擎代执行 SQL。
2. 不得针对文件名、数据内容或查询结果硬编码。
3. 面向单进程、本地普通文件。输入只读，不得修改源文件，也不得通过自动建表、全量导入等方式代替文件扫描。

   当前参考实现在 Unix 平台提供文件入口；非 Unix 平台明确返回不支持，避免回退到未经加固的路径操作。
4. 功能默认关闭；会话开启 `enable_file_sql` 后，仍须通过本题规定的 `secure_file_priv` 校验。
5. CSV/JSONL 在绑定列结构时流式遍历整个文件。省略 `sample_size` 与显式指定 `sample_size = -1` 的行为相同；其他值报错。
6. 执行阶段支持批量取数、重扫和查询取消。文件读取与 Schema 推断不得在内存中累计全文件行数据；允许缓存列结构、受限的输入缓冲区及当前批次。
7. seekdb 原生排序、聚合、JOIN 的内存使用和落盘按现有执行引擎机制处理，不属于上述文件读取阶段的物化禁令。文件适配层不得自行缓存全文件以模拟重扫。
8. 普通 SQL 行为保持兼容。异常输入、路径越界、文件变化和资源超限应报错，并释放文件句柄、缓冲区等资源。

本题不要求远程文件、多文件自动合并、嵌套结构、分布式扫描或性能排名。COPY 导出属于扩展功能，不计分，也不是获得满分的前置条件。

## 三、SQL 接口

### 3.1 读取入口

```sql
SET SESSION enable_file_sql = ON;

SELECT id, name
FROM read_csv('/allowed/users.csv', header = true, sample_size = -1) AS u
WHERE id > 10
ORDER BY id
LIMIT 20;

SELECT * FROM read_json('/allowed/events.jsonl', format = 'newline_delimited');
SELECT * FROM read_ndjson('/allowed/events.jsonl');
SELECT * FROM read_parquet('/allowed/events.parquet');

DESCRIBE SELECT * FROM read_csv('/allowed/users.csv');
SELECT file FROM glob('/allowed/*.csv') ORDER BY file;
```

示例中的 `/allowed` 为占位目录，运行前须由管理员配置为允许访问的目录。

路径为第一个位置参数，必须是字符串字面量；其他参数使用 `name = value`。参数名不区分 ASCII 大小写。未知参数、重复参数、非字面量参数和格式不适用的参数均报错；`delim` 和 `delimiter` 视为同一个参数。

本题必做范围只包含绝对路径。相对路径可明确报错。每次 `read_*` 只读取一个文件，路径中的通配符不自动展开；多文件发现使用 `glob`。

| 函数 | 命名参数与默认值 |
| --- | --- |
| `read_csv` | `header = true`、`delimiter = ','`（别名 `delim`）、`nullstr = 'NULL'`、`sample_size = -1` |
| `read_json` | `format = 'newline_delimited'`、`sample_size = -1` |
| `read_ndjson` | 与 `read_json` 相同，仅支持 JSONL |
| `read_parquet` | 无必做命名参数，Schema 来自 Footer |
| `glob` | 无必做命名参数 |

`read_json` 不要求支持完整 JSON 数组或跨行 JSON 文档。普通查询支持表别名和列引用。`DESCRIBE SELECT * FROM read_*()` 应返回列名、SQL 类型和可空性；BOOLEAN 的底层 MySQL 类型码可沿用 seekdb，但描述结果须能识别其布尔语义。

### 3.2 文件发现

`glob(pattern)` 支持 `*`、`?` 和字符集合 `[abc]`，不要求递归 `**`。返回一列非 NULL 的 `VARCHAR`，列名为 `file`，内容为规范化绝对路径；仅返回普通文件，去重后按路径 UTF-8 字节序排列，无匹配时返回零行。

匹配目录和匹配结果均须接受路径校验。匹配到越界文件应使查询失败，不得返回其路径；匹配到目录则跳过。单次最多返回 10000 个文件，超限报错。`glob` 的结果在本次绑定时固定，重扫返回同一列表；后续读取文件仍需重新校验。

文件列表与 Schema 必须在每条新语句中重新绑定，不能从计划缓存中复用。当前实现还限制最多遍历 100000 个目录项、128 个路径组件；遍历期间支持取消，允许目录下不存在的路径返回空集。

## 四、文件格式与列结构

### 4.1 CSV

- 支持 UTF-8、LF/CRLF、末行无换行符；非法 UTF-8 报错。不要求支持 UTF-8 BOM。
- 分隔符必须是单字节 ASCII 字符，不能为 NUL、双引号、CR 或 LF。
- 双引号用于包围字段，包围字段内用两个双引号表示一个双引号，允许包含分隔符和换行。反斜杠不作为额外转义符；引号未闭合、未包围字段内出现引号、闭引号后出现非分隔字符均报错。
- `header = true` 时第一条逻辑记录为列名；`header = false` 时生成 `column1`、`column2` 等列名，第一条记录也是数据。
- 空列名先替换为对应位置的 `columnN`。从左到右处理重名，对当前名称依次追加 `__2`、`__3`，直到唯一；判重时将 ASCII 大写转为小写，输出保留原名称的大小写。例如 `id,id,id__2` 得到 `id,id__2,id__2__2`。
- 数据记录的字段数必须与列数一致。空物理行跳过；只有分隔符的记录仍是一条数据记录。
- 解析引号和转义之后，空字段及与 `nullstr` 完全相同的字段均视为 NULL，包围字段也适用此规则；其他字段保留空白，不自动 trim。
- 只有表头的文件返回零行，列类型为 NULL、可空。无任何逻辑记录的文件无法推断 Schema，应报错。
- 坏行使查询失败，不要求跳过坏行或返回部分成功结果。诊断至少包括文件、逻辑记录号和原因；逻辑记录号从 1 开始，包含表头，不计被跳过的空物理行。

### 4.2 JSONL

- 每个非空物理行是一个完整的扁平 JSON 对象。支持 LF/CRLF 和末行无换行符，空白行跳过。
- 支持 JSON 的 null、boolean、number、string；嵌套对象、数组和非对象顶层值报错。重复键报错，不能静默覆盖。
- 全量扫描后取所有对象的字段并集；列顺序按字段名 UTF-8 字节序排列，缺失字段补 NULL。字段名仅 ASCII 大小写不同且会导致 SQL 列引用歧义时，报错。
- 字段名不允许为空。空对象可作为一行存在；整个文件没有任何字段时无法推断 Schema，应报错。
- JSON 数字按原生数值参与推断；JSON 字符串中的 `"123"`、`"true"` 保持 VARCHAR，不当作数字或布尔值。合法日期或日期时间字符串按 4.4 节推断。
- 整数词法的 JSON 数字超出有符号 BIGINT 范围时报错，避免在进入类型推断前静默丢失整数精度。非整数数字必须可表示为有限 DOUBLE。
- 诊断至少包含文件、从 1 开始的物理行号和原因；存在明确字段时还应包含字段名。

### 4.3 Parquet

Schema 以 Footer 声明为准，不根据数据内容改判字符串类型；零行文件仍须返回声明的列结构。支持多个 Row Group、required/optional 列，以及只解码查询所需列的列裁剪；`COUNT(*)` 不依赖某个固定数据列。

| 必做类型 | SQL 映射 |
| --- | --- |
| BOOLEAN | BOOLEAN |
| 无逻辑标注的 INT32、INT64；有符号 INT 逻辑类型 | BIGINT |
| FLOAT、DOUBLE | DOUBLE，仅支持有限值 |
| UTF8/STRING 标注的 BYTE_ARRAY | VARCHAR，保持字符串语义 |
| DATE | DATE |
| TIMESTAMP，毫秒或微秒精度 | DATETIME(3) 或 DATETIME(6)；按 epoch 值转换为 UTC 日历时间，不随会话时区变化 |

必做编码为 PLAIN、RLE/RLE_DICTIONARY（含定义层级使用的 RLE），压缩为 UNCOMPRESSED、SNAPPY。测试覆盖 Data Page V1、多 Row Group 和字典编码。其他类型、编码、压缩及 Data Page V2 不计入必做；实现可以扩展，未支持时须明确报错。

嵌套、重复字段、DECIMAL、INT96、纳秒时间戳、无符号整数及未标注二进制不属于必做范围。损坏 Footer、损坏被读取的数据页和不支持的必需类型应报错。列名须非空且在 SQL 列引用规则下无歧义。无需为列裁剪而主动读取未使用列的数据页来检查其内容损坏。

### 4.4 确定性类型推断

CSV/JSONL 必须全量推断，列类型不能因采样大小、批次大小或输入行顺序而改变。CSV 列序仍由表头或字段位置决定；JSONL 列序按 4.2 节决定。

CSV 非 NULL 字段按以下顺序识别：

1. `true` / `false`（ASCII 大小写不敏感）为 BOOLEAN；`0` / `1` 为数字。
2. 匹配 `[+-]?[0-9]+` 且在有符号 64 位范围内为 BIGINT；整数词法超出此范围时为 VARCHAR，不转 DOUBLE。
3. 含小数点或十进制指数、符合十进制数值语法且可表示为有限值的字段为 DOUBLE。NaN、Infinity、十六进制数字及带前后空白的数值文本为 VARCHAR。
4. 严格符合 `YYYY-MM-DD` 且日历日期合法的字段为 DATE，年份范围为 0001 至 9999。
5. 严格符合 `YYYY-MM-DD HH:MM:SS[.ffffff]` 或使用 `T` 分隔日期与时间，且各部分合法的字段为 DATETIME；小数秒为 1 至 6 位，不含时区，不接受闰秒。无小数秒时精度为 0。
6. 其他字段为 VARCHAR。非法日期或日期时间文本因此保留为 VARCHAR。

JSONL 的 null、boolean、整数和非整数数字分别产生 NULL、BOOLEAN、BIGINT、DOUBLE；字符串只进行上述日期、日期时间识别，否则为 VARCHAR。

类型按下表合并，规则与输入顺序无关：

| 输入类型 | 合并结果 |
| --- | --- |
| NULL 与任意 T | T |
| 相同类型 | 该类型 |
| BIGINT 与 DOUBLE | DOUBLE |
| DATE 与 DATETIME | DATETIME，DATE 值补 `00:00:00` |
| 其他不同类型组合 | VARCHAR |

全 NULL 列的逻辑类型为 NULL，可空；不得丢弃此列。出现 NULL 或缺失值的列为可空。只有表头的 CSV 列也为可空；Parquet 的可空性遵循声明。DATETIME 精度取本列有效日期时间值的小数秒位数最大值。

发生 BIGINT→DOUBLE 宽化时按 IEEE 754 双精度语义处理，可能损失大整数精度；同样规则适用于预期结果。宽化为 VARCHAR 时，CSV 保留原字段文本；JSONL 字符串保留解码后的内容，布尔值为 `true` / `false`，数字输出为合法 JSON 数字文本，测试按数值含义比较其字符串表示。NULL 仍为 SQL NULL。

## 五、SQL 执行要求

单文件查询支持列投影、表达式、别名、WHERE、ORDER BY、LIMIT、GROUP BY、HAVING，以及 COUNT/SUM/AVG/MIN/MAX。双文件查询支持 INNER JOIN、LEFT JOIN，并覆盖 CSV、JSONL、Parquet 中至少两种格式之间的 JOIN。还须支持文件与 seekdb 普通表 JOIN。

上述运算须进入 seekdb 原生解析、绑定、优化和执行流程。文件适配层负责提供动态列结构和扫描行；允许投影等合理下推。执行计划和调用链应能证明 SQL 运算由 seekdb 完成。

无 ORDER BY 时不对结果行顺序作要求。表达式、聚合、排序、NULL 比较和 JOIN 语义沿用 seekdb；LEFT JOIN 未匹配侧输出 NULL，不得继承文件源列的 NOT NULL 属性。执行器请求重扫时须从文件开头重读，并保持绑定的列结构。

## 六、安全、变化检测与资源限制

### 6.1 路径边界

本题文件入口采用以下规则：`secure_file_priv` 未配置、为空或为 NULL 时拒绝访问；配置有效目录时，只允许读取该目录及其子目录的普通文件。此约定仅描述本题新增文件入口。

规范化路径后校验目录边界，不得使用容易被同名前缀目录绕过的字符串前缀判断。`..`、符号链接及打开时的路径替换不能绕过校验。符号链接只有在最终目标仍位于允许目录内时才可读取；实现也可统一拒绝符号链接。

读取函数、`glob` 和 `DESCRIBE` 均受功能开关和路径限制。文件实际打开时及重扫时仍须校验，不能仅信任绑定时缓存的路径。禁止读取目录、设备、FIFO 等非普通文件。

### 6.2 文件变化

正常输入在一条语句的绑定和执行期间保持不变。本题不要求对任意并发写入提供文件系统快照或事务隔离。

至少记录文件身份（如 device/inode）、大小和高精度修改时间，在 Schema 推断前后、执行打开时、每批读取前后（含 EOF）及重扫时检查。检测到截断、追加、原子替换、删除或修改元数据不一致时，本条语句报错，不得以成功状态返回混合版本的结果。

测试在可控检查点改变文件，确保至少一个上述属性可观察地变化；不使用保留全部身份与元数据的隐蔽改写作为必做用例。若协议已流式发送部分行，可随后返回语句错误；不要求撤回已发送的行。

### 6.3 资源与错误

每文件最多 1024 列；CSV 一条逻辑记录、JSONL 一条物理行最多 16 MiB（按原始 UTF-8 字节计，含记录内换行、不含记录末尾 LF/CRLF）。超过上限须报错，且应在持续读取时限制缓冲区增长。

当前参考实现限制 Parquet Footer 为 16 MiB、Compact 嵌套为 64 层、字段/容器值预算为 1000000；单页头为 64 KiB、页头缓存总量为 16 MiB 且最多 100000 个。单页压缩及解压尺寸各为 64 MiB、最多 1000000 个值，字符串单值为 16 MiB；一个 Row Group 的投影列最大页尺寸之和不超过 128 MiB。超限报错，正式评测数据与环境须遵守这些已公布边界。

文件扫描的内存不得随总行数线性增长。大文件测试使用固定大小的记录，避开上述单行、列数和单值边界，并随公开测试公布输入规模、进程内存预算、超时和运行命令。

解析错误、文件变化、超限、取消和重复打开/关闭后不得泄漏资源或导致崩溃。错误码可遵循 seekdb 现有分类；除公开测试明确声明外，不要求某个数值错误码或完整错误文本逐字相同。

## 七、评分标准

| 子题 | 评分重点 | 分值 |
| --- | --- | ---: |
| SQL 接口与文件发现 | 读取入口、命名参数、别名、动态元数据、DESCRIBE、glob | 7 |
| CSV 读取 | 表头、UTF-8、换行、引号转义、NULL、坏行定位与批量读取 | 10 |
| JSONL 读取 | 扁平对象、字段并集、缺失值、类型冲突与错误定位 | 7 |
| Parquet 读取 | Footer Schema、多 Row Group、可空列、规定类型/编码/压缩与列裁剪 | 6 |
| 确定性类型推断 | 全量扫描、合并规则、数值边界、日期合法性和全 NULL 列 | 6 |
| 单文件 SQL | 表达式、过滤、排序、LIMIT、分组、HAVING、五类聚合与原生计划 | 6 |
| 双文件 JOIN | INNER/LEFT、跨格式、文件与普通表、原生计划和重扫正确性 | 4 |
| 安全、稳定性与工程质量 | 默认关闭、路径校验、文件变化、资源限制、取消和测试 | 4 |
| 合计 | | 50 |

每项按测试清单中预先分配的权重计分；公开测试与隐藏测试的权重随赛题测试说明公布，不能仅用不同数量的断言改变权重。各项独立计分，某一格式未实现时只影响依赖该格式的用例。

使用外部查询引擎代执行 SQL 或针对测试硬编码，整题 0 分。可复现的越权文件访问、绕过 `secure_file_priv` 或修改源文件，安全项 0 分，并按随赛题公布的违规处理规则处理；不得将未公布的处罚作为隐藏评分条件。因全文件物化而超过已公布内存上限的用例判定失败。

隐藏测试只覆盖本文已经定义的行为；不以未公布的新参数、格式或边界规则扣分。COPY 扩展的实现与否不影响上述 50 分。

## 八、测试与提交

当前仓库中的测试入口如下。它们是基础回归测试，不代表已经覆盖本文全部要求。

| 类型 | 文件或入口 | 当前覆盖内容 |
| --- | --- | --- |
| Rust 单元测试 | `rust/file-sql/src/lib.rs` 中的 `#[cfg(test)]` | CSV、JSONL、Parquet、重扫、路径限制、变化检测及扩展 COPY 的部分行为 |
| SQL 集成测试 | `unittest/sql/engine/file_sql/test_integration.py` | 读取、聚合、过滤、跨格式 INNER JOIN、交叉连接、元数据和扩展 COPY |
| SQL 边界矩阵 | `unittest/sql/engine/file_sql/test_file_sql_edges.py` | 普通表结果对照、多列 Parquet、CTE/UNION/子查询、预处理语句、取消和异常路径 |
| Mysqltest | `tools/deploy/mysql_test/test_suite/file_sql/t/file_sql_basic.test` | 开关、基础格式、聚合、DESCRIBE、glob 和扩展 COPY |
| 测试数据 | `tools/deploy/mysql_test/test_suite/file_sql/data/` | CSV、JSONL 和 Base64 编码的 Parquet 样例 |

Rust 单测从仓库根目录执行：

```sh
cargo test --manifest-path rust/Cargo.toml -p file-sql
```

SQL 集成测试需先启动本地 seekdb，准备可连接的 `test` 数据库、MySQL 客户端及允许读写测试夹具的目录：

```sh
python3 unittest/sql/engine/file_sql/test_integration.py \
  --client /absolute/path/to/mysql \
  --port 12883 \
  --data-dir /allowed/test-fixtures
```

现有两个 SQL 测试入口包含 COPY 检查；发布本题正式计分测试时，须将 COPY 检查移入可单独运行的扩展套件，不能直接把当前脚本的整体退出码作为本题得分。

提交应包含实现代码、测试及运行说明，并说明文件扫描与 seekdb 原生执行计划的衔接方式、资源上限和未支持的扩展功能。正式评测前须补齐公开验收矩阵，并确认参考实现与本文一致。

## 九、扩展：COPY 导出（不计分）

可以支持 `COPY table TO path` 或 `COPY (query) TO path` 导出 CSV，包含表头、分隔符、NULL 文本选项，以及与 CSV 读取一致的双引号转义。

COPY 是独立的写文件扩展，不改变读取入口对源文件的只读要求。启用后须校验功能开关和输出目录权限；目标存在时失败，成功后原子发布结果，失败或取消时清理临时文件。

导出后可使用匹配参数的 `read_csv` 读回。按本题 CSV 的 NULL 约定，空字符串和等于 `nullstr` 的字符串可能被识别为 NULL；不承诺所有字符串和原始 SQL 类型均无损往返。实现若提供更强的往返语义，应另行说明。
