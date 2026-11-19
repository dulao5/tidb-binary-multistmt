# tidb-binary-multistmt

[English](README.md) | 简体中文 | [日本語](README.ja.md)

基于 MySQL **二进制协议**（Binary Protocol）的高性能 TiDB 批量语句流水线执行库（Pipeline）。**模拟 PostgreSQL 的 Pipeline 特性**，把事务内的多个语句打包在 **2 次 TCP 往返** 内完成，充分利用 TiDB Plan Cache，在显著降低延迟的同时大幅削减 CPU 消耗。

---

## 痛点与背景：为什么做这个？

在业务开发中，一个事务往往包含多条 SQL（例如：查询余额、扣减库存、创建订单、记录日志）。

### 1. 传统的困境：网络往返延迟 (RTT)
使用传统的 `database/sql` 执行事务时，每条 SQL 都必须“发一条、等一条的响应”。如果事务里有 5 条语句，加上 `BEGIN` 和 `COMMIT`，一共需要产生 **7 次网络往返**。在云环境跨可用区部署时，网络往返延迟（RTT）往往在数毫秒以上，这导致大部分事务耗时都白白浪费在等待网络往返上。

### 2. TiDB 原生 Multi-Statements（文本模式）的局限
为了减少网络往返，很多开发者想利用 TiDB 原生的 **Multi-Statements** 特性把多条 SQL 一起发送给服务器：
- **优点**：多行 SQL 一起发送后，如果中间某条 SQL 报错，TiDB 会**立刻停止**执行后续的 SQL。
- **缺点**：TiDB 的 Multi-Statements **仅支持文本模式**（Text Protocol）：
  1. **直接发送原始 SQL 文本**：无法有效命中 TiDB 的 **Plan Cache**（执行计划缓存），TiDB 需要对每条 SQL 反复进行语法解析和优化，导致 **TiDB CPU 开销大幅飙升**。
  2. **发送文本 PREPARE / EXECUTE**：MySQL 文本协议在 `EXECUTE` 传参时**必须通过用户变量**（`@var`），这意味着每次传参前都必须额外插入一堆 `SET @a=...` 语句。实际压测和 CPU Profile 发现，这堆额外的 `SET` 语句带来了巨大的解析与执行开销，得不偿失。

### 3. 设计灵感：模拟 PostgreSQL 的 Pipeline 特性
熟悉 PostgreSQL 的开发者可能了解其强大的 **Pipeline 模式**（如 `pgx` 或 `libpq`），允许客户端把多条 SQL 打包连续发出，随后流水线式读取各自的响应。

相比之下，普通的 MySQL 客户端驱动（如 `database/sql`）并不支持 Pipeline：
- **无法成批发送多条 SQL**；
- 也**无法逐个返回这一批 SQL 中每条语句的独立结果（Result 或 Error）**。

但从底层原理来看：
- **TCP 协议天然支持在同一个 TCP 包/流中连续发送多个 MySQL Command**；
- **MySQL 二进制协议也天然包含每条语句各自独立的响应包（OK / ERR / ResultSet）**。

协议层面原本就支持这一切，只是传统的 MySQL 驱动没有封装出这样的接口。本库正是挖掘了这一底层协议潜力，把它包装成了简单易用的 Go 接口，为 TiDB / MySQL 带来了媲美 PostgreSQL 的 Pipeline 能力！

---

## 核心方案：二进制 Multi-Statements (Pipeline)

为了兼顾“低延迟”与“低 CPU”，我们开发了 **`tidb-binary-multistmt`**，彻底放弃 TiDB 原生的文本模式 Multi-Statements，转向 MySQL **二进制协议**（Binary Protocol）+ **网络流水线**（Pipeline）：

1. **共享 Plan Cache，降低 CPU**：基于二进制预处理语句（`COM_STMT_EXECUTE`），自动复用执行计划，极大减少 TiDB 解析和优化 SQL 的 CPU 消耗。
2. **零 `SET` 负担**：参数直接编码在二进制协议包中，无需用户变量，完全消除了额外的 `SET` 语句开销。
3. **极致压缩往返（仅需 2 次 TCP 交互）**：将 `BEGIN` 与所有语句的二进制 `EXECUTE` 打包在同一个 TCP 包中一口气发送，随后读取所有响应。

### 方案的权衡与解决：中间出错怎么办？
- **潜在缺陷**：多条 SQL 在同一个 TCP 流中一次性发送后，由于服务端的 socket 缓冲区已接收所有命令，如果中间某条 SQL 执行出错，**服务端仍会继续执行后续已经收到的 SQL**（无法像文本 Multi-Statements 那样由服务端自动中断）。
- **我们的解法：始终使用悲观事务（Pessimistic Transactions）**：
  - 本库**强制且仅支持悲观事务**。在悲观事务中，语句执行时即加行锁。
  - 客户端流式接收全部响应，精准判断每一条语句的成败。
  - **决策提交或回滚**：如果所有语句均成功，客户端发送 `COMMIT`；只要有任何一条语句出错，客户端发送 `ROLLBACK`。
  - 即使后续语句被执行了，由于处于悲观事务中，一次 `ROLLBACK` 就能干净地撤销所有修改，**彻底保证事务的原子性（All-or-Nothing）**！
  - 整个悲观事务仅需 **2 次 TCP 通信**。

---

## 架构与执行流程 (ASCII 图)

### 方案对比
```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. 传统逐条执行 (N + 2 次往返): 语句多 = 往返多 = 延迟极高                   │
│    App ── BEGIN ──> TiDB ──> App ── INSERT ──> TiDB ──> App ── COMMIT ──> TiDB │
│    [RTT 1]                  [RTT 2]                    [RTT 3]              │
├─────────────────────────────────────────────────────────────────────────────┤
│ 2. TiDB 原生 Multi-Statements (文本模式): 延迟降低，但 CPU 成本高           │
│    App ── "SET @a=1; INSERT ...; SET @b=2; UPDATE ...;" ───────────> TiDB    │
│    - 原生 SQL 无法命中 Plan Cache，CPU 解析开销大                           │
│    - 文本 PREPARE 需要 SET @var 传参，带来大量额外语句开销                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ 3. 本库: 二进制 Pipeline (模拟 PG Pipeline，极低延迟 + 极低 CPU)             │
│    App ── [ BEGIN + COM_STMT_EXECUTE(二进制传参) + ... ] ──────────> TiDB    │
│    - 突破 MySQL 普通驱动限制，成批发送并逐个获取每条 SQL 的结果与错误       │
│    - 二进制传参，无 SET 负担；天然命中 Plan Cache，大幅削减 CPU             │
│    - 无论包含多少条语句，整个悲观事务仅需 2 次 TCP 通信！                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 悲观事务两阶段往返流程
```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ 第 1 次 TCP 往返: 一口气发送 BEGIN 与所有二进制 EXECUTE 命令                │
│                                                                             │
│ App ───────────── [ BEGIN + EXECUTE 1 + EXECUTE 2 + EXECUTE 3 ] ──────────> TiDB
│                                                                             │
│                                                                (TiDB 顺序执行)
│                                                                             │
│ App <──────────── [ OK(BEGIN) + OK(stmt1) + ERROR(stmt2) + OK(stmt3) ] <── TiDB
│  │                                                                          │
│  │ (客户端流水线接收每个语句的响应，精准定位出错语句)                        │
└──┼──────────────────────────────────────────────────────────────────────────┘
   │
   ├─► [全部成功] ─── (第 2 次 TCP 往返) 发送 COMMIT   ───> 事务提交生效
   │
   └─► [任一失败] ─── (第 2 次 TCP 往返) 发送 ROLLBACK ───> 事务完全回滚，保证原子性
```

---

## 性能表现

在真实的 TiDB Cloud 集群实测对比（每个事务包含多条写与读操作）：

| 指标 | 逐条往返执行 (`database/sql`) | 二进制 Pipeline（本库） | 改善幅度 |
|---|---|---|---|
| **事务 P95 延迟** | 62.4 ms | **31.5 ms** | **降低约 50%** |
| **TiDB CPU 使用率** | 205% | **195%** | 命中 Plan Cache，CPU 稳中有降 |

---

## 快速上手

### 安装
```bash
go get github.com/dulao5/tidb-binary-multistmt
```

### 基本用法

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	binarymultistmt "github.com/dulao5/tidb-binary-multistmt"
)

func main() {
	ctx := context.Background()
	dsn := "root:@tcp(127.0.0.1:4000)/test"

	// 1. 建立连接 (生产建议使用连接池 binarymultistmt.Open)
	conn, err := binarymultistmt.Dial(ctx, dsn)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	// 2. 组装 Batch 任务
	b := binarymultistmt.NewBatch()

	// 添加写语句：在回调函数中检查该语句的执行结果
	b.Add("INSERT INTO accounts (id, balance) VALUES (?, ?)", []any{1, 100}, func(sr *binarymultistmt.StatementResult) {
		if sr.Err != nil {
			log.Printf("第 #%d 条写入失败: %v", sr.Index, sr.Err)
		}
	})

	// 添加读语句：通过 Rows 流式读取查询结果
	b.Add("SELECT id, balance FROM accounts WHERE id = ?", []any{1}, func(sr *binarymultistmt.StatementResult) {
		if sr.Err != nil {
			log.Printf("第 #%d 条查询失败: %v", sr.Index, sr.Err)
			return
		}
		for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
			fmt.Printf("账户 ID=%v, 余额=%v\n", row[0], row[1])
		}
	})

	// 3. 一次性打包执行 (第 1 次 TCP 往返: 发送 BEGIN + 所有 EXECUTE)
	res, err := conn.Execute(ctx, b)

	// 4. 事务结果判定与处理
	var commitErr *binarymultistmt.CommitError
	switch {
	case errors.As(err, &commitErr):
		// 所有语句都执行成功，但 COMMIT 时被服务端拒绝（例如写冲突）
		// TiDB 服务端已自动回滚，连接依然健康，可根据业务需要决定是否重试整个 Batch
		log.Printf("提交被拒绝，服务端已自动回滚: %v", commitErr)

	case err != nil:
		// 网络断开或协议级错误：连接已不可用，关闭连接
		conn.Close()
		log.Fatalf("网络/协议错误: %v", err)

	case !res.AllSucceeded:
		// 存在执行失败的语句：Execute 不会自动回滚，事务依然在连接上保持开启
		// 发送 ROLLBACK (第 2 次 TCP 往返)，安全回滚撤销所有已执行语句
		log.Println("Batch 中存在失败语句，正在回滚事务...")
		if rbErr := conn.Rollback(ctx); rbErr != nil {
			log.Printf("回滚失败: %v", rbErr)
		}

	default:
		// 全部语句执行成功：Execute 已自动发出 COMMIT (第 2 次 TCP 往返)
		log.Println("事务全部成功并已顺利提交！")
	}
}
```

---

## 常用特性

### 1. 生产级连接池 (`Open` / `AcquireConn`)
长期运行的后台服务中，应使用连接池复用已建立的物理连接与鉴权状态：

```go
// 创建连接池，指定最大连接数
db, err := binarymultistmt.Open(dsn, 40)
if err != nil { ... }
defer db.Close()

// 从池中获取一条连接
conn, err := db.AcquireConn(ctx)
if err != nil { ... }
defer conn.Close() // 归还连接到空闲池（若发生协议错误则自动销毁）

res, err := conn.Execute(ctx, b)
```

### 2. 预处理语句缓存 (Plan Cache)
- 连接首次执行某个 SQL 时会自动向服务端发送 `COM_STMT_PREPARE`，并缓存在连接的 LRU 中。
- 后续相同的 SQL 文本直接复用 `stmtID`，命中 TiDB 的 Plan Cache。
- 默认缓存上限为 90 条（可使用 `conn.SetStmtCacheLimit(n)` 调整），超出后自动以 LRU 顺序关闭并淘汰旧语句，防止内存和 TiDB 服务端内存无限膨胀。

### 3. 流式读取结果（零内存堆积）
对于 `SELECT` 语句，`sr.Rows` 提供 `Next()` 迭代器，直接从底层网络流边读边解包：
- 无需在内存中预先缓存整个大结果集。
- 允许提前 `break` 退出循环，未读取的剩余行会自动被排出（Drain），保证协议流对齐。

### 4. 辅助函数：动态 `IN` 与批量 `INSERT`
在动态拼接参数长度时，可使用内置辅助函数自动展开占位符：

```go
// 展开 IN (?, ?, ...)
sql, args, err := binarymultistmt.ExpandIn("SELECT * FROM t WHERE id IN (?)", []any{ids})
b.Add(sql, args, nil)

// 批量插入展开 VALUES (?, ?), (?, ?)
sql, args, err := binarymultistmt.ExpandValues("INSERT INTO t (id, name) VALUES (?, ?)", rows)
b.Add(sql, args, nil)
```

### 5. `ExecuteAutoCommit`：免事务极速执行
如果你的批处理全为**只读查询**，或者属于允许局部成功的日志写入等场景，可以使用 `ExecuteAutoCommit`：
- 不发送 `BEGIN` 和 `COMMIT`，所有语句在 autocommit 模式下执行。
- 整个批处理仅需 **1 次 TCP 往返**！

```go
res, err := conn.ExecuteAutoCommit(ctx, b)
```

---

## 注意事项与限制

1. **必须使用悲观事务**：本库不支持且不会支持乐观事务。乐观事务将冲突推迟到提交阶段，无法精准把错误归因于具体的单条语句，也无法在中间出错时由客户端决定回滚。
2. **暂不支持 TLS 与协议压缩**：当前连接劫持机制工作在握手层，暂未封装 TLS 加密流及压缩流。建议在 VPC 内网或同主机安全网络环境下使用。
3. **单参数大小上限**：不支持 `COM_STMT_SEND_LONG_DATA`，单个参数上限受 MySQL 单包大小限制（约 16MB），适用于常规业务数据，但不适合超大文件或大 BLOB 写入。
4. **语句结果仅在 Callback 中可见**：为了极致性能与内存零开销，`Execute` 执行完毕后不会在连接中保留逐条结果，请在 `b.Add` 的回调中处理需要的数据或错误。

---

## 开源协议

[MIT License](LICENSE)
