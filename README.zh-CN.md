# tidb-binary-multistmt

[English](README.md) | 简体中文 | [日本語](README.ja.md)

把一个 batch 里的每条语句通过 MySQL 的**二进制协议**（`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`）打包进**一次**网络往返发出去，而不是一条语句一次往返。

## 一句话概览

- **是什么**：把每条语句的 `EXECUTE` 包连续写完，再统一读所有响应——跟
  `database/sql`"写一条、读完它的响应"的常规做法不一样。
- **好处**：在真实 TiDB Cloud 集群上实测，相比"一条语句一次往返"，事务
  P95 延迟大致减半（62.4ms → 31.5ms）。
- **代价**：一条语句的结果只能通过它的 `Callback` 拿到，事后拿不到；只支持悲观事务；暂不支持
  TLS 和协议压缩。详见[已知限制](#已知限制)。

## 为什么要做这个

N 条语句的 batch，这个库把 N 次往返压成 1 次——因为每个
`EXECUTE` 本来就带着自己的响应、顺序也跟发送顺序一致，只要连接保持健康，从
wire 上读到的第 N 个响应**就是**你发出去的第 N 条语句，不需要额外代价。

| | 一条语句一次往返 | pipeline（本库） |
|---|---|---|
| 事务 P95 | 62.4ms | 31.5ms |
| TiDB CPU | 205% | 195% |

### 背景：为什么用二进制而不是文本协议

[tidb-multistmt](https://github.com/dulao5/tidb-multistmt) 已经用**文本协议**把
N 条语句压成一次往返了：把它们拼进一个 `COM_QUERY` 大包，每条语句前面插一个
`SET @_multistmt_statement_num=N` 标记，这样每条语句各自的成败才能从一个本来会压扁这类信息的响应流里恢复出来。一次生产环境的
CPU profile 对比发现，真正拖累 multi-statement 模式 CPU 的是这些标记用的
`SET` 语句，不是文本协议还是二进制协议这个选择本身。本库换成 pipeline 方式发二进制
`EXECUTE`，完全不需要标记。

## 用法

```go
conn, err := binarymultistmt.Dial(ctx, "user:pass@tcp(host:4000)/db")
if err != nil { ... }
defer conn.Close()

var failed []string
b := binarymultistmt.NewBatch()
b.Add("INSERT INTO accounts (id, balance) VALUES (?, ?)", []any{1, 100}, func(sr *binarymultistmt.StatementResult) {
    if sr.Err != nil {
        failed = append(failed, fmt.Sprintf("#%d (%s): %v", sr.Index, sr.SQL, sr.Err))
    }
})
b.Add("SELECT balance FROM accounts WHERE id = ?", []any{1}, func(sr *binarymultistmt.StatementResult) {
    if sr.Err != nil {
        failed = append(failed, fmt.Sprintf("#%d (%s): %v", sr.Index, sr.SQL, sr.Err))
        return
    }
    for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
        fmt.Println("balance:", row[0])
    }
})

res, err := conn.Execute(ctx, b)
var commitErr *binarymultistmt.CommitError
switch {
case errors.As(err, &commitErr):
    // 每条语句都成功了，但 COMMIT 本身被服务端拒绝了（比如写冲突）——
    // TiDB 已经在服务端自动回滚了，conn 仍然健康，没有什么需要 Rollback
    // 的。接下来自己决定要不要重试整个 batch。
    log.Println("commit rejected, already rolled back:", commitErr)
case err != nil:
    // 连接/协议层的失败——conn 已经不能再用了，Close 掉
    conn.Close()
    return err
case !res.AllSucceeded:
    for _, f := range failed {
        log.Println("failed:", f)
    }
    // Execute 没有发 ROLLBACK——事务还开在 conn 上。
    // 自己决定怎么处理（回滚、进一步检查、重试）并显式执行：
    if err := conn.Rollback(ctx); err != nil { ... }
default:
    // 每条语句都成功了——Execute 已经自己发了 COMMIT。
}
```

几个需要知道的点：

- 一条语句会不会返回结果集，是**自动检测**出来的——从服务端自己的
  `COM_STMT_PREPARE` 响应里拿，不需要你声明，也就不存在声明错了的问题。
- 一条语句的错误和结果，**只能**通过它的 `Callback`（见下文）拿到——`Execute`
  跑完之后不会保留任何逐条语句的记录，`ExecuteResult` 除了 `AllSucceeded`
  什么都不带。
- `Execute` 只在 batch 里每条语句都成功时才会发 `COMMIT`。只要有一条失败，它**不会**发
  `ROLLBACK`——事务留在 `conn` 上打开着，由你显式处理。
- 连接/协议层的失败（不是某条语句的错误，也不是 `*CommitError`）会让
  `Execute` 返回非 nil 的 `error`，`conn` 不能再用——`Close` 掉，重新
  `Dial` 一个新的。
- 如果每条语句都成功了，但服务端随后拒绝了 `COMMIT` 本身（比如写冲突），`Execute`
  会返回一个 `*CommitError`（用 `errors.As` 判断）。这种情况下 TiDB
  已经在服务端自动回滚了——`conn` 照样健康能用，也没有什么需要回滚的。
- 只支持悲观事务。pipeline 中途一旦有语句失败，并不会阻止已经写出去的那些
  `EXECUTE` 继续被执行（服务端根本不知道这些是"一个 batch"），所以行锁必须在每条语句执行时就已经拿到，不能推迟到
  `COMMIT`，"只要失败就整体回滚"才站得住。乐观事务把冲突检测推迟到
  `COMMIT` 才做，一旦那时候冲突，整个 batch 会一次性失败，没法知道是哪条语句冲突的。本库不支持乐观事务。

### `ExecuteAutoCommit`：完全跳过 `BEGIN`/`COMMIT`

`Execute` 发 `BEGIN` 的时候，是跟 batch 里的 `EXECUTE`
一起 pipeline 发出去的——不是单独写完就同步等一次往返。所以只要每条语句都已经
prepare 过（复用一条连接时的常见情况；见下面的[预处理语句缓存](#预处理语句缓存)），`Execute`
比起 pipeline 本身只多花一次往返：成功之后同步等一次
`COMMIT`。`ExecuteAutoCommit` 跑的是同一套 pipeline，但连
`BEGIN`/`COMMIT` 都不发——每条语句在自己执行的时候就各自提交，跟不开显式事务、逐条发语句一样（MySQL
默认的 autocommit 行为）：

```go
res, err := conn.ExecuteAutoCommit(ctx, b)
```

事后没有什么可以回滚的——不管执行到哪一步，已经执行的部分无论成功失败都已经落盘了，这时候再调用
`Rollback` 也只是个无害的空操作。这种模式下也不会出现
`*CommitError`，因为根本没有 `COMMIT` 会被拒绝。适合只读的 batch，或者那种部分失败确实不需要撤销的场景（比如尽力而为的日志写入）。只要需求是"要么整体落地、要么一条都不落地"，就用
`Execute`——拿不准的时候它仍然是更安全的默认选择。

### 连接池（`DB`/`AcquireConn`）

`Dial` 每次都给一条专用连接——短命的小工具够用，但对想把拨号+鉴权的代价只付一次、之后在很多个
batch 间复用连接的长期进程来说就浪费了。`Open`/`AcquireConn` 解决这个问题：

```go
db, err := binarymultistmt.Open("user:pass@tcp(host:4000)/db", 40) // maxConns
if err != nil { ... }
defer db.Close()

conn, err := db.AcquireConn(ctx) // 复用一条空闲连接，不够 maxConns 就新拨一条
if err != nil { ... }
defer conn.Close() // 把 conn 还给 db 的空闲池——或者直接丢弃，见下文

b := binarymultistmt.NewBatch()
b.Add(...)
res, err := conn.Execute(ctx, b)
```

这里的 `conn.Close()` 不会真的关掉 socket：会把 `conn` 还给 `db`
自己的空闲列表留给下次复用——除非 `conn` 遇到了连接/协议层的失败，这种情况下
`Close` 会自动直接销毁它。不管哪种情况，`Close` 都只调用一次，之后不要再用这个
`conn`。

这个连接池故意没用 `database/sql` 自带的那套：本库需要**确切知道**自己接下来要在哪条物理连接上讲裸协议，而
`database/sql` 的池子没有任何 API 能提前告诉调用方它要复用的空闲连接到底是哪一条。所以
`DB` 里的 `*sql.DB` 只负责拨号和鉴权；`AcquireConn`/`Close`
在它上面自己实现了一套空闲列表复用。

如果你的程序里已经有别的代码用常规方式构造了自己的 `*sql.DB`，请把它跟
`binarymultistmt.DB` 并列建立——而不是从它派生：这是两个各自独立拨号的连接池（`*binarymultistmt.DB`
不嵌入 `*sql.DB`，也不能赋值给它）。典型做法是用一个工厂函数从同一个 dsn
里同时返回两者：

```go
func NewPools(dsn string) (plain *sql.DB, binary *binarymultistmt.DB, err error) {
    plain, err = sql.Open("mysql", dsn)
    if err != nil { return nil, nil, err }
    binary, err = binarymultistmt.Open(dsn, 40)
    if err != nil { plain.Close(); return nil, nil, err }
    return plain, binary, nil
}
```

代码库里大部分地方继续用 `plain`，不用改；只有想用 pipelined binary batch
的代码路径才用 `binary.AcquireConn`。

### 预处理语句缓存

同一条 SQL 文本，一个 `Conn` 只会发一次 `COM_STMT_PREPARE`；之后每次用同样的文本
`Add`，都会复用缓存好的语句 ID。这个缓存跟 `Conn` 本身同生命周期（不管中间经过多少次
`Execute`，如果是从连接池来的，也不管经过多少轮 `AcquireConn`/`Close`），按
SQL 文本的原文做 key——这对动态拼出来的文本（比如下面
[`ExpandIn`/`ExpandValues` 那一节](#where-id-in---批量-insert)）意味着什么，见那一节。

超过 `SetStmtCacheLimit` 设的条数（默认 90）之后，每加一条新语句就会淘汰最近最少用的那条，并对它发
`COM_STMT_CLOSE`——这样不管是本库自己的 map 还是服务端的 prepared statement
表，都不会在一条长期存活的 `Conn` 上无限涨下去。90 这个默认值刻意比 TiDB 自己的默认值
`tidb_session_plan_cache_size`（每个 session 100 条计划，以 v8.5 为准）略低一点——因为本库会独占这条连接，那个
session 的 plan cache 里存的全是本库自己 prepare 出来的计划，所以客户端缓存稍微留点余量，能保证淘汰顺序由这条
`Conn` 自己的 LRU 决定，而不是偶尔跟服务端自己的淘汰撞车：

```go
conn.SetStmtCacheLimit(50) // 比如用来匹配一个非默认的 tidb_session_plan_cache_size
```

要在 `Dial`/`AcquireConn` 之后、第一次 `Execute` 之前调用——它只影响之后
`prepare()` 做的淘汰。`n <= 0` 会彻底关掉淘汰（缓存——以及它背后服务端的 prepared
statement 表——会无限增长）。

### Callback：在语句排队的地方就地处理，并流式读取结果

`Add` 的第三个参数，只要非 nil，就会被 `Execute` 同步调用恰好一次——按排队顺序，在这条语句的响应刚好可用的那一刻：

```go
b := binarymultistmt.NewBatch()
b.Add("SELECT id, balance FROM accounts WHERE balance > ?", []any{1000}, func(sr *binarymultistmt.StatementResult) {
    if sr.Err != nil {
        log.Printf("query failed: %v", sr.Err)
        return
    }
    for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
        fmt.Println(row[0], row[1])
    }
    if err := sr.Rows.Err(); err != nil {
        log.Printf("stream broke mid-result: %v", err)
    }
})
res, err := conn.Execute(ctx, b)
```

对于会返回结果集的语句（`sr.HasResultSet`），`sr.Rows` 是一个
`*RowIterator`，callback 调用 `Next()` 的时候它会直接从 wire 上读取并解码——`Execute`
从来不会把整个结果集缓存进内存。一条用 `nil` Callback 排进去的会返回结果集的语句，它的行照样会被
drain 掉（保证后面语句在 wire 上对齐），只是不会被解码或暴露出来——所以如果你关心某条语句的结果，就得给它一个
Callback。

callback 也可以提前结束读取（比如读到第一条匹配的行就 `break`）：没读完的部分会在它返回之后自动被
drain 掉，不需要你自己读到 EOF，同一个 batch 里后面的语句也照样能正确对齐。

一条语句如果在还没产生结果集头部之前就直接失败了（来的是 ERR 包），它的
callback 依然会被恰好调用一次，带着 `sr.Rows == nil` 和设置好的
`sr.Err`。

`Next()` 按 `sr.Rows.Columns()` 里的每一列解码出一行，SQL 的 `NULL`
对应 `nil`。各 MySQL 列类型对应的 Go 类型：

| MySQL 类型族 | Go 类型 |
|---|---|
| `TINY`/`SHORT`/`LONG`/`LONGLONG`/`INT24`/`YEAR` | `int64`，如果列是 `UNSIGNED` 则是 `uint64` |
| `FLOAT`/`DOUBLE` | `float64` |
| `DATE`/`DATETIME`/`TIMESTAMP` | `time.Time` |
| `TIME` | `time.Duration`（可以是负数；MySQL 的 `TIME` 并不限定在 24 小时以内） |
| `VARCHAR`/`TEXT`/`BLOB` 系列/`DECIMAL`/`JSON`/`ENUM`/`SET`/`BIT`/`GEOMETRY` | `[]byte`——本库不知道一列的字符集，所以把转成 `string`（以及它的代价）留给调用方 |

### `WHERE id IN (?)` / 批量 `INSERT`

两个辅助函数，在 `Batch.Add` 之前调用：

```go
sql, args, err := binarymultistmt.ExpandIn("SELECT c FROM t WHERE id IN (?)", []any{ids})
b.Add(sql, args, nil)

sql, args, err := binarymultistmt.ExpandValues("INSERT INTO t (id, c) VALUES (?, ?)", rows)
b.Add(sql, args, nil)
```

变长的 `IN` 列表会随着长度改变渲染出来的 SQL 文本（也就跟着改变了本库内部
PREPARE 缓存用的 key），所以如果某个调用点的列表长度经常变化，从 PREPARE
复用里获得的收益会很有限——超过
[`SetStmtCacheLimit`](#预处理语句缓存) 之后还会主动把 LRU
搅乱（每种不同长度都会淘汰一条旧的、冷
`PREPARE` 一条新的）。如果这对你的负载很重要，自己把长度 pad
成固定的几档桶大小。

## 工作原理

MySQL 的命令包都自带长度前缀、自分隔——协议本身并不要求命令之间必须有一次往返。`database/sql`
只是没有暴露"先写后读"这种 API。

`BEGIN`（一条 `COM_QUERY`）跟它后面 pipeline 的 `COM_STMT_EXECUTE`
适用的是同一个道理：`Execute` 会把 `BEGIN` 和 batch 里每条语句的
`EXECUTE` 连续写完，再按顺序依次读 `BEGIN` 的响应和每条 `EXECUTE`
的响应——而不是先等 `BEGIN` 自己的 OK 回来才去写别的。本库早期版本里"要多花两次往返"说的就是后面这种写法。

本库没有去 fork go-sql-driver/mysql 绕开这个限制，而是通过驱动自己公开的
[`mysql.RegisterDialContext`](https://github.com/go-sql-driver/mysql)
钩子注册一个自定义拨号函数，在 go-sql-driver 照常完成握手/鉴权的同时截获真正的
`net.Conn`。等 `Dial` 返回时，鉴权已经完成，之后本库会直接接管所有读写——底层的
`*sql.Conn`/`*sql.DB` 只用来占住连接池的那个槽位，再也不会通过驱动 API 使用（本库的裸写操作会打乱驱动内部按连接维护的包序号记账）。

## 现状

实验性质，是从 [database_workload](https://github.com/dulao5/database_workload)
内嵌的一个 benchmark 里迁出来的。已经在真实 TiDB（v8.5.8）上验证过：

- pipeline 的 insert 能正确提交；batch 中间一个重复主键的失败会被准确归因到对应的语句，并把事务留在打开状态等
  `Rollback`
- batch 中间的一条 `SELECT` 不会让它后面的语句错位
- 每一种支持的参数类型都能正确往返，通过普通驱动连接读回来确认（不只是本库自己编解码自洽）
- `ExpandIn`/`ExpandValues` 跟 pipeline 执行路径端到端组合正确
- 一条覆盖所有支持列类型的 `SELECT`（包括一个 unsigned 最大值和一个微秒精度的
  `DATETIME`）能通过 `RowIterator` 正确解码，用的是 TiDB 真正发出来的列元数据和行字节
- `ExecuteAutoCommit`：同一个 batch 里后面的语句失败，不会回滚前面已经成功的语句，因为根本没开过事务
- `*CommitError`：用一次真实的写冲突验证过（两个 `Conn` 在 TiDB 的乐观事务模式下并发
  `UPDATE` 同一行）——输掉冲突那个 `Conn` 的 `Execute` 会返回
  `*CommitError`，而且确认这同一个 `Conn` 之后还能正常发起另一次 `Execute`
- `BEGIN` 跟 batch 里的 `EXECUTE` 一起 pipeline 发出去（而不是单独写完同步等一次）照样能正确提交事务
- 从 `SetStmtCacheLimit` 的 LRU 缓存里被淘汰（并被 `COM_STMT_CLOSE`
  掉）的语句，下次在同一个 `Conn` 上复用同样的 SQL 文本时，能正常重新 prepare 并执行

每一个解析 wire 上字节的入口（`readPacket`、`decodeColumnDef`、`decodeBinaryRow`、`drainExecuteResponse`、`readOKorErr`）都配有一个
Go 原生 fuzz target（`fuzz_test.go`）——CI 每次 push 都会短暂跑一下，完整语料库（包括过去发现过的崩溃用例）每次
`go test` 都会作为普通的确定性测试重放一遍。Fuzzing 已经用这种方式发现并修复过一个真实
bug：一个不可信的 column count 被直接当成 slice 容量参数用，在一个恶意或意外出现的超大值上触发了
panic。

参数绑定全程使用真正的 `COM_STMT_EXECUTE` 二进制协议参数——从不走字符串字面量拼接——所以这里完全不存在基于转义的注入面需要考虑。

## 已知限制

- **暂不支持 TLS。** dial-hook 的截获发生在 go-sql-driver/mysql
  握手过程中用 `tls.Client(...)` 包住 net.Conn**之前**，所以如果 DSN
  要求用 TLS，本库最终会把明文的二进制协议字节写到一条服务端认为应该加密的连接上。这个问题修好之前，只在不需要
  TLS 的连接上使用本库（比如私网内部链路）。
- **MySQL 协议压缩（`compress=true`）同样不支持，原因跟 TLS 一样**——dial-hook
  的截获发生在 go-sql-driver/mysql 套上压缩帧之前。跟 TLS 一起搁置。
- **不支持 `COM_STMT_SEND_LONG_DATA`。** 每个参数值必须塞进单个包里（`maxPacketPayload`，约
  16MB）——对普通列值来说相当宽裕，只有真正超大的 BLOB/TEXT 才会受影响。
- **假定 `CLIENT_DEPRECATE_EOF` 永远不会被协商开启。** 本库是搭便车用 go-sql-driver/mysql
  的握手，而这个驱动目前并不会协商这个 capability（看过源码确认的）。如果你
  vendor 了一个会协商 deprecate-EOF 的驱动版本，先确认一下这一点——它的失败模式是悄无声息的解析错位，不是一个响亮的报错。

## License

MIT
