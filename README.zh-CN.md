# tidb-binary-multistmt

[English](README.md) | 简体中文 | [日本語](README.ja.md)

把一个事务里的多条语句通过 MySQL 的**二进制协议**（`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`）背靠背发送，不等任何一条的响应——也就是把每条语句的 `EXECUTE` 包连续写完之后再统一读响应，而不是 `database/sql`（以及
[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)）那种"写一条命令、同步读完它的响应"的常规做法。

## 为什么要做这个

[tidb-multistmt](https://github.com/dulao5/tidb-multistmt) 已经用**文本协议**解决了"N 条语句一次往返"的问题：把所有语句打包进一个用分号拼接的 `COM_QUERY` 大包（`CLIENT_MULTI_STATEMENTS`），每条语句前面插一个
`SET @_multistmt_statement_num=N` 标记，这样客户端才能从一个本来会把成功/失败信息全部压扁的响应流里，恢复出每条语句各自的成败。

一次真实生产环境的 CPU profile 对比发现，这些标记用的 `SET` 语句才是 multi-statement 模式比"一条语句一次往返"多耗 CPU 的主要原因——跟文本协议还是二进制协议本身没关系。这个库换了个思路，用 pipeline 方式发送**二进制** `EXECUTE`：完全不需要标记，因为每个 `EXECUTE` 本来就是一条独立命令、带着自己的响应，而且响应顺序和发送顺序一致——只要连接保持健康，从 wire 上读到的第 N 个响应**就是**你发出去的第 N 条语句，不需要额外代价。在真实 TiDB Cloud 集群上实测：

| | 普通方式（prepare + binary，每条语句一次往返） | pipelined binary（本库） |
|---|---|---|
| 事务 P95 | 62.4ms | 31.5ms |
| TiDB CPU | 205% | 195% |

## 这个连接是怎么拿到的

MySQL 的命令包都是自带长度前缀、自分隔的——无论是协议本身，还是 TiDB 的连接读取循环，都不要求命令之间必须有一次往返。之所以用 `database/sql` 通常做不到这一点，是因为主流客户端都没有暴露"先写后读"这种 API。

这个库没有去 fork go-sql-driver/mysql 来绕开这个限制。做法是：通过驱动自己公开的
`mysql.RegisterDialContext` 钩子注册一个自定义拨号函数，这个函数会拨真正的 TCP 连接，同时把它塞进本库读取的一个 channel 里。go-sql-driver 照常在这条连接上完成握手/鉴权；等 `Dial` 返回时，鉴权已经完成，之后本库会接管所有读写，完全绕开 `database/sql`。从这一刻起，底层的
`*sql.Conn`/`*sql.DB` 只是用来占住连接池里的那个槽位——再也不会通过驱动 API 使用它们，因为本库的裸写操作会永久打乱驱动内部按连接维护的包序号记账。

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

- 一条语句是不是会返回结果集，是**自动检测**出来的——从服务端自己的
  `COM_STMT_PREPARE` 响应里拿（它的 column-count 字段，非行返回语句这里是
  `0`）——跟 tidb-multistmt 不一样，调用方不需要自己声明，也就不存在声明错了的问题。
- 一条语句的错误和结果，**只能**通过它的 `Callback`（见下文）拿到——`Execute`
  在整个 batch 跑完之后不会保留任何逐条语句的记录，所以只有一个地方可以看，不存在两条路径。`ExecuteResult`
  本身除了 `AllSucceeded` 什么都不带。
- `Execute` 只在 batch 里每条语句都成功时才会自动发 `COMMIT`。只要有一条失败，它**不会**发
  `ROLLBACK`——失败那条语句的 `Callback` 在失败发生的当下就已经拿到了错误，事务被留在打开状态，由调用方显式处理。这跟
  tidb-multistmt 自己"库不发 ROLLBACK，调用方发"的分工是一致的。
- 连接/协议层的失败（区别于某条语句的 SQL 错误，或者下面说的
  `*CommitError`）会让 `Execute` 返回一个非 nil 的 `error`，并且让 `conn`
  不可再用——`Close` 它，然后重新 `Dial` 一个新的。
- 如果每条语句都成功了，但服务端随后拒绝了 `COMMIT` 本身（已经用真实的写冲突验证过：这种情况下
  TiDB 已经在服务端自动回滚了事务），`Execute` 会返回一个非 nil 的
  `*CommitError`（用 `errors.As` 判断），同时 `ExecuteResult` 的
  `AllSucceeded` 是 false——`conn` 不受影响、照样能用，也没有什么需要
  `Rollback` 的。这是区别于"某条语句失败了"和"连接坏了"的第三种结果，不要跟这两种混在一起处理。
- 只支持悲观事务，这是有意为之的设计，**不是**暂时的缺口。pipeline 中途一旦有语句失败，并不会阻止已经写出去的那些
  `EXECUTE` 继续被服务端执行（每一条对服务端来说都是独立命令，它根本不知道这些是"一个 batch"）——所以行锁必须在每条语句执行时就已经拿到，而不能推迟到
  commit 阶段，"只要失败就整体回滚"才能保持正确。乐观事务则是把冲突检测推迟到
  `COMMIT`（prewrite）阶段：一旦那时候冲突，整个事务会一次性失败，没法把这个失败归因到具体是哪条语句冲突了——这正好废掉了本库赖以构建的"逐条语句
  `Callback`"这个核心机制。不打算支持乐观事务。

### `ExecuteAutoCommit`：完全跳过 `BEGIN`/`COMMIT`

`Execute` 总会比 pipeline 本身多花两次往返：pipeline 之前同步等一次
`BEGIN`，全部成功之后再同步等一次 `COMMIT`。`ExecuteAutoCommit`
用的是同一套 pipeline，但两者都不发——每条语句在自己执行的时候就各自提交，就跟不开显式事务、逐条发送语句一样（MySQL
session 默认的 autocommit 行为）：

```go
res, err := conn.ExecuteAutoCommit(ctx, b)
```

事后没有什么可以回滚的——**`ExecuteAutoCommit` 之后绝对不要调用
`Rollback`**；不管执行到哪一步，已经执行的部分都已经落盘了，成功的也好失败的也好。这种模式下也不会出现
`*CommitError`，因为根本没有 `COMMIT` 会被拒绝。适合只读的 batch，或者那种"部分失败/写入失败也确实不需要撤销"的场景（比如尽力而为的日志写入）。只要需求是"这个
batch 要么整体落地、要么一条都不落地"，就还是得用 `Execute`——拿不准的时候，`Execute` 仍然是更安全的默认选择。

### 连接池（`DB`/`AcquireConn`）

`Dial` 每次调用都给一条专用连接——对短命的小工具够用了，但对那种想把拨号+鉴权的代价只付一次、之后在很多个
batch 之间复用连接的长期运行进程来说就很浪费。`Open`/`AcquireConn` 就是干这个的：

```go
db, err := binarymultistmt.Open("user:pass@tcp(host:4000)/db", 40) // maxConns
if err != nil { ... }
defer db.Close()

conn, err := db.AcquireConn(ctx) // 复用一条空闲连接，不够 maxConns 的话就新拨一条
if err != nil { ... }
defer conn.Close() // 把 conn 还给 db 的空闲池——或者直接丢弃，见下文

b := binarymultistmt.NewBatch()
b.Add(...)
res, err := conn.Execute(ctx, b)
```

这里的 `conn.Close()` 并不会真的关掉 socket：它会把 `conn` 还给 `db`
自己的空闲列表，留给下一次 `AcquireConn` 复用——除非 `conn` 刚好遇到了连接/协议层的失败（跟上面
`Dial` 用法里让你丢弃连接的那几种失败是同一拨），这种情况下 `Close`
会直接销毁它，全靠 `Conn` 自己内部的记账自动判断。不管哪种情况，`Close`
都只调用一次，之后不要再用这个 `conn`——跟 `Dial` 拿到的 `Conn` 的契约是一样的。

这个连接池故意没有用 `database/sql` 自带的那套：本库需要**确切知道**自己接下来要在哪条物理连接上讲裸二进制协议，而
`database/sql` 的连接池没有任何 API 能在把一条空闲连接复用出去的时候提前告诉调用方是哪一条。所以
`DB` 内部那个 `*sql.DB` 只用来拨号、做鉴权；`AcquireConn`/`Close`
在它上面自己实现了一套空闲列表复用逻辑，"这是不是跟上次同一条物理连接"这个问题根本不会出现，因为该把哪个
`Conn` 发出去，完全是本库自己决定的。

如果你的程序里已经有别的代码用常规方式构造了自己的 `*sql.DB`，请把它跟
`binarymultistmt.DB` 并列建立——而不是从它派生：这是两个各自独立拨号的连接池，不是同一个池子的两种视角，`*binarymultistmt.DB`
也不是 `*sql.DB`（不嵌入它，也不能赋值给 `*sql.DB` 类型的参数/字段）。典型的做法是用一个工厂函数从同一个
dsn 里同时返回两者：

```go
func NewPools(dsn string) (plain *sql.DB, binary *binarymultistmt.DB, err error) {
    plain, err = sql.Open("mysql", dsn)
    if err != nil { return nil, nil, err }
    binary, err = binarymultistmt.Open(dsn, 40)
    if err != nil { plain.Close(); return nil, nil, err }
    return plain, binary, nil
}
```

这样代码库里大部分地方继续用 `plain`（原有调用点完全不用改），只有那些真正想用
pipelined binary batch 的代码路径才去用 `binary.AcquireConn`。

### Callback：在语句排队的地方就地处理，并且流式读取结果

`Add` 的第三个参数，只要非 nil，就会被 `Execute` 同步调用恰好一次——在队列里的顺序，就在这条语句的响应刚刚可用的那一刻。这是**唯一**能看到一条语句的错误或结果的办法：`Execute`
在整个 batch 跑完之后不保留任何逐条语句的记录（`ExecuteResult`
除了 `AllSucceeded` 什么都不带），所以只有一个地方能看，不存在两条路径。

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
`*RowIterator`，它会在 callback 调用 `Next()` 的时候直接从 wire
上读取并解码——`Execute` 从来不会把整个结果集缓存进内存。一条用 `nil`
Callback 排进去的返回结果集的语句，它的行照样会被 drain 掉（为了让同一个
batch 里后面语句的响应保持对齐），只是不会被解码或暴露给任何人——所以如果你关心某条返回结果集语句的结果，就必须给它一个
Callback。

这个 callback 也可以提前结束读取（比如读到第一条匹配的行就 `break`）：它没读完的部分会在它返回之后被自动
drain 掉，让同一个 batch 里后面的语句在 wire 上保持正确对齐——跟 tidb-multistmt 的
`Callback`/`Rows` 契约不一样，这里的 callback 从来不要求必须读到 EOF 为止。

一条语句如果在还没产生结果集头部之前就直接失败了（来的是一个 ERR
包而不是结果集头），它的 callback 依然会被恰好调用一次，带着 `sr.Rows == nil`
和设置好的 `sr.Err`——不管一条语句的响应走的是哪条路径，`Execute`
自己的记账逻辑都保证"恰好一次"。

`Next()` 按 `sr.Rows.Columns()`（名称/类型/`Unsigned`/`Decimals`，都是从服务端自己的列元数据解码出来的）解码出每一行，SQL
的 `NULL` 对应 `nil`。各 MySQL 列类型对应的 Go 类型：

| MySQL 类型族 | Go 类型 |
|---|---|
| `TINY`/`SHORT`/`LONG`/`LONGLONG`/`INT24`/`YEAR` | `int64`，如果列是 `UNSIGNED` 则是 `uint64` |
| `FLOAT`/`DOUBLE` | `float64` |
| `DATE`/`DATETIME`/`TIMESTAMP` | `time.Time` |
| `TIME` | `time.Duration`（可以是负数；MySQL 的 `TIME` 并不限定在 24 小时以内） |
| `VARCHAR`/`TEXT`/`BLOB` 系列/`DECIMAL`/`JSON`/`ENUM`/`SET`/`BIT`/`GEOMETRY` | `[]byte`——本库并不清楚一列的字符集，没法判断转成 `string` 是不是安全的，所以把这个判断（以及它的代价）留给调用方 |

### `WHERE id IN (?)` / 批量 `INSERT`

跟 tidb-multistmt 一样的两个函数、一样的调用方式——在 `Batch.Add` 之前调用：

```go
sql, args, err := binarymultistmt.ExpandIn("SELECT c FROM t WHERE id IN (?)", []any{ids})
b.Add(sql, args, nil)

sql, args, err := binarymultistmt.ExpandValues("INSERT INTO t (id, c) VALUES (?, ?)", rows)
b.Add(sql, args, nil)
```

跟 tidb-multistmt 一样，变长的 `IN` 列表会随着列表长度改变渲染出来的 SQL
文本（也就跟着改变了本库内部 PREPARE 缓存用的 key），所以如果某个调用点的列表长度经常变化，从
PREPARE 复用里获得的收益会很有限——如果这对你的负载很重要，自己把长度 pad
成固定的几档桶大小。

## 现状

实验性质，是从 [database_workload](https://github.com/dulao5/database_workload)
里内嵌的一个 benchmark 迁出来的。已经在真实 TiDB（v8.5.8）上验证过：pipeline 的
insert 能正确提交；batch 中间一个重复主键的失败会被准确归因到对应的语句下标，并把事务留在打开状态等调用方
`Rollback`；batch 中间的一条 `SELECT` 不会让它后面的语句错位；每一种支持的参数类型都能正确往返（通过普通驱动连接读回来，确认
TiDB 自己确实理解了编码后的值，而不只是本库自己的编解码自洽）；`ExpandIn`/`ExpandValues`
跟 pipelined-binary 执行路径端到端组合正确；一条覆盖所有支持列类型的
`SELECT`（包括一个 unsigned 的最大值和一个微秒精度的 `DATETIME`）能通过本库自己的
`RowIterator` 正确解码——用的是列元数据和行字节这些 TiDB 真正发出来的东西，不是手搓的 fixture。`ExecuteAutoCommit`
也验证过：同一个 batch 里后面的语句失败，不会回滚前面已经成功的语句，因为本来就没开过事务。`*CommitError`
用一次真实的写冲突验证过（两个 `Conn` 在 TiDB 的乐观事务模式下并发
`UPDATE` 同一行）：输掉冲突那个 `Conn` 的 `Execute` 会返回 `*CommitError`，而且确认这同一个
`Conn` 之后还能正常用来发起另一次 `Execute`——不需要 Close/Dial，也不需要 Rollback。

每一个解析来自 wire 上字节的入口（`readPacket`、`decodeColumnDef`、`decodeBinaryRow`、`drainExecuteResponse`、`readOKorErr`）都配有一个
Go 原生 fuzz target（`fuzz_test.go`）——CI 每次 push 都会短暂跑一下，完整语料库（包括过去发现过的崩溃用例）每次
`go test` 都会作为普通的确定性测试重放一遍。Fuzzing 已经用这种方式发现并修复过一个真实
bug：一个不可信的 column count 被直接当成 slice 容量参数用，在一个恶意或者意外出现的超大值上触发了"cap
out of range"的 panic。

参数绑定全程使用真正的 `COM_STMT_EXECUTE` 二进制协议参数——包括通过
`ExpandIn`/`ExpandValues` 的那些——从来不走字符串字面量拼接，所以（跟
tidb-multistmt 那套文本协议的 `SET`-字面量机制不一样）这里完全不存在基于转义的注入面需要考虑。

**已知限制**（在本仓库里以 issue 的形式跟踪）：

- **暂不支持 TLS。** dial-hook 的劫持是在 go-sql-driver/mysql 握手过程中把
  net.Conn 用 `tls.Client(...)` 包起来**之前**就把它截获了，所以如果 DSN
  要求用 TLS，本库最终会把明文的二进制协议字节写到一条服务端认为应该是加密的连接上——这是硬性的协议破坏。在这个问题修好之前，只在不需要
  TLS 的连接上使用本库（比如私网内部链路）。
- **MySQL 协议压缩（`compress=true`）同样不支持，原因跟 TLS 一样**——dial-hook
  的劫持是在 go-sql-driver/mysql 在握手过程中套上压缩帧**之前**就截获了
  net.Conn，所以本库裸的 `readPacket`/`writePacket`
  会跟一条压缩过的流错位，跟碰到 TLS 流时的道理一样。跟 TLS 一起搁置；在这个问题解决之前不要用
  `compress=true`。
- **不支持 `COM_STMT_SEND_LONG_DATA`。** 每个参数值必须塞进单个包里（`maxPacketPayload`，约
  16MB）——不像 go-sql-driver/mysql 那样有更大值的兜底方案。实际使用中这对普通列值来说是一个相当宽裕的上限；只有真正超大的
  BLOB/TEXT 才会受影响。
- **假定 `CLIENT_DEPRECATE_EOF` 永远不会被协商开启。** 本库解析
  `PREPARE`/`EXECUTE` 响应时，预期在参数定义列表和列定义列表之后都会跟着
  EOF 包，因为它是搭便车用的 go-sql-driver/mysql 握手过程（就是上面说的
  dial-hook 劫持），而这个驱动目前并不会去协商 `CLIENT_DEPRECATE_EOF`——这是看过它源码确认的：这个
  capability 常量定义了，但握手时从来没被置位过。这是跟某个特定驱动版本行为的耦合，不是本库自己独立协商或校验的东西。如果你 vendor
  了一个（现在的或未来的）会开始协商 deprecate-EOF 的 go-sql-driver/mysql
  版本，在依赖本库之前先确认一下这一点——它的失败模式是悄无声息的解析错位，不是一个响亮的报错。

## License

MIT
