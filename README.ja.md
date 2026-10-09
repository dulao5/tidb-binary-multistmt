# tidb-binary-multistmt

[English](README.md) | [简体中文](README.zh-CN.md) | 日本語

1つのトランザクション内の複数ステートメントを、MySQL の**バイナリプロトコル**（`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`）でレスポンスを待たずに連続送信するパッケージです。各ステートメントの `EXECUTE` パケットをまとめて書き込んでから、まとめてレスポンスを読む方式を採用しており、`database/sql`（および
[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)）が通常採用している「1コマンド書き込み→同期的にそのレスポンスを読む」という方式とは異なります。

## なぜ作ったか

[tidb-multistmt](https://github.com/dulao5/tidb-multistmt) はすでに**テキストプロトコル**を使って「N個のステートメントを1往復で」を実現しています。全ステートメントをセミコロンで連結した1つの
`COM_QUERY` ブロブ（`CLIENT_MULTI_STATEMENTS`）にまとめ、各ステートメントの前に
`SET @_multistmt_statement_num=N` というマーカーを挿入することで、本来であれば成否の情報がつぶれてしまうレスポンスストリームから、クライアント側でステートメントごとの成功/失敗を復元できるようにしています。

実際の本番環境での CPU プロファイル比較により、multi-statement
モードが単純な「1ステートメントにつき1往復」よりも多くの CPU
を消費する主な原因は、このマーカー用の `SET`
文であることが判明しました――テキストかバイナリかというプロトコルの選択自体が原因ではありません。本パッケージは代わりにバイナリ
`EXECUTE` のパイプライン方式を採用しています：マーカーは一切不要です。なぜなら各
`EXECUTE` はもともと独立したコマンドであり、それぞれ自身のレスポンスを順序どおりに返すからです――接続さえ健全であれば、ワイヤから読み取った
N 番目のレスポンスは、そのまま送信した N
番目のステートメントのレスポンスになります。実際の TiDB Cloud クラスタで計測した結果：

| | 通常方式（prepare + binary、ステートメントごとに1往復） | pipelined binary（本パッケージ） |
|---|---|---|
| トランザクション P95 | 62.4ms | 31.5ms |
| TiDB CPU | 205% | 195% |

## コネクションの取得方法

MySQL のコマンドパケットは自己完結的（長さプレフィックス付き）です――プロトコル自体にも、TiDB
の接続読み取りループにも、コマンド間で往復通信を要求する仕様はありません。これが
`database/sql` では通常実現できない理由は、主要なクライアントのどれもが「読む前に書く」ための
API を公開していないからです。

本パッケージはこれを回避するために go-sql-driver/mysql
をフォークしてはいません。ドライバ自身が公開している
`mysql.RegisterDialContext` フックを通じてカスタムダイアル関数を登録し、この関数が実際の
TCP 接続を確立すると同時に、本パッケージが読み取るチャネルにその接続を渡します。go-sql-driver
はその接続上で通常どおりハンドシェイク／認証を行います。`Dial`
が返った時点で認証は完了しており、以降は本パッケージがすべての読み書きを直接引き継ぎ、`database/sql`
の外側で動作します。その時点から、内部の `*sql.Conn`/`*sql.DB`
ペアはコネクションプールの枠を確保しておくためだけに開かれたままになります――ドライバ
API 経由で再び使われることは二度とありません。本パッケージの生の書き込みが、ドライバが接続ごとに内部で管理しているパケットのシーケンス番号を恒久的にずらしてしまうためです。

## 使い方

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
    // 全ステートメントは成功したが、COMMIT 自体がサーバーに拒否された
    // （例：write conflict）場合です。TiDB はすでにサーバー側でロール
    // バック済みなので、conn は健全なままで Rollback すべきものは何も
    // ありません。バッチ全体をリトライするかどうかをここで判断します。
    log.Println("commit rejected, already rolled back:", commitErr)
case err != nil:
    // コネクション／プロトコルレベルの失敗——conn はもう使えないので Close する
    conn.Close()
    return err
case !res.AllSucceeded:
    for _, f := range failed {
        log.Println("failed:", f)
    }
    // Execute は ROLLBACK を送っていません——トランザクションは conn 上で
    // 開いたままです。どうするか（ロールバック、さらなる調査、リトライ）
    // を判断し、明示的に実行してください：
    if err := conn.Rollback(ctx); err != nil { ... }
default:
    // 全ステートメントが成功した——Execute がすでに COMMIT を送信済み。
}
```

- あるステートメントが行を返すかどうかは**自動判定**されます。サーバー自身の
  `COM_STMT_PREPARE` レスポンス（その column-count
  フィールドは、行を返さないステートメントでは `0` になります）から判定するため、tidb-multistmt
  とは異なり、呼び出し側がこれを宣言する必要はなく、間違えようもありません。
- あるステートメントのエラーと結果は、その `Callback`（後述）を通じてしか見ることができません――`Execute`
  はバッチ終了後にステートメントごとの記録を一切保持しないため、確認できる場所は1つだけで、2通りの見方が存在することはありません。`ExecuteResult`
  自体も `AllSucceeded` 以外は何も持ちません。
- `Execute` はバッチ内の全ステートメントが成功した場合に限り自動的に
  `COMMIT` を送信します。1つでも失敗した場合、`Execute` は
  `ROLLBACK` を**送信しません**――失敗したステートメントの `Callback`
  はその場でエラーをすでに受け取っており、トランザクションは開いたままにされ、呼び出し側が明示的に対処することになります。これは
  tidb-multistmt 自身の「ライブラリは ROLLBACK
  を送らず、呼び出し側が送る」という役割分担と同じです。
- コネクション／プロトコルレベルの失敗（1つのステートメントの SQL
  エラーや、後述の `*CommitError` とは異なるもの）が発生すると、`Execute`
  は非 nil の `error` を返し、`conn` は使用不能になります――`Close`
  してから新たに `Dial` し直してください。
- 全ステートメントが成功したにもかかわらず、サーバーが `COMMIT`
  自体を拒否した場合（実際の write conflict で検証済み：この場合 TiDB
  はすでにサーバー側でトランザクションをロールバック済みです）、`Execute`
  は非 nil の `*CommitError`（`errors.As` で判定可能）を返し、同時に
  `ExecuteResult` の `AllSucceeded` は false になります――`conn`
  は影響を受けず使用可能なままで、`Rollback` すべきものもありません。これは「あるステートメントが失敗した」場合や「コネクションが壊れた」場合とは異なる、第三の結果です――どちらとも混同しないでください。
- 悲観的トランザクションのみをサポートしています。これは設計上の決定であり、一時的な制限では**ありません**。パイプラインの途中でステートメントが失敗しても、すでに書き込み済みの
  `EXECUTE`
  の実行は止まりません（サーバーにとってはそれぞれが独立したコマンドであり、それらが「1つのバッチ」であることを知る由もないためです）。したがって「失敗したら全体をロールバックする」という挙動を正しく保つには、各ステートメント実行時点ですでに行ロックが取得されていなければならず、コミット時点まで遅延させることはできません。一方、楽観的トランザクションは競合検出を
  `COMMIT`（prewrite）の時点まで遅延させます――その時点で競合が起きると、トランザクション全体が一度に失敗してしまい、実際にどのステートメントが衝突したのかを特定する手段がありません。これは本パッケージの中核をなす「ステートメントごとの
  `Callback`」という仕組みを根本から無効化してしまいます。楽観的トランザクションのサポート予定はありません。

### `ExecuteAutoCommit`：`BEGIN`/`COMMIT` を完全に省略する

`Execute` はパイプライン化された `EXECUTE`
群に加えて、常に2回分の余計な往復のコストがかかります：前段の同期的な
`BEGIN` 1回と、全成功時の後段の同期的な `COMMIT` 1回です。`ExecuteAutoCommit`
は同じパイプライン処理を行いますが、どちらも送信しません――各ステートメントは実行される端から自分自身でコミットされます。これは明示的なトランザクションを開かずに1件ずつステートメントを発行する場合（MySQL
のセッションデフォルトである autocommit の挙動）とまったく同じです：

```go
res, err := conn.ExecuteAutoCommit(ctx, b)
```

実行後にロールバックすべきものは何もありません――**`ExecuteAutoCommit` の後に
`Rollback` を呼び出しては絶対にいけません**。すでに実行された部分は、成功・失敗を問わずすでに永続化されています。ここでは
`*CommitError` も発生し得ません。拒否されるべき `COMMIT`
がそもそも存在しないためです。読み取り専用のバッチや、書き込みの一部が失敗してもロールバックする必要が本当にないケース（例：ベストエフォートのロギング）に適しています。「このバッチ内の全ステートメントが反映されるか、まったく反映されないか」が必要な場合は
`Execute` を使う必要があります――迷った場合はいまも `Execute`
がデフォルトとして正しい選択です。

### コネクションプール（`DB`/`AcquireConn`）

`Dial` は呼び出すたびに専用のコネクションを1本払い出します――短命なツールには十分ですが、ダイアル＋認証のコストを一度だけ払って、多数のバッチ間でコネクションを使い回したい長時間稼働するプロセスにとっては無駄があります。`Open`/`AcquireConn`
はそのために用意されています：

```go
db, err := binarymultistmt.Open("user:pass@tcp(host:4000)/db", 40) // maxConns
if err != nil { ... }
defer db.Close()

conn, err := db.AcquireConn(ctx) // アイドル中の1本を再利用するか、maxConns 未満なら新規にダイアルする
if err != nil { ... }
defer conn.Close() // conn を db のアイドルプールに返却する――あるいは破棄する。後述。

b := binarymultistmt.NewBatch()
b.Add(...)
res, err := conn.Execute(ctx, b)
```

ここでの `conn.Close()` はソケットを実際には閉じません。`conn` を
`db` 自身のアイドルリストに返却し、次回の `AcquireConn`
呼び出しで再利用できるようにします――ただし `conn`
がコネクション／プロトコルレベルの失敗に遭遇していた場合は例外です（これは上記の
`Dial` の使い方の説明で、コネクションを破棄すべきとされているのと同じ種類の失敗です）。その場合
`Close` は代わりにそれを破棄します。これは `Conn`
自身の内部的な状態管理に基づいて自動的に判断されます。いずれにせよ、`Close`
はちょうど1回だけ呼び出し、その後は `conn` を使わないでください――`Dial`
で得た `Conn` と同じ契約です。

このプールは意図的に `database/sql`
自身のコネクションプールを使っていません。本パッケージは、これから生のバイナリプロトコルで通信しようとしている物理コネクションが**どれであるかを確実に知る**必要がありますが、`database/sql`
のプールには、アイドルリストから再利用しようとしているコネクションがどれであるかを事前に呼び出し側へ伝える
API がありません。そのため `DB` 内部の `*sql.DB`
は新しい物理コネクションのダイアルと認証にのみ使われます。`AcquireConn`/`Close`
はその上に独自のアイドルリスト再利用ロジックを実装しており、「これは前回と同じ物理コネクションか」という問いは、どの
`Conn` を払い出すかを本パッケージ自身が決めているため、そもそも問題になりません。

すでにあなたのプログラムに、通常の方法で構築された別の `*sql.DB`
を使うコードがある場合は、それを `binarymultistmt.DB`
から派生させるのではなく、並べて構築してください。両者は別々に独立してダイアルされる、2つのコネクションプールであり、1つのプールに対する2通りの見方ではありません。また
`*binarymultistmt.DB` は `*sql.DB` ではありません（それを embed
してもいませんし、`*sql.DB` 型のパラメータ／フィールドに代入することもできません）。典型的な構成は、1つの
dsn から両方を返すファクトリ関数を用意することです：

```go
func NewPools(dsn string) (plain *sql.DB, binary *binarymultistmt.DB, err error) {
    plain, err = sql.Open("mysql", dsn)
    if err != nil { return nil, nil, err }
    binary, err = binarymultistmt.Open(dsn, 40)
    if err != nil { plain.Close(); return nil, nil, err }
    return plain, binary, nil
}
```

こうすることで、コードベースの大部分は引き続き `plain`
を使い続けられ（既存の呼び出し箇所は一切変更不要）、パイプライン化されたバイナリバッチを使いたい特定のコードパスだけが
`binary.AcquireConn` を使うようになります。

### Callback：キューに積んだその場でステートメントを処理し、行をストリーミングで読む

`Add` の第3引数は、非 nil であれば `Execute`
によって同期的に、キューの順序どおりに、ちょうど1回だけ呼び出されます――そのステートメントのレスポンスが利用可能になった、まさにその場所で。これは、あるステートメントのエラーや結果を確認できる**唯一**の方法です――`Execute`
はバッチ終了後にステートメントごとの記録を一切保持しないため（`ExecuteResult`
は `AllSucceeded` 以外何も持ちません）、確認できる場所は1つだけで、2通りの見方が存在することはありません。

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

行を返すステートメントの場合（`sr.HasResultSet`）、`sr.Rows` は
`*RowIterator` であり、callback が `Next()`
を呼び出すたびにワイヤから直接読み取って行をデコードします――この場合
`Execute` は結果セットをメモリ上にバッファリングすることが一切ありません。`nil`
の Callback とともにキューに積まれた、行を返すステートメントについても、その行はちゃんと
drain されます（同じバッチ内の後続ステートメントとの整合性を保つため）が、それらはデコードされることも誰かに公開されることもありません――したがって、ある行返却ステートメントの結果に関心がある場合は、それに
Callback を付ける必要があります。

この callback は途中で読み取りを打ち切ることもできます（例えば最初に一致した行だけ読んで
`break` するなど）：それが読み残した分は、callback
が戻った後に自動的に drain され、同じバッチ内の後続ステートメントがワイヤ上で正しく整合性を保てるようにします――tidb-multistmt
の `Callback`/`Rows` の契約とは異なり、この callback は EOF
まで読み切ることを一切要求されません。

あるステートメントが、結果セットのヘッダーが生成される前に失敗した場合（結果セットヘッダーの代わりに
ERR パケットが来た場合）でも、その callback は `sr.Rows == nil`
とセットされた `sr.Err` を伴って、やはりちょうど1回だけ呼び出されます――ステートメントのレスポンスがどちらの経路をたどったかにかかわらず、`Execute`
自身の管理ロジックが「ちょうど1回」であることを保証します。

`Next()` は `sr.Rows.Columns()`（名前／型／`Unsigned`/`Decimals`。いずれもサーバー自身のカラムメタデータからデコードされます）の各カラムに対応する1行をデコードします。SQL
の `NULL` は `nil` に対応します。MySQL の各カラム型に対応する Go の型：

| MySQL 型ファミリー | Go の型 |
|---|---|
| `TINY`/`SHORT`/`LONG`/`LONGLONG`/`INT24`/`YEAR` | `int64`。カラムが `UNSIGNED` の場合は `uint64` |
| `FLOAT`/`DOUBLE` | `float64` |
| `DATE`/`DATETIME`/`TIMESTAMP` | `time.Time` |
| `TIME` | `time.Duration`（負の値もありえます。MySQL の `TIME` は24時間以内に収まるとは限りません） |
| `VARCHAR`/`TEXT`/`BLOB` 系／`DECIMAL`/`JSON`/`ENUM`/`SET`/`BIT`/`GEOMETRY` | `[]byte`――本パッケージはカラムの文字セットを把握していないため、`string` への変換が安全かどうかを判断できません。そのため、その判断（および変換コスト）は呼び出し側に委ねています |

### `WHERE id IN (?)` ／ バルク `INSERT`

tidb-multistmt と同じ2つの関数、同じ呼び出し方です――`Batch.Add` より前に呼び出します：

```go
sql, args, err := binarymultistmt.ExpandIn("SELECT c FROM t WHERE id IN (?)", []any{ids})
b.Add(sql, args, nil)

sql, args, err := binarymultistmt.ExpandValues("INSERT INTO t (id, c) VALUES (?, ?)", rows)
b.Add(sql, args, nil)
```

tidb-multistmt と同様、可変長の `IN`
リストはレンダリングされる SQL テキスト（したがって本パッケージ内部の
PREPARE キャッシュのキー）をリストの長さに応じて変化させます。そのためリストの長さが頻繁に変動する呼び出し箇所では、PREPARE
の再利用による恩恵はあまり得られません――もしそれがあなたのワークロードにとって重要であれば、あらかじめ固定サイズのいくつかのバケットにパディングしてください。

## ステータス

実験的な段階です。[database_workload](https://github.com/dulao5/database_workload)
に組み込まれていたベンチマークから切り出されたものです。実際の TiDB（v8.5.8）に対して検証済みです：パイプライン化された
insert は正しくコミットされる、バッチ途中の重複キーによる失敗が正しいステートメントのインデックスに帰属し、トランザクションは呼び出し側の
`Rollback` のために開いたままにされる、バッチ途中の `SELECT`
がそれ以降のステートメントをデシンクさせない、サポートされているすべてのパラメータ型が正しく往復する（通常のドライバコネクション経由で読み戻し、本パッケージ自身のエンコード／デコードの自己整合性だけでなく、TiDB
自身が実際にエンコードされた値を理解していることを確認済み）、`ExpandIn`/`ExpandValues`
が pipelined-binary の実行パスとエンドツーエンドで正しく組み合わさる、サポートされているすべてのカラム型（符号なしの最大値やマイクロ秒精度の
`DATETIME` を含む）をカバーする `SELECT` が本パッケージ自身の
`RowIterator` を通じて正しくデコードされる――手作りのフィクスチャではなく、TiDB
が実際に送ってくるカラムメタデータと行バイトを使って検証済みです。`ExecuteAutoCommit`
も検証済みです：同じバッチ内の後続ステートメントの失敗は、前段のステートメントをロールバックしません。そもそもトランザクションが一度も開かれていないためです。`*CommitError`
は実際の write conflict（TiDB
の楽観的トランザクションモード下で2つの `Conn` が同じ行への `UPDATE`
を競合させる）で検証済みです：競合に負けた方の `Conn` の `Execute` は
`*CommitError` を返し、さらにその同じ `Conn`
が別の `Execute` 呼び出しに対して引き続き正常に使用できることも確認済みです――Close/Dial
も Rollback も不要です。

ワイヤから来るバイト列を解析するすべてのエントリーポイント（`readPacket`、`decodeColumnDef`、`decodeBinaryRow`、`drainExecuteResponse`、`readOKorErr`）には
Go ネイティブの fuzz ターゲット（`fuzz_test.go`）が用意されています――CI
は push のたびに短時間実行し、完全なコーパス（過去に見つかったクラッシュケースを含む）は
`go test` のたびに通常の決定的なテストとして再生されます。Fuzzing
はすでにこの方法で実際のバグを1つ発見・修正しています：信頼できないカラム数がそのままスライスの容量引数として使われており、悪意のある、あるいは偶発的な巨大な値に対して
"cap out of range" で panic していました。

パラメータバインディングは一貫して本物の `COM_STMT_EXECUTE`
バイナリプロトコルのパラメータを使用します――`ExpandIn`/`ExpandValues`
経由のものも含め、文字列リテラルによる置換は一切行いません。そのため（tidb-multistmt
のテキストプロトコルによる `SET`
リテラル方式とは異なり）ここではエスケープに起因するインジェクションの懸念はそもそも存在しません。

**既知の制限事項**（本リポジトリの issue として追跡しています）：

- **TLS は現時点で未対応です。** dial-hook
  によるハイジャックは、go-sql-driver/mysql
  がハンドシェイク中に `tls.Client(...)` で net.Conn
  をラップする**前**に横取りしてしまいます。そのため DSN が TLS
  を要求している場合、本パッケージは最終的に平文のバイナリプロトコルのバイト列を、サーバー側が暗号化されていると見なしているコネクションに書き込んでしまいます――これはプロトコルレベルの重大な破壊です。この問題が修正されるまでは、TLS
  を必要としないコネクション（例：プライベートネットワーク内のリンク）に対してのみ本パッケージを使用してください。
- **MySQL プロトコル圧縮（`compress=true`）も同様の理由で未対応です。**
  dial-hook によるハイジャックは、go-sql-driver/mysql
  がハンドシェイク中に圧縮フレーミングを適用する**前**に net.Conn
  を横取りしてしまうため、本パッケージの生の
  `readPacket`/`writePacket` は、TLS の場合とまったく同じ理屈で、圧縮済みのストリームに対してずれを起こします。TLS
  と合わせて保留中です。この問題が解決されるまでは `compress=true`
  を使わないでください。
- **`COM_STMT_SEND_LONG_DATA` は未対応です。**
  各パラメータ値は単一のパケット内（`maxPacketPayload`、約16MB）に収まる必要があります――go-sql-driver/mysql
  にあるような、より大きな値に対するフォールバックはありません。実運用上、通常のカラム値に対してはかなり余裕のある上限ですが、本当に巨大な
  BLOB/TEXT を扱う場合にのみ問題になります。
- **`CLIENT_DEPRECATE_EOF` が交渉されないことを前提としています。**
  本パッケージの `PREPARE`/`EXECUTE`
  レスポンスの解析は、パラメータ定義リストおよびカラム定義リストの後に
  EOF パケットが続くことを前提としています。これは本パッケージが（上記の
  dial-hook によるハイジャックを通じて）go-sql-driver/mysql
  のハンドシェイクに便乗しており、かつ現時点でこのドライバが
  `CLIENT_DEPRECATE_EOF` を交渉しないためです――これはソースコードを読んで確認済みです：当該
  capability の定数は定義されているものの、ハンドシェイク時に一度もセットされていません。これは本パッケージが独自に交渉・検証しているものではなく、特定のドライババージョンの挙動への結合です。もし
  deprecate-EOF
  の交渉を開始する（現在または将来の）別バージョンの go-sql-driver/mysql
  を vendor している場合は、本パッケージに依存する前にこの点を確認してください――その場合の失敗モードは、派手なエラーではなく、静かなパース結果のずれです。

## License

MIT
