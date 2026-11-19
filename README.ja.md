# tidb-binary-multistmt

[English](README.md) | [简体中文](README.zh-CN.md) | 日本語

MySQL の**バイナリプロトコル**（`COM_STMT_EXECUTE`）に基づく、TiDB 向け高性能バッチ／パイプライン実行ライブラリ。**PostgreSQL の Pipeline 機能を参考に設計**されており、トランザクション内の複数ステートメントを **2 回の TCP 往復** で完了させ、TiDB Plan Cache を最大限に活用することで、レイテンシを半減させると同時に CPU 消費を大幅に削減します。

---

## 背景と課題：なぜ作ったのか？

一般的な業務アプリケーションでは、1 つのトランザクション内に複数の SQL ステートメント（例：残高確認、在庫引き当て、注文作成、ログ記録など）が含まれることがよくあります。

### 1. 従来の課題：ネットワーク往復（RTT）による遅延
Go 標準の `database/sql` などを用いる場合、ステートメントは「1 件送信してレスポンスを待つ」という順次実行になります。5 つのステートメントからなるトランザクションでは、`BEGIN` と `COMMIT` を含めて合計 **7 回のネットワーク往復** が発生します。クラウドのアベイラビリティゾーン（AZ）間通信のように RTT が数ミリ秒かかる環境では、トランザクション処理時間の大半がネットワークの往復待ちに費やされてしまいます。

### 2. TiDB ネイティブ Multi-Statements（テキストモード）の限界
往復回数を減らすため、TiDB の **Multi-Statements** 機能を使って複数 SQL をまとめてサーバーへ送信する方法があります：
- **メリット**：複数行の SQL を一括送信した際、途中のステートメントでエラーが発生すると、TiDB は後続の SQL 実行を**即座に中断**します。
- **デメリット**：TiDB の Multi-Statements は**テキストプロトコル限定**（`COM_QUERY`）です：
  1. **生の SQL テキストを送信**：TiDB の **Plan Cache**（実行計画キャッシュ）が有効に機能せず、ステートメントごとに構文解析や実行計画の生成が繰り返されるため、**TiDB の CPU 使用率が跳ね上がります**。
  2. **テキスト PREPARE / EXECUTE を送信**：MySQL テキストプロトコルの仕様上、`EXECUTE` へのパラメータ渡しには**ユーザー変数（`@var`）が必須**となります。そのため、各ステートメントの前に大量の `SET @a=...` 文を挟む必要が生じます。実際の CPU プロファイル調査の結果、この大量の `SET` 文の解析と実行が莫大なオーバーヘッドを生んでいることが判明しました。

### 3. 設計の原点：PostgreSQL の Pipeline 機能を再現する
PostgreSQL のユーザー（`pgx` や `libpq` の利用者）にはお馴染みの **Pipeline モード** は、1 つずつレスポンスを待つことなく、複数の SQL コマンドをソケットへ連続して一括送信できる強力な機能です。

対照的に、一般的な MySQL クライアントライブラリ（Go の `database/sql` など）は Pipeline に対応していません：
- 複数本の SQL を**1 つの通信ストリームにまとめて一括送信できない**；
- そのバッチ内の各ステートメントの**結果やエラーを個別に順次取得することもできない**。

しかしプロトコルの実態に目を向けると：
- **TCP プロトコルは、単一の TCP パケット／ストリーム内に複数の MySQL コマンドを連続して格納・送信することを完全に許容しています。**
- **MySQL バイナリプロトコルもまた、各ステートメントに対する自己完結した独立レスポンス（OK／ERR／ResultSet）を順序通り返却する仕組みを標準で備えています。**

プロトコル自体は元から対応しているにもかかわらず、従来のドライバーがそのインターフェースを提供していなかっただけなのです。当ライブラリはこのプロトコルの潜在能力を解放し、使いやすい Go の API としてパッケージ化することで、TiDB / MySQL 上で PostgreSQL のような Pipeline 実行を可能にしました！

---

## 解決策：バイナリ Multi-Statements (Pipeline)

「低レイテンシ」と「低 CPU」を両立するため、私たちは **`tidb-binary-multistmt`** を開発しました。TiDB ネイティブのテキスト Multi-Statements を完全に排除し、MySQL の**バイナリプロトコル**（`COM_STMT_EXECUTE`）と**ネットワークパイプライン**を採用しています：

1. **Plan Cache を活用して CPU を大幅削減**：バイナリのプリペアドステートメント（`COM_STMT_EXECUTE`）を利用することで、TiDB の実行計画キャッシュを確実にヒットさせ、解析・最適化の CPU コストを削ぎ落とします。
2. **`SET` 文のオーバーヘッドがゼロ**：パラメータはバイナリパケット内に直接型付けしてエンコードされるため、ユーザー変数や不要な `SET` 文が一切不要になります。
3. **わずか 2 回の TCP 往復に圧縮**：`BEGIN` とすべてのバイナリ `EXECUTE` コマンドを**1 つの TCP パケット**にまとめて一括送信し、レスポンスをストリームとして一括受信します。

### トレードオフとその解決策：途中でエラーが起きたら？
- **潜在的な欠点**：全コマンドを同一 TCP ストリームへ一括送信するため、サーバーのソケットバッファにはすでに後続コマンドが届いています。途中のステートメントでエラーが発生しても、**サーバーはすでに届いた後続の SQL をそのまま実行してしまいます**（テキスト Multi-Statements のようにサーバー側で自動中断されません）。
- **私たちの解決策：常に悲観的トランザクション（Pessimistic Transactions）を使用**：
  - 本ライブラリは**悲観的トランザクションのみをサポート**します。悲観的モードでは、ステートメント実行時に行ロックが獲得されます。
  - クライアントはすべてのレスポンスを順次読み取り、どのステートメントが失敗したかを正確に判定します。
  - **コミットまたはロールバックの判断**：
    - すべて成功した場合 ──> クライアントが `COMMIT` を送信。
    - 1 つでも失敗した場合 ──> クライアントが `ROLLBACK` を送信。
  - 後続のステートメントが実行されてしまったとしても、悲観的トランザクション下にあるため、`ROLLBACK` を発行すればすべての変更が綺麗に取り消され、**トランザクションの原子性（All-or-Nothing）が完全に保証**されます。
  - 悲観的トランザクション全体がわずか **2 回の TCP 通信** で完結します！

---

## アーキテクチャと実行フロー (ASCII 図)

### 方式の比較
```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. 従来の順次実行 (N + 2 回の往復): ステートメントが多い = 高レイテンシ     │
│    App ── BEGIN ──> TiDB ──> App ── INSERT ──> TiDB ──> App ── COMMIT ──> TiDB │
│    [RTT 1]                  [RTT 2]                    [RTT 3]              │
├─────────────────────────────────────────────────────────────────────────────┤
│ 2. TiDB ネイティブ Multi-Statements (テキスト): レイテンシ減、CPU 高騰      │
│    App ── "SET @a=1; INSERT ...; SET @b=2; UPDATE ...;" ───────────> TiDB    │
│    - 生の SQL は Plan Cache が効かず、TiDB の解析 CPU コスト大              │
│    - テキスト PREPARE は SET @var が必須で、大量の追加文オーバーヘッドが発生 │
├─────────────────────────────────────────────────────────────────────────────┤
│ 3. 本ライブラリ: バイナリ Pipeline (PG の Pipeline を再現、超低遅延 + CPU)   │
│    App ── [ BEGIN + COM_STMT_EXECUTE(バイナリ引数) + ... ] ────────> TiDB   │
│    - 普通の MySQL 駆動の限界を突破、バッチ送信＆個別結果/エラー取得が可能   │
│    - バイナリ直接転送で SET 不要、TiDB Plan Cache にヒットし CPU を大幅削減 │
│    - 何文あっても悲観的トランザクション全体でわずか 2 回の TCP 往復！       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 悲観的トランザクションの 2 往復フロー
```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ 第 1 回 TCP 往復: BEGIN と全バイナリ EXECUTE を 1 つの TCP 送信にまとめて発行 │
│                                                                             │
│ App ───────────── [ BEGIN + EXECUTE 1 + EXECUTE 2 + EXECUTE 3 ] ──────────> TiDB
│                                                                             │
│                                                               (TiDB が順次実行)
│                                                                             │
│ App <──────────── [ OK(BEGIN) + OK(stmt1) + ERROR(stmt2) + OK(stmt3) ] <── TiDB
│  │                                                                          │
│  │ (クライアントがレスポンスを順次受信し、失敗したステートメントを特定)     │
└──┼──────────────────────────────────────────────────────────────────────────┘
   │
   ├─► [全件成功] ─── (第 2 回 TCP 往復) COMMIT を送信   ───> トランザクションコミット
   │
   └─► [一部失敗] ─── (第 2 回 TCP 往復) ROLLBACK を送信 ───> 安全に全件ロールバック
```

---

## ベンチマーク結果

実際の TiDB Cloud クラスタでの実測値（トランザクション内で複数件の更新・参照を実行）：

| 指标 | 1 文ずつ往復実行 (`database/sql`) | バイナリ Pipeline（本ライブラリ） | 改善効果 |
|---|---|---|---|
| **トランザクション P95 遅延** | 62.4 ms | **31.5 ms** | **約 50% 削減** |
| **TiDB CPU 使用率** | 205% | **195%** | Plan Cache ヒットにより CPU 抑制 |

---

## クイックスタート

### インストール
```bash
go get github.com/dulao5/tidb-binary-multistmt
```

### 基本的な使い方

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

	// 1. コネクションを確立 (本番環境では binarymultistmt.Open のプールを推奨)
	conn, err := binarymultistmt.Dial(ctx, dsn)
	if err != nil {
		log.Fatalf("接続失敗: %v", err)
	}
	defer conn.Close()

	// 2. Batch を作成
	b := binarymultistmt.NewBatch()

	// 書き込み文の追加：コールバックで成否を確認
	b.Add("INSERT INTO accounts (id, balance) VALUES (?, ?)", []any{1, 100}, func(sr *binarymultistmt.StatementResult) {
		if sr.Err != nil {
			log.Printf("ステートメント #%d 失敗: %v", sr.Index, sr.Err)
		}
	})

	// 参照文の追加：Rows イテレータでストリーミング受信
	b.Add("SELECT id, balance FROM accounts WHERE id = ?", []any{1}, func(sr *binarymultistmt.StatementResult) {
		if sr.Err != nil {
			log.Printf("ステートメント #%d 失敗: %v", sr.Index, sr.Err)
			return
		}
		for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
			fmt.Printf("口座 ID=%v, 残高=%v\n", row[0], row[1])
		}
	})

	// 3. 一括実行 (第 1 回 TCP 往復: BEGIN + 全 EXECUTE を送信)
	res, err := conn.Execute(ctx, b)

	// 4. 結果の判定と処理
	var commitErr *binarymultistmt.CommitError
	switch {
	case errors.As(err, &commitErr):
		// 全文成功したが、COMMIT 自体がサーバーに拒否された（書き込み競合など）
		// TiDB サーバー側ですでに自動ロールバック済み。conn は健全なまま再利用可能
		log.Printf("コミット拒否（自動ロールバック済み）: %v", commitErr)

	case err != nil:
		// ネットワーク切断またはプロトコルレベルのエラー：conn は再利用不可のため閉じる
		conn.Close()
		log.Fatalf("接続/プロトコルエラー: %v", err)

	case !res.AllSucceeded:
		// 失敗したステートメントが存在する：Execute は自動ロールバックを行わない
		// ROLLBACK を明示的に送信 (第 2 回 TCP 往復) し、安全に変更を破棄
		log.Println("バッチ内に失敗した文があります。ロールバック中...")
		if rbErr := conn.Rollback(ctx); rbErr != nil {
			log.Printf("ロールバック失敗: %v", rbErr)
		}

	default:
		// 全ステートメント成功：Execute が自動的に COMMIT を送信済み (第 2 回 TCP 往復)
		log.Println("トランザクションが正常にコミットされました！")
	}
}
```

---

## 主な機能

### 1. コネクションプール (`Open` / `AcquireConn`)
常駐サービスでは、毎回 Dial せずにコネクションプールを利用します：

```go
db, err := binarymultistmt.Open(dsn, 40) // maxConns
if err != nil { ... }
defer db.Close()

conn, err := db.AcquireConn(ctx)
if err != nil { ... }
defer conn.Close() // アイドルプールへ返却（破損時は自動破棄）

res, err := conn.Execute(ctx, b)
```

### 2. プリペアドステートメントキャッシュ (Plan Cache)
- コネクション上で初めて実行される SQL は自動的に `COM_STMT_PREPARE` され、LRU キャッシュに保持されます。
- 以降の同一 SQL は statement ID を再利用し、TiDB の Plan Cache にヒットします。
- デフォルト上限は 90 件（`conn.SetStmtCacheLimit(n)` で変更可能）。あふれたエントリは自動的に `COM_STMT_CLOSE` され、サーバー側のメモリ肥大化を防ぎます。

### 3. クエリ結果のストリーミング読み取り（メモリ圧迫ゼロ）
`SELECT` 文の `sr.Rows` は、ネットワークソケットから行データを直接読み出します：
- 大きな結果セットをメモリに一括バッファリングしません。
- 途中で `break` してループを抜けた場合でも、未読の行は自動的に破棄（Drain）され、プロトコルの同期を保ちます。

### 4. 補助関数：動的 `IN (?)` と一括 `INSERT`
プレースホルダー数を動的に展開するヘルパー関数を提供しています：

```go
// IN (?, ?, ...) の動的展開
sql, args, err := binarymultistmt.ExpandIn("SELECT * FROM t WHERE id IN (?)", []any{ids})
b.Add(sql, args, nil)

// VALUES (?, ?), (?, ?) の一括展開
sql, args, err := binarymultistmt.ExpandValues("INSERT INTO t (id, name) VALUES (?, ?)", rows)
b.Add(sql, args, nil)
```

### 5. `ExecuteAutoCommit`：トランザクション不要時の超高速実行
バッチがすべて**参照専用クエリ**の場合や、部分的な成功が許容されるログ書き込みなどの場合：
- `BEGIN` と `COMMIT` を送信せず、autocommit モードで実行します。
- バッチ全体がわずか **1 回の TCP 往復** で完了します！

```go
res, err := conn.ExecuteAutoCommit(ctx, b)
```

---

## 注意事項と制限事項

1. **悲観的トランザクションのみ対応**：楽観的トランザクションは競合検知をコミット時まで遅延させるため、個別のステートメント単位でエラーを特定できず、サポートしていません。
2. **TLS およびプロトコル圧縮は未対応**：接続ハイジャックがハンドシェイク直後に行われるため、TLS 暗号化ストリームや圧縮フレームに対応していません。VPC 内部などの安全なプライベートネットワークでご使用ください。
3. **単一パラメータのサイズ上限**：`COM_STMT_SEND_LONG_DATA` は未実装です。1 パラメータの上限は MySQL パケットサイズ（約 16MB）までとなります。
4. **ステートメント結果は Callback 内でのみ参照可能**：ゼロコピーと省メモリを実現するため、`Execute` 完了後に結果は保持されません。必要なデータやエラーは各ステートメントのコールバック内で処理してください。

---

## ライセンス

[MIT License](LICENSE)
