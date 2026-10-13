# flowkit

flowkit là workflow engine nhúng vào ứng dụng Go, lưu trạng thái trên Postgres. Ứng dụng đăng ký event, action và fact bằng code; workflow (definition) là JSON với biểu thức CEL, được validate và publish lúc chạy mà không cần deploy lại. Ứng dụng phát event trong chính transaction của nó; flowkit định tuyến event tới run và thực thi action theo kiểu claim-then-execute. Job nền chạy bằng [River](https://riverqueue.com) trong một schema Postgres riêng.

Phù hợp với các luồng nghiệp vụ do event kích hoạt và kéo dài theo giờ hoặc ngày: onboarding, nhắc việc, fulfillment đơn hàng.

## Yêu cầu

- Go 1.26 trở lên.
- PostgreSQL (bản được kiểm thử: 16).
- Mọi module `github.com/riverqueue/river/...` phải ở `v0.49.x`. `flowkit.New` từ chối khởi động nếu một dependency kéo River ra ngoài minor này (xem [runbook-migrate.md](docs/runbook-migrate.md#yêu-cầu-phiên-bản)).

## Cài đặt

```sh
go get github.com/g0-up/flowkit
go install github.com/g0-up/flowkit/cmd/flowkit@latest   # CLI: migrate, validate
```

## Quickstart

Ví dụ dưới đây gửi email chào mừng khi người dùng đăng ký, chờ họ kích hoạt tài khoản tối đa 3 ngày, quá hạn thì gửi email nhắc.

### 1. Migrate schema

`flowkit.New` không bao giờ tự migrate. Chạy CLI trước (hoặc `migrate.Run` từ code):

```sh
export FLOWKIT_DATABASE_URL='postgres://user@localhost:5432/app'   # hoặc PGHOST, PGUSER, PGPASSWORD...
flowkit migrate --schema flowkit
```

Schema đã có dữ liệu thì sao lưu (`pg_dump -n flowkit`) rồi chạy lại với `--yes`. `--dsn` không được chứa mật khẩu.

### 2. Đăng ký event và action

Registry là phần code của ứng dụng: event nào có thể phát, action nào workflow được gọi.

```go
func newRegistry() *flowkit.Registry {
	reg := flowkit.NewRegistry()

	reg.MustAddEvent(flowkit.EventDef{
		Type: "user.signed_up",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"required": ["user_id", "email"],
			"properties": {"user_id": {"type": "string"}, "email": {"type": "string"}}
		}`),
		EntityKind:    "user",
		EntityIDField: "user_id",
		DedupKeyField: "user_id",
	})
	reg.MustAddEvent(flowkit.EventDef{
		Type:          "user.activated",
		PayloadSchema: json.RawMessage(`{"type": "object", "required": ["user_id"], "properties": {"user_id": {"type": "string"}}}`),
		EntityKind:    "user",
		EntityIDField: "user_id",
	})

	reg.MustAddAction(flowkit.ActionDef{
		Name: "send_email",
		ParamsSchema: json.RawMessage(`{
			"type": "object",
			"required": ["to", "template"],
			"properties": {"to": {"type": "string"}, "template": {"type": "string"}}
		}`),
		Idempotent: true, // the mail provider deduplicates by IdempotencyKey
		Execute: func(ctx context.Context, in flowkit.ActionInput) (flowkit.ActionResult, error) {
			slog.InfoContext(ctx, "send email", "to", in.Params["to"], "template", in.Params["template"], "key", in.IdempotencyKey)
			return flowkit.ActionResult{}, nil
		},
	})
	return reg
}
```

Ngoài event và action, `MustAddFact` đăng ký fact: giá trị đọc từ bảng domain (trong transaction chỉ đọc) để definition rẽ nhánh, ví dụ `facts.customer.revenue_90d > 1000`.

### 3. Viết definition

```go
var onboarding = json.RawMessage(`{
  "metamodel_version": 1,
  "trigger": {"event": "user.signed_up"},
  "steps": [
    {"id": "welcome", "type": "action", "action": "send_email",
     "params": {"to": {"$expr": "event.email"}, "template": "welcome"},
     "next": "activation"},
    {"id": "activation", "type": "wait_for_event", "event": "user.activated",
     "match": "event.user_id == scope.entity_id",
     "timeout": "3d", "on_timeout": "reminder", "next": "activated"},
    {"id": "reminder", "type": "action", "action": "send_email",
     "params": {"to": {"$expr": "event.email"}, "template": "activation_reminder"},
     "next": "reminded"},
    {"id": "activated", "type": "end", "name": "activated"},
    {"id": "reminded", "type": "end", "name": "reminded"}
  ]
}`)
```

Step đầu tiên là điểm vào. Metamodel v1 có bốn loại step: `action`, `wait_for_event`, `switch`, `end`. Mặc định mọi trigger đều tạo run (`concurrent`); khối `concurrency` chọn `serial`, `cancel_previous` hoặc `dedup_window` theo một `serial_key`. Biểu thức, fact, `has()` và giới hạn: [definition-authoring.md](docs/definition-authoring.md).

### 4. Chạy Engine, publish và phát event

```go
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	eng, err := flowkit.New(flowkit.Config{Pool: pool}, newRegistry())
	if err != nil {
		log.Fatal(err) // errors.Is(err, flowkit.ErrSchemaDrift): run `flowkit migrate` first
	}
	if err := eng.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = eng.Close(closeCtx)
	}()

	// Usually done from an admin tool, not on every start: each Publish
	// creates a new version.
	defs := eng.Definitions()
	draft, err := defs.SaveDraft(ctx, "onboarding", onboarding)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := defs.Publish(ctx, draft, flowkit.PublishOpts{Actor: "quickstart"}); err != nil {
		log.Fatal(err)
	}

	// The event commits or rolls back together with the domain write.
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)
	// ... INSERT INTO users ... on the same tx ...
	err = eng.Emit(ctx, tx, flowkit.Event{
		Type:    "user.signed_up",
		Payload: map[string]any{"user_id": "u-1", "email": "u-1@example.com"},
	})
	if err != nil && !errors.Is(err, flowkit.ErrDuplicateEvent) {
		log.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
}
```

Sau khi commit, worker định tuyến event, chạy `welcome` rồi run chuyển sang `waiting` ở step `activation`. Một event `user.activated` cùng `user_id` sẽ kết thúc run ở `activated`; không có thì sau 3 ngày run gửi `reminder` và kết thúc ở `reminded`.

### 5. Xem run

```go
runs, err := eng.Runs().List(ctx, flowkit.RunFilter{DefName: "onboarding"})
trace, err := eng.Runs().Trace(ctx, runs[0].RunID) // every step the run took
```

`Runs().Cancel` hủy một run; `Runs().ResolveDeadLetter` xử lý dead letter; `Definitions().Disable` / `Enable` là công tắc tắt khẩn cấp của một definition.

## Test workflow

Gói `flowkittest` có hai cách chạy definition trong test, dùng chung matcher:

- `NewSim`: chạy trong bộ nhớ, không cần database; action được thay bằng stub.
- `NewPGHarness`: Engine thật trên Postgres trong schema tạm (cần `FLOWKIT_TEST_DATABASE_URL`).

Cả hai dùng đồng hồ ảo, nên `Advance(72 * time.Hour)` vượt qua bước chờ 3 ngày ngay lập tức:

```go
func TestOnboardingReminder(t *testing.T) {
	sim := flowkittest.NewSim(t, newRegistry())
	sim.StubAction("send_email", func(context.Context, flowkit.ActionInput) (flowkit.ActionResult, error) {
		return flowkit.ActionResult{}, nil
	})
	sim.Publish("onboarding", onboarding)
	sim.Emit(flowkit.Event{Type: "user.signed_up", Payload: map[string]any{"user_id": "u-1", "email": "u-1@example.com"}})
	sim.Advance(0)              // welcome email, then the wait starts
	sim.Advance(72 * time.Hour) // no activation: the wait times out

	runs, err := sim.Runs().List(context.Background(), flowkit.RunFilter{DefName: "onboarding"})
	if err != nil || len(runs) != 1 {
		t.Fatalf("List = %v, %v", runs, err)
	}
	flowkittest.AssertTrace(t, sim.Runs(), runs[0].RunID,
		flowkittest.StepExecuted("welcome"),
		flowkittest.StepExecuted("reminder"),
		flowkittest.RunTerminal("reminded"),
	)
}
```

Validate definition trong CI bằng registry thật, không cần database:

```go
err := newRegistry().ValidateDefinition(body) // flowkit.ValidationErrors, each with a JSON Pointer Path
```

`flowkit validate <file>` chỉ kiểm tra cấu trúc (step, chuyển tiếp, cú pháp CEL), hợp với pre-commit trên repo chỉ chứa definition; nó không thay được `ValidateDefinition`.

## Trước khi lên production

- **Action không idempotent:** lỗi không được bọc `flowkit.NotPerformed(err)` bị coi là "in doubt" và vào dead letter thay vì được retry. Truyền `ActionInput.IdempotencyKey` cho hệ thống bên ngoài khi có thể.
- **Deploy trước, publish sau:** deploy code có event, action, fact mới lên mọi instance rồi mới publish definition dùng chúng. Instance thiếu component sẽ làm run thất bại vĩnh viễn.
- **Janitor:** ít nhất một instance đang chạy phải có `RoleJanitor` (mặc định có), nếu không timeout và pump hẹn giờ không bao giờ tới hạn.
- **Connection:** `MaxConns` của pool ≥ `Workers.MaxWorkers` + số connection ứng dụng dùng + 1. Qua PgBouncer transaction mode thì đặt `Workers.PollOnly = true`.
- **Đồng hồ:** đồng bộ NTP; `Config.LookbackMargin` phải lớn hơn độ lệch đồng hồ cộng thời gian transaction còn mở sau `Emit`.
- **Dữ liệu cá nhân:** `Config.RedactError` mặc định lưu nguyên văn lỗi; tự cấp hàm lọc nếu lỗi có thể chứa dữ liệu cá nhân.

## Tài liệu

- [Viết và publish definition](docs/definition-authoring.md): metamodel, biểu thức CEL, fact, vòng đời, định tuyến và concurrency.
- [Runbook: chạy Engine](docs/runbook-engine.md): role, kết nối, thời gian chờ, retention, janitor, dừng êm.
- [Runbook: migrate schema](docs/runbook-migrate.md): yêu cầu phiên bản, sao lưu, `flowkit migrate`.
- [contrib/](contrib/README.md): dashboard Grafana và alert Prometheus.
- Godoc của `flowkit`, `flowkittest` và `migrate` mô tả đầy đủ API.

## Phát triển

```sh
make pg-up    # Postgres 16 cho test, cổng 55432
make test     # go test -race ./...; test cần Postgres bị bỏ qua nếu thiếu FLOWKIT_TEST_DATABASE_URL
make lint     # go vet + staticcheck
make freeze   # API công khai so với api/freeze-v1.txt
make corpus   # phát lại testdata/corpus trên Engine thật
make pg-down
```
