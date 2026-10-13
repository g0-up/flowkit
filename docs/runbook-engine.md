# Runbook: chạy Engine flowkit

Áp dụng cho `flowkit.New`, `Engine.Start` và `Engine.Close`. Migrate schema theo [runbook-migrate.md](runbook-migrate.md) trước; `New` không bao giờ tự migrate.

## Khởi động

`flowkit.New(cfg, reg)` chỉ đọc, không ghi gì vào database. Nó kiểm tra:

- `Config` hợp lệ (xem các giới hạn bên dưới);
- registry nhất quán: schema JSON biên dịch được, mỗi pattern `FactDef.AvailableOn` khớp một event đã đăng ký, `OutputFact` không trùng tên `FactDef`;
- mọi module River thuộc `v0.49.x`, schema đã migrate cho bản build này và chỉ chứa object của flowkit và River.

Schema lệch trả lỗi khớp `errors.Is(err, flowkit.ErrSchemaDrift)`. Thông điệp lỗi cho biết cách xử lý:

| Thông điệp chứa | Ý nghĩa | Việc cần làm |
|---|---|---|
| `run \`flowkit migrate --schema X\`` | schema chưa migrate hoặc thấp hơn bản lib | chạy `flowkit migrate` |
| `upgrade flowkit or restore the schema` | schema cần bản lib mới hơn, hoặc có object lạ | nâng flowkit, hoặc khôi phục schema từ backup |
| `River module ... is linked` | dependency kéo River ra ngoài `v0.49.x` | ghim mọi module River trong `go.mod` |

`Emit` chạy được ngay sau `New`, không cần `Start`. Ở isolation REPEATABLE READ hoặc SERIALIZABLE, một event trùng dedup key do transaction khác commit sau khi transaction của ứng dụng lấy snapshot sẽ trả lỗi serialization (SQLSTATE 40001) và làm hỏng transaction; ứng dụng phải retry cả transaction, lần retry nhận `ErrDuplicateEvent`.

## Role

`Config.Roles` mặc định gồm cả `RoleWorker` và `RoleJanitor`.

- `RoleWorker`: xử lý queue `flowkit` (route event, pump run, wait timeout) với `Workers.MaxWorkers` worker.
- `RoleJanitor`: tham gia bầu leader. Chỉ leader chạy scheduler của River (đưa job hẹn giờ thành job sẵn sàng), rescuer và janitor định kỳ. Instance chỉ có `RoleJanitor` subscribe queue rỗng `flowkit_janitor_idle` và không xử lý job flowkit nào.

**Toàn bộ triển khai phải có ít nhất một instance đang chạy với `RoleJanitor`.** Thiếu janitor thì wait timeout và pump hẹn giờ không bao giờ tới hạn. `Start` ghi Warn `this instance has no janitor role` trên instance không có `RoleJanitor`; nếu mọi instance đều ghi dòng này, triển khai đang thiếu janitor.

## Kết nối

- River giữ một connection riêng cho LISTEN/NOTIFY. Pool của instance worker cần `MaxConns` ≥ `MaxWorkers` + số connection ứng dụng dùng + 1.
- Đi qua PgBouncer ở transaction mode: đặt `Workers.PollOnly = true`. LISTEN không chạy qua transaction pooling; worker chỉ poll theo `Workers.PollInterval` (mặc định 1s, tối thiểu 100ms).

## Đồng hồ

Mọi mốc thời gian flowkit ghi, kể cả `inserted_at` của event, lấy từ đồng hồ của instance ghi chứ không phải giờ commit của database.

- Đồng bộ đồng hồ mọi instance bằng NTP.
- `Config.LookbackMargin` (mặc định 1 phút) phải lớn hơn độ lệch đồng hồ giữa các instance cộng thời gian dài nhất một transaction của ứng dụng còn mở sau `Emit`. Transaction dài hơn thế có thể làm run bỏ sót event.

## Thời gian chờ

Chỉ `Config.ClaimTimeout` cấu hình được (mặc định 5 phút). flowkit suy ra phần còn lại:

| Mốc | Giá trị |
|---|---|
| deadline của một action | 0,8 × ClaimTimeout |
| timeout của job `pump_run` | 0,9 × ClaimTimeout |
| run bị coi là kẹt | 2 × ClaimTimeout |
| River cứu job đang chạy quá lâu | 1 giờ |
| timeout các job khác | 1 phút |

`New` từ chối `ClaimTimeout` dưới 5 giây hoặc từ 30 phút trở lên (2 × ClaimTimeout phải nhỏ hơn 1 giờ).

Riêng việc resolve fact của mỗi lượt pump có hạn `Config.FactTimeout` (mặc định 5 giây), dùng làm `statement_timeout` và deadline của context; quá hạn thì run thất bại (xem [definition-authoring.md](definition-authoring.md#chạy-run)).

## Reindex

flowkit tắt reindexer của River: leader không chạy `REINDEX CONCURRENTLY` hằng ngày trên bảng River. Nếu index của `river_job` phình to, DBA tự chạy `REINDEX INDEX CONCURRENTLY <schema>.<index>` ngoài giờ cao điểm.

## Triển khai component

Khi thêm event, action hay fact mới, rolling deploy code lên **mọi** instance xong rồi mới publish definition dùng nó. Gỡ component thì làm ngược lại: chỉ gỡ khi `Refs` và `PinnedRuns` cho thấy không còn run nào cần. Một instance thiếu component mà gặp run của definition dùng component đó sẽ cho run thất bại vĩnh viễn với terminal `$failed`, kèm log Error `flowkit: run's definition does not compile on this instance; is every component it uses deployed here?` (xem [definition-authoring.md](definition-authoring.md#vòng-đời)).

## Dừng

`Engine.Close(ctx)` ngừng nhận job và chờ job đang chạy xong. Khi `ctx` hết hạn trước, `Close` hủy context của job đang chạy, chờ thêm tối đa 10 giây rồi trả lỗi `graceful stop cut short`. Vì vậy `Close` có thể kéo dài tới thời hạn của `ctx` cộng 10 giây: chọn `ctx` sao cho thời hạn đó cộng 10 giây vẫn nhỏ hơn thời gian ân hạn của orchestrator (ví dụ với `terminationGracePeriodSeconds: 60` của Kubernetes, dùng `ctx` tối đa khoảng 45 giây). `Close` không đóng `Config.Pool`.

Hủy `ctx` truyền cho `Start` cũng dừng Engine, nhưng hủy job đang chạy ngay; dùng `Close` khi muốn dừng êm.

## Dữ liệu nhạy cảm

- Payload event không được log ở mức trên Debug; log `Emit` chỉ có type và id.
- `Config.RedactError` mặc định lưu `err.Error()` cắt còn 2 KiB, chỉ thay UTF-8 hỏng và ký tự U+0000 bằng U+FFFD, không lọc gì khác. Ứng dụng có lỗi chứa dữ liệu cá nhân phải tự cấp hàm lọc.

## Janitor

Leader River (một instance có `RoleJanitor`) chạy job định kỳ `flowkit.janitor` mỗi 30 giây, và một lần ngay khi được bầu làm leader. Mỗi lượt đi qua năm bước theo thứ tự. Bước nào cũng idempotent, nên một lượt chết giữa chừng hay leader đổi giữa lượt đều không để lại trạng thái dở:

1. **Claim quá hạn.** Effect đã claim quá `ClaimTimeout` mà chưa xong: action idempotent được tính là một lần thử (step `attempt_failed`, pump lại); action không idempotent (hoặc không còn trong registry) chuyển sang in doubt, có dead letter `effect_in_doubt` và chờ người quyết (xem [ResolveDeadLetter](#resolvedeadletter)).
2. **Look-back dở.** Run `waiting` có look-back chưa xong quá `ClaimTimeout` (pump chết giữa look-back) được pump lại; pump chạy lại look-back từ đầu.
3. **Hàng đợi mồ côi.** Key `serial` có trigger xếp hàng mà không còn run nào chưa kết thúc sẽ nhả hàng đợi, ví dụ hàng đợi để lại trong lúc definition bị tắt: janitor promote trigger đầu hàng. Definition còn tắt thì hàng đợi được giữ nguyên.
4. **Run kẹt.** Run `running` không được ghi trong 2 × `ClaimTimeout` được pump lại, trừ khi definition đang tắt, run đã có job pump đang chờ, có effect in doubt (chờ người xử lý) hoặc có effect đang claim còn hạn. Pump thừa vô hại nhờ kiểm `rev`.
5. **Retention.** Xem [Retention](#retention).

Mỗi bước xử lý theo batch 500 hàng, mỗi batch một transaction ngắn, tối đa 20 batch mỗi bước mỗi lượt; phần còn lại để lượt sau. Một bước lỗi không chặn các bước sau; job ghi lỗi của mọi bước và không retry, lượt kế tiếp chạy sau 30 giây.

| Log | Mức | Ý nghĩa |
|---|---|---|
| `flowkit: janitor resumed look-backs` | Info | bước 2 pump lại `runs` run |
| `flowkit: janitor promoted an orphaned queue` | Info | bước 3 chạy trigger đầu hàng của một key |
| `flowkit: janitor pumped stuck runs` | Warn | bước 4 pump lại `runs` run; lặp lại thường xuyên nghĩa là worker chết hoặc job bị mất |
| `flowkit: janitor applied retention` | Info | bước 5 xóa `rows` hàng ở bước `step` |

## ResolveDeadLetter

Dead letter là việc flowkit không tự quyết được. Dead letter mở được đếm ở gauge `flowkit.dead_letters_open{kind}` và không bao giờ bị xóa tự động. Engine chưa có API liệt kê dead letter; lấy id bằng truy vấn chỉ đọc:

```sql
SELECT id, kind, ref, run_id, event_id, note, created_at
FROM <schema>.dead_letters
WHERE resolved_at IS NULL
ORDER BY created_at;
```

`Runs().Trace(ctx, runID)` cho thấy run đã đi tới đâu (step `effect_in_doubt` kèm action và lỗi). `Trace` trả mọi step trong một response, không phân trang: run lặp hàng nghìn step trả về hết. Xử lý bằng `Runs().ResolveDeadLetter(ctx, id, flowkit.Resolution{Kind, Actor, Reason, Output})`; `Actor` và `Reason` được lưu lại để đối soát.

| Loại | `ResolveRedrive` | `ResolveMarkDone` | `ResolveSkip` |
|---|---|---|---|
| `effect_in_doubt` | gỡ in doubt, action chạy lại với cùng idempotency key; từ chối khi effect đã có output từ lần thành công muộn hoặc run đã kết thúc | lưu `Output` (phải khớp `OutputFacts` của action) làm output, run đi tiếp; từ chối khi run đã kết thúc | chỉ ghi nhận; run vẫn chặn tới khi `Cancel`. Là lựa chọn duy nhất khi run đã kết thúc |
| `exhausted` | từ chối | từ chối | ghi nhận (run đã `failed`; muốn chạy lại thì emit event mới) |
| `schema_mismatch`, `unroutable_event` | route lại event; event đã bị retention xóa thì `ErrNotFound` | từ chối | ghi nhận |

Với `effect_in_doubt`, kiểm tra phía nhà cung cấp trước: hiệu ứng **chưa** xảy ra thì `ResolveRedrive`; **đã** xảy ra thì `ResolveMarkDone` với output thật; không muốn run đi tiếp thì `ResolveSkip` rồi `Runs().Cancel`.

Action bị janitor đưa vào in doubt vẫn có thể trả kết quả muộn. Khi đó flowkit thêm một dòng vào `note` của dead letter ("the action succeeded after its claim was revoked; its output is the effect's result") và lưu output vào `executed_effects.result` (ref của dead letter là `idem_key`). Effect như vậy từ chối `ResolveRedrive` với `ValidationErrors` path `/Kind` ("the effect has the output of a late success; resolve with mark_done"), vì action chắc chắn đã chạy và chạy lại sẽ lặp hiệu ứng. Dùng `ResolveMarkDone` với output đó. Nếu redrive commit trước khi kết quả muộn tới, output muộn bị bỏ (log Warn `the orphaned output is dropped`) và action chạy lại như redrive bình thường.

Effect của run đã kết thúc (thường do `Cancel`) chỉ nhận `ResolveSkip`. `ResolveRedrive` và `ResolveMarkDone` trả `ValidationErrors` path `/Kind` ("run … has ended; only skip is accepted"), dead letter vẫn mở.

Việc claim dead letter, hành động và job tiếp theo commit trong một transaction. Hai lệnh xử lý cùng một dead letter đồng thời: đúng một lệnh có hiệu lực, lệnh kia nhận `ErrAlreadyResolved`, nên redrive hai lần không làm action chạy hai lần. Lệnh bị từ chối hoặc `Output` không khớp (kể cả chuỗi chứa U+0000) trả `ValidationErrors` (path `/Kind` hoặc `/Output`) và dead letter vẫn mở. Id không tồn tại trả `ErrNotFound`. `ErrNotFound` cũng được trả khi dead letter vẫn mở nhưng effect của nó đã bị retention xóa (run bị `Cancel` rồi quá `StepDetail`): khi đó dùng `ResolveSkip` để đóng dead letter.

`Runs().Cancel(ctx, runID, reason)` kết thúc run với state `cancelled`, terminal `$cancelled`, `reason` ghi ở step kết thúc. Đăng ký chờ của run bị xóa (timeout đến sau chỉ ghi `claim_lost`) và trigger kế tiếp trong hàng đợi `serial` được chạy. Hủy run đã kết thúc không làm gì; run không tồn tại trả `ErrNotFound`.

Run id suy ra được từ tên definition và id event, nên không phải bí mật: ứng dụng phải tự phân quyền mọi lệnh `Runs()`.

## Retention

Bước 5 của janitor xóa dữ liệu theo `Config.Retention`:

| Trường | Mặc định | Tối thiểu | Xóa gì |
|---|---|---|---|
| `StepDetail` | 90 ngày | 1 ngày | run đã kết thúc quá hạn (tính từ lúc kết thúc): ghi `run_summaries` rồi xóa steps, event đã dùng, đăng ký chờ, effect và hàng `runs` trong cùng transaction |
| `Run` | 2 năm | `StepDetail` | summary quá hạn, tính từ lúc run kết thúc |
| `Event` | 30 ngày | `LookbackMargin` + `ClaimTimeout` | event quá hạn không còn run, event đã dùng hay hàng đợi nào tham chiếu |
| `DeadLetter` | 90 ngày | 1 giờ | dead letter **đã xử lý**, tính từ lúc xử lý |

`New` từ chối giá trị dưới mức tối thiểu. Hệ quả cần biết:

- Run đã tóm tắt: `Trace` trả `ErrNotFound`, `List` vẫn liệt kê (cùng state, terminal name, số step). Run đó không còn được tính cho `dedup_window` và `cancel_previous`, nên giữ `StepDetail` lớn hơn `dedup_window` dài nhất đang dùng.
- `Emit` chống trùng theo dedup key chỉ trong `Retention.Event`.
- Look-back chỉ thấy event còn giữ. Event cũ hơn `Retention.Event` đã bị xóa (trừ khi một run, một event đã dùng hay một hàng đợi còn tham chiếu), nên một run vào wait sau hơn `Retention.Event` kể từ lúc tạo chỉ nhận event còn trong hạn giữ, không phải mọi event từ lúc run bắt đầu. Run có wait xa hơn thế cần `Retention.Event` dài hơn. Mức tối thiểu `LookbackMargin` + `ClaimTimeout` chỉ giữ event quanh lúc tạo run cho look-back mà janitor chạy lại sau khi pump mất claim.
- Event của dead letter `schema_mismatch` hay `unroutable_event` có thể bị xóa trước khi dead letter được xử lý; khi đó redrive trả `ErrNotFound`.

Retention chỉ dùng `DELETE` theo batch trên các index sẵn có, không đổi DDL. Một lượt xóa tối đa 20 × 500 hàng mỗi loại; lần bật đầu tiên trên database đã tích lũy nhiều năm sẽ mất nhiều lượt (mỗi 30 giây) mới bắt kịp.

## Metrics và cảnh báo

flowkit ghi trace và metric qua OpenTelemetry API trên `Config.Tracer` và `Config.Meter` (mặc định no-op) và không kèm exporter. Mỗi lượt pump là một span `flowkit.run` (thuộc tính `run_id`, `def_name`, `def_version`, `metamodel_version`, `lib_version`, `instance_id`), mỗi step là span con `flowkit.step`.

| Metric | Loại | Thuộc tính | Ý nghĩa |
|---|---|---|---|
| `flowkit.events_emitted` | counter | `type` | event `Emit` đã lưu (đếm cả event của transaction rollback) |
| `flowkit.routes` | counter | `def`, `outcome` | kết quả route (xem [definition-authoring.md](definition-authoring.md)) |
| `flowkit.runs_started` | counter | | run được tạo |
| `flowkit.runs_terminal` | counter | `state` | run kết thúc |
| `flowkit.effect_duration` | histogram (s) | `action`, `def` | thời gian chạy action |
| `flowkit.effect_retries` | counter | `action`, `def` | lần chạy lỗi sẽ thử lại |
| `flowkit.effects_inflight` | up-down counter | `action`, `def` | action đang chạy trên instance này |
| `flowkit.effect_orphaned` | counter | `action`, `def` | action xong sau khi janitor đã thu hồi claim |
| `flowkit.effect_rate_limited` | counter | `action`, `def` | lần chạy bị hoãn do rate limit |
| `flowkit.wake_latency` | histogram (s) | | từ lúc lưu event tới lúc wait được đánh thức |
| `flowkit.timer_lag` | histogram (s) | | từ hạn timeout tới lúc xử lý |
| `flowkit.lookback_truncated` | counter | `def` | look-back chạm giới hạn 10 000 event |
| `flowkit.match_errors` | counter | `def` | `match` lỗi lúc chạy |
| `flowkit.dead_letters` | counter | `kind` | dead letter được ghi |
| `flowkit.dead_letters_open` | gauge | `kind` | dead letter chưa xử lý |
| `flowkit.in_doubt_open` | gauge | | effect in doubt của run chưa kết thúc |
| `flowkit.queue_depth` | gauge | | job sẵn sàng trong queue `flowkit` |
| `flowkit.scheduler_leader` | gauge | `instance` | 1 trên instance đang là leader, 0 trên các instance khác |

Các gauge đọc từ database mỗi lần MeterProvider thu thập, nên mọi instance báo cùng một giá trị: tổng hợp bằng `max`, không cộng.

`flowkit.scheduler_leader` mang thuộc tính `instance` là `Config.InstanceID`, mặc định `hostname-pid`, nên mỗi lần restart sinh một series mới. Khi thu metric, đặt `InstanceID` ổn định (ví dụ tên pod) để số series không tăng theo số lần deploy.

Dashboard Grafana và luật cảnh báo Prometheus nằm trong `contrib/`; cách import và quy ước tên metric ở [contrib/README.md](../contrib/README.md).

| Cảnh báo | Điều kiện | Việc cần làm |
|---|---|---|
| `FlowkitDeadLettersOpen` | có dead letter mở 5 phút | xử lý theo [ResolveDeadLetter](#resolvedeadletter) |
| `FlowkitEffectInDoubt` | có effect in doubt 1 phút | kiểm tra nhà cung cấp rồi xử lý dead letter `effect_in_doubt` |
| `FlowkitEffectOrphaned` | `effect_orphaned` tăng trong 15 phút | action chạy lâu hơn claim: tăng `ClaimTimeout` hoặc rút ngắn action; effect đó đang in doubt |
| `FlowkitTimerLag` | p95 `timer_lag` > 60s trong 10 phút | kiểm tra leader, NTP trên host janitor, và `queue_depth` |
| `FlowkitWakeLatency` | p95 `wake_latency` > 30s trong 10 phút | thêm worker, kiểm tra `PollOnly`/`PollInterval` |
| `FlowkitNoSchedulerLeader` | không instance nào là leader 5 phút | khởi động một instance có `RoleJanitor` (xem [Role](#role)) |

### Độ trễ đo được

Ngân sách: `wake_latency` p95 ≤ 30s, `timer_lag` ≤ 60s. Đo bằng `make pg-up && make nfr` (test `internal/nfr`, chỉ chạy khi `FLOWKIT_NFR=1`): một Engine thật, đồng hồ hệ thống, `MaxWorkers` 4, một instance có cả `RoleWorker` và `RoleJanitor`. Tải: 100 run chờ event được đánh thức và 100 run chờ hết timeout 10s, mỗi loại 5 event/giây trong 20 giây — cao hơn đỉnh dự phóng (ngàn event/giờ). Số liệu là giá trị thật từng lần ghi vào hai histogram của Engine, percentile nearest-rank.

Lần đo 12/10/2026, MacBook M1 Pro, PostgreSQL 16 trong Docker cục bộ, Go 1.26.0, không bật `-race`:

| Cấu hình | `wake_latency` p50 / p95 / max | `timer_lag` p50 / p95 / max |
|---|---|---|
| Mặc định (LISTEN/NOTIFY) | 0.40s / 0.53s / 0.54s | 0.19s / 0.74s / 0.95s |
| `PollOnly` | 0.39s / 0.55s / 0.57s | 0.34s / 0.60s / 1.07s |

Cùng ngày, chạy với `-race`: mặc định wake p95 0.54s, timer lag p95 0.71s, max 0.92s; `PollOnly` wake p95 1.09s, timer lag p95 0.63s, max 1.05s.

Cả hai cấu hình nằm trong ngân sách với biên rộng. Đây là số đo trên laptop với Postgres cục bộ, chỉ để so tương đối: đo lại trên hạ tầng thật của consumer và đặt ngưỡng cảnh báo theo đó. `timer_lag` trên mạng thật còn cộng thêm độ lệch giờ của host janitor, nên giữ NTP (xem [Đồng hồ](#đồng-hồ)). Luật `FlowkitTimerLag` hiện dùng p95; giá trị max ở bảng trên cho biết khoảng cách nếu chuyển sang cảnh báo theo max.
