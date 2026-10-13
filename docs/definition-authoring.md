# Viết và publish definition

Áp dụng cho body definition (metamodel version 1), `Registry.ValidateDefinition` và `Engine.Definitions()`. Mẫu đầy đủ: [`testdata/valid/sellkit-fulfillment.json`](../testdata/valid/sellkit-fulfillment.json), [`testdata/valid/lms-onboarding.json`](../testdata/valid/lms-onboarding.json); registry tương ứng là `sampleRegistry` trong `definition_validate_test.go`.

## Metamodel

Body là JSON theo [`schema/m2-v1.json`](../schema/m2-v1.json). **Đây là bản nháp**: chỉ phủ `trigger`, `concurrency` và bốn loại step `action`, `wait_for_event`, `switch`, `end`. Khi metamodel M2 chính thức có, chỉ parser đổi; model nội bộ (`internal/interpreter/model`) độc lập với hình dạng JSON. Lựa chọn macro CEL của mỗi metamodel version khai báo trong `schema/schema.go`; đổi danh sách đó nghĩa là tăng metamodel version.

Mỗi metamodel version được hỗ trợ có ít nhất một case trong corpus tương thích `testdata/corpus/<version>/<tên>/` (definition, kịch bản event, trace mong đợi), chạy trên Postgres bởi `corpus_test.go`. Thay đổi làm lệch một trace đã ghi là thay đổi hành vi của definition đã publish; chỉ ghi lại trace (`go test -run TestCorpus -update-corpus .`) khi thay đổi đó là chủ ý.

Giới hạn: body ≤ 1 MiB, ≤ 500 step, biểu thức ≤ 4096 byte và độ sâu ≤ 64, `timeout` và `dedup_window` > 0 và ≤ 365 ngày (đơn vị `s`, `m`, `h`, `d`, ghép được: `1h30m`). Step đầu tiên là điểm vào. Mọi step phải tới được từ step đầu và có đường tới một step `end`; vòng lặp được phép nếu có lối ra. `metamodel_version` là số nguyên; cách viết khác của cùng số (`1.0`, `1e0`) được chấp nhận.

Số trong body (và trong payload của `Emit`) phải là float64 hữu hạn, khác 0 thì không được tràn dưới về 0, số mũ trong khoảng ±1000 và không quá 16383 chữ số sau dấu thập phân; vi phạm là lỗi validate có path, trước khi ghi gì vào database. Số nguyên lớn hơn 2^53 (ví dụ `9007199254740993`) vẫn được giữ chính xác.

`concurrency.policy` là `concurrent` (mặc định khi bỏ khối `concurrency`), `serial`, `cancel_previous` hoặc `dedup_window`. Mọi policy trừ `concurrent` bắt buộc `serial_key` (biểu thức trả `string`); `dedup_window` chỉ dùng với policy cùng tên.

## Biểu thức

Biểu thức là CEL, kiểm tra kiểu lúc validate. Biến:

| Biến | Có ở | Kiểu |
|---|---|---|
| `event` | mọi biểu thức | payload của trigger; trong `match` của `wait_for_event` là payload của event đang chờ |
| `facts` | `when` của action, `cases[].when`, `$expr` trong params | xem dưới |
| `now` | mọi biểu thức | `timestamp`, cố định trong một lượt pump |
| `scope` | mọi biểu thức | `entity_kind`, `entity_id` (string), `extra` (map string→string) |

`match` và `serial_key` không có `facts`.

**Kiểu từ JSON Schema.** `string` → string, `string` + `format: date-time` → timestamp, `integer` → int, `number` → double, `boolean` → bool, object có `properties` → object (field sai tên bị bắt, kể cả field lồng và field của phần tử list), object chỉ có `additionalProperties` → map, `array` → list. `oneOf`/`anyOf`, `$ref` đệ quy và field không khai báo kiểu thành `dyn`: ở đó tên field và kiểu không được kiểm tra lúc validate mà chỉ lỗi lúc chạy. Payload đã đi qua jsonb vẫn so sánh đúng: field `integer` thành int (`event.qty == 3` đúng), giá trị không nguyên cho field `integer` là lỗi.

**`has()` bắt buộc cho giá trị có thể vắng.** Field không nằm trong `required` của schema, khóa của map và fact không chắc có phải được `has()` bảo vệ trên cùng nhánh logic:

```
has(event.shipping) && event.shipping.country == 'VN'
has(event.name) ? event.name : 'there'
!has(event.carrier) || event.carrier != 'test'
```

Đọc map theo khóa (`scope.extra["school"]`, `m[k]`) cũng vậy: guard bằng `has(scope.extra.school)` (khóa là identifier) hoặc `"school" in scope.extra` (khóa bất kỳ, kể cả biến: `event.region in scope.extra`). Biến của macro chạy trên chính map đó là khóa có thật: `m.exists(k, m[k] == 'x')` không cần guard. Truy cập list theo chỉ số (`event.items[0]`) không được kiểm tra; chỉ số ngoài phạm vi là lỗi lúc chạy.

Kiểm tra so path đúng như đã viết nên bảo thủ: guard qua kết quả hàm hoặc qua biến của macro không được nhận ra, và biểu thức phải viết lại. Với object lồng tùy chọn, guard từng tầng: `has(event.customer.address) && has(event.customer.address.zip)`.

**Fact.** `facts.<tên có chấm>` gồm:

- fact đăng ký bằng `MustAddFact` có `AvailableOn` khớp event trigger: luôn có;
- `OutputFacts` của action đứng trước step: dùng thẳng nếu mọi đường tới step đều đi qua action đó và action không có `when`; ngược lại phải `has(facts.x.y)`.

Fact không thuộc tập này là lỗi validate. Tên fact (của `MustAddFact` và `OutputFacts`) là các identifier CEL nối bằng dấu chấm, không trùng từ khóa CEL (`in`, `null`, ...), và không được lồng dưới tên fact khác (`customer` và `customer.vip` không cùng tồn tại); registry vi phạm bị `flowkit.New` từ chối.

**Params.** Giá trị `{"$expr": "<cel>"}` ở bất kỳ độ sâu nào trong `params` là biểu thức, đánh giá khi step chạy; kiểu của nó phải khớp kiểu `ParamsSchema` khai báo tại vị trí đó. Phần còn lại của `params` là literal và phải qua `ParamsSchema`. Ngoại lệ: khi trên đường từ gốc `params` tới một `$expr` có schema dùng `oneOf`, `anyOf`, `not` hoặc `if` (đi theo `properties`, `items`, `additionalProperties`, `allOf` và `$ref` nội bộ), literal hợp lệ hay không phụ thuộc giá trị của biểu thức, nên các literal từ chỗ đó trở xuống chỉ được kiểm tra khi step chạy, trên params đã resolve. Biểu thức vẫn được kiểm tra kiểu như thường.

**Chi phí.** Lúc validate, chi phí ước lượng tính từ `maxItems`/`maxProperties`/`maxLength` của schema (không khai báo: 1000 phần tử, 4096 byte) không được vượt 1 000 000. Lúc chạy mỗi lần đánh giá bị chặn ở chi phí 1 000 000 và 100 ms; với vòng lặp nặng thì mốc 100 ms thường tới trước. Vượt giới hạn là lỗi cấu hình của definition, không phải lỗi tạm thời. Muốn ước lượng sát hơn, khai báo `maxItems` và `maxLength` trong schema.

## Validate trong CI

```go
reg := app.Registry() // registry thật của ứng dụng
if err := reg.ValidateDefinition(body); err != nil {
	var verrs flowkit.ValidationErrors
	errors.As(err, &verrs) // mỗi lỗi có Path là JSON Pointer vào body
}
```

Không cần database. Mọi lỗi được trả cùng lúc, sắp theo path; lỗi trong biểu thức trỏ tới chính field chứa biểu thức (ví dụ `/steps/2/params/express/$expr`), message có `dòng:cột` trong biểu thức. Registry mà `flowkit.New` sẽ từ chối trả lỗi registry thay vì `ValidationErrors`.

**Kiểm tra cấu trúc bằng CLI.** Khi không có registry trong tay (ví dụ pre-commit trên repo chỉ chứa definition), `flowkit validate <file>` kiểm tra phần không cần registry: schema M2 của `metamodel_version`, giới hạn kích thước, id step, mọi `next`/`default`/`on_timeout` trỏ tới step có thật, mọi step tới được và có đường tới `end`, và cú pháp CEL của mọi biểu thức (`serial_key`, `when`, `cases[].when`, `match`, `$expr`), chỉ parse chứ không kiểm tra kiểu.

```console
$ flowkit validate defs/fulfillment.json
defs/fulfillment.json: /steps/0/when: 1:14: Syntax error: mismatched input '<EOF>' expecting ...
defs/fulfillment.json: /steps/2: step is unreachable from the first step
defs/fulfillment.json: 2 problems
flowkit validate checks structure only; run Registry.ValidateDefinition with the application's registry for full validation (events, actions, facts, params and expression types)
```

Mỗi lỗi một dòng trên stderr, dạng `<file>: <JSON Pointer>: <message>`, sắp theo path; có lỗi thì exit 1, cấu trúc hợp lệ thì exit 0 và in `<file>: structure is valid`, sai cách gọi thì exit 2. Dòng cuối luôn nhắc rằng CLI **không** thay `Registry.ValidateDefinition`: event, action, fact chưa đăng ký, params sai schema, kiểu biểu thức, `has()` thiếu và chi phí chỉ được bắt khi validate với registry thật, nên CI vẫn phải chạy đoạn Go ở trên.

## Vòng đời

- `SaveDraft(ctx, name, body)` validate rồi lưu bản nháp; tên ≤ 255 byte.
- `Publish(ctx, draftID, PublishOpts{Actor, Note})` validate lại với registry hiện tại của Engine (trước khi mở transaction ghi), rồi trong một transaction: tạo definition (enabled) nếu chưa có, ghi version kế tiếp, ghi component dùng tới (event, action, fact đăng ký; output fact tính theo action), và cập nhật các cột route đọc (`current_version`, `trigger_event_type`, `concurrency_policy`, `serial_key_cel`, `dedup_window_seconds`). Publish đồng thời cho cùng tên ra các version liên tiếp. Run đang chạy giữ version nó bắt đầu. `Actor` được lưu nguyên văn; ứng dụng chịu trách nhiệm xác thực. Publish lại cùng một draft là hợp lệ và tạo version mới với cùng body (cùng `Checksum`); flowkit không chặn trùng.
- `Get(ctx, name, version)` trả version đã publish, `ErrNotFound` nếu không có. `Checksum` là SHA-256 của body như đã lưu trong jsonb (Postgres chuẩn hóa số), với khóa sắp xếp và số nguyên viết dạng thập phân: không phụ thuộc khoảng trắng, thứ tự khóa hay cách viết số nguyên (`2`, `2.0`, `2e0` như nhau). Số lẻ giữ cách viết của jsonb: `1.5e0` và `1.5` như nhau, nhưng `1.50` khác `1.5`.
- `Disable` ngừng tạo run mới và dừng các run `running` ở bước kế tiếp. `Enable` bật lại và, trong cùng transaction, xếp lịch tiếp tục mọi run `running`. Run `waiting` không cần: event chờ vẫn được nhận khi definition bị tắt. Gọi `Enable` cho definition đang bật lại xếp lịch các run `running` lần nữa; job thừa không có gì để làm.
- `Get`, `Enable`, `Disable`, `PinnedRuns` trả `ErrNotFound` cho tên mà không definition nào có thể có (rỗng, quá 255 byte, chứa NUL hoặc không phải UTF-8).
- `Refs(ctx, ComponentRef{Kind, Ref})` liệt kê các version dùng một component; dùng trước khi gỡ event, action hoặc fact khỏi registry. `PinnedRuns(ctx, name)` đếm run chưa kết thúc theo version.
- **Deploy trước, publish sau.** `Publish` chỉ validate với registry của instance gọi nó. Mọi instance chạy Engine đều compile lại body đã pin của run khi xử lý run đó. Vì vậy phải deploy code có component mới (event, action, fact) lên **mọi** instance trước khi publish definition dùng component đó, và chỉ gỡ component khỏi registry khi `Refs` và `PinnedRuns` cho thấy không còn run nào cần nó. Làm ngược lại thì một instance thiếu component sẽ không compile được definition. Mọi run của version đó mà instance ấy xử lý, dù là lượt pump, look-back, timeout hay route event tới wait, đều thất bại có kiểm soát: state `failed`, terminal `$failed` ở step `$run`, lý do (đã qua `Config.RedactError`) nằm trong `run_steps.error`, kèm log Error `flowkit: run's definition does not compile on this instance; is every component it uses deployed here?`. Run đã thất bại không tự chạy lại, và deploy bù hay publish lại cũng không cứu được nó.

## Định tuyến và concurrency

Worker route mỗi event đã lưu tới mọi definition đang bật có `trigger.event` trùng type, mỗi definition trong một transaction riêng, đọc version hiện hành ngay trong transaction đó; run giữ version này. Mỗi cặp (definition, event) có nhiều nhất một run, id là UUIDv5 của `tên:event_id`, nên route lại một event (job retry) hay publish version mới giữa hai lần route không tạo run thứ hai. Một definition lỗi không chặn các definition khác; job được River retry và bỏ qua các definition đã route. Definition đang tắt lúc event được route thì bỏ qua event đó: `Enable` sau này không tạo run cho event đã phát trong lúc tắt.

Trước khi route, payload được kiểm tra lại với `EventDef` hiện có; không khớp thì event không được route và có một dead letter `schema_mismatch` (ref là `event_id`). Trong rolling upgrade, instance còn chạy bản cũ (chưa đăng ký event type mới, hoặc còn schema cũ) có thể nhận job route của event do bản mới phát và ghi `schema_mismatch`; nâng cấp xong mọi instance thì redrive các dead letter này. `serial_key` lỗi, trả chuỗi rỗng, dài hơn 512 byte hoặc chứa U+0000 thì definition đó không tạo run và có một dead letter `unroutable_event` (ref là `event_id:tên`); route lại không tạo dead letter trùng khi bản cũ chưa resolve.

Policy áp cho các trigger cùng `serial_key` của một definition. Thứ tự các trigger là thứ tự emit, tức `(inserted_at, id)` của event, không phải thứ tự job route chạy (job có thể chạy lệch nhau hoặc retry):

- `concurrent`: mọi trigger tạo run.
- `serial`: một run chưa kết thúc mỗi key. Trigger đến khi key không có run nào đang chạy và không có hàng đợi thì chạy ngay, kể cả khi một event phát sớm hơn được route sau nó; các trigger còn lại xếp hàng và chạy lần lượt theo thứ tự emit khi run trước kết thúc. Definition bị tắt giữ nguyên hàng đợi; sau `Enable`, janitor promote trigger đầu hàng của mỗi key không còn run nào đang chạy, trong vòng một chu kỳ janitor (30 giây) (xem [runbook-engine.md](runbook-engine.md#janitor)).
- `cancel_previous`: trigger hủy run chưa kết thúc của key (state `cancelled`, terminal name `$cancelled_by_newer`) rồi tạo run. Nếu key đã có run (ở bất kỳ state nào) của một event phát sau trigger này, trigger bị bỏ qua với outcome `superseded`: không hủy gì, không tạo run, route lại cũng vậy. Một trigger route tới key không còn run nào thì vẫn chạy.
- `dedup_window`: trigger bị bỏ qua nếu key đã có run tạo sau thời điểm event được emit trừ `dedup_window`. Cửa sổ tính từ lúc emit, nên route trễ hay route lại không làm event lọt cửa sổ.

Kết quả mỗi lần route được log (`flowkit: event routed`, kèm `event_id`, `def_name`, `outcome`, `run_id`) và đếm ở metric `flowkit.routes{def, outcome}`, outcome là `started`, `queued`, `deduped`, `superseded`, `cancelled_previous`, `already_routed` hoặc `unroutable`. Route lại một event đã có run, đang xếp hàng hoặc đã có dead letter `unroutable_event` chưa resolve được đếm là `already_routed`; `deduped` và `superseded` không để lại dấu vết trong database nên được đếm lại ở mỗi lần route.

## Chạy run

Run được đẩy đi bởi job `pump_run`. Mỗi lượt pump đọc run, tính `now` và resolve fact một lần, rồi đi qua các step `switch` và các action có `when` sai cho tới khi gặp một action được chạy, một `wait_for_event` hoặc một `end`. **Mỗi lượt chạy nhiều nhất một action**; lượt sau được xếp lịch ngay khi action xong. Một lượt quay lại step nó đã đi qua mà chưa chạy action hay chờ event nào thì run thất bại (`$failed`): vòng lặp phải chứa một action hoặc một wait.

Mọi ghi trạng thái run so `rev` của run; lượt pump thua một lượt khác đồng thời ghi step `stale_pump` và dừng, không chạy action. Lịch sử từng run nằm trong `run_steps` (`entered`, `executed`, `skipped`, `branched`, `waiting`, `claimed`, `claim_lost`, `match_error`, `stale_pump`, `terminal`).

**Action.** Trước khi chạy, step được đánh dấu `entered` cùng fact và `now` của lượt đó. Nếu worker chết sau bước này, lượt sau chạy lại đúng action đó với đúng fact và `now` đã đóng băng: fact đổi trong lúc đó không làm đổi nhánh hay params. Params đã resolve được kiểm tra lại với toàn bộ `ParamsSchema`; không hợp lệ thì run thất bại `$failed`. Mỗi lần ghé một action step là một lần thực thi riêng với idempotency key `<run_id>:<step_id>:<lần ghé>`, nên trong vòng lặp mỗi vòng có hiệu ứng riêng. Kết quả của executor:

| Kết quả | Run |
|---|---|
| xong | ghi output fact, sang `next` |
| xong nhưng output không khớp `OutputFacts`, hoặc thất bại hẳn | `failed`, terminal `$action_failed`; thất bại hẳn có thêm dead letter `exhausted` (ref là idempotency key) |
| cần thử lại sau | pump được hẹn lại theo độ trễ executor trả |
| đang chờ xác minh (in doubt) | dừng; bên giải quyết hiệu ứng sẽ pump lại |
| bị bỏ dở (job hết context) hoặc lỗi tạm | job được River retry |

**Fact.** Resolver chạy trong transaction chỉ đọc, bị chặn bởi `SET LOCAL statement_timeout` và deadline của context, cùng bằng `Config.FactTimeout` (mặc định 5 giây). Resolver lỗi, panic, chậm quá hạn hoặc thử ghi thì run thất bại `$failed`, lý do đi qua `Config.RedactError`; panic được bắt lại thành lỗi của run. Chế độ chỉ đọc và `statement_timeout` là rào chắn chống nhầm lẫn, **không phải sandbox**. Một câu `COMMIT` hay `SET` truyền qua `Query` sẽ gỡ cả hai, khi đó chỉ còn deadline của context giữ thời gian chạy. Resolver là code tin cậy của ứng dụng, chạy trong process Engine với quyền của role database.

**Wait.** Vào `wait_for_event`, run chuyển sang `waiting`, đăng ký chờ và hẹn timeout sau `timeout`. Event được nhận theo hai đường dùng chung một luật: route của event mới tới, và look-back đọc các event đã lưu trước khi đăng ký commit. Một event được nhận khi nó đúng type, được lưu không sớm hơn lúc tạo run trừ `Config.LookbackMargin`, không phải trigger của chính run, run chưa dùng nó, và `match` trả `true`. Wait không tự gắn với entity của run: muốn chờ event của chính đơn hàng hay học viên đó, viết điều kiện trong `match`, ví dụ `event.order_id == scope.entity_id`. Event đầu tiên thỏa (theo thứ tự lưu) thắng; run đi tiếp sang `next` và event đó được ghi là đã dùng. **Mỗi event chỉ thỏa một wait của một run**: hai wait cùng type nối tiếp nhau cần hai event, route lại cùng một event không thỏa wait sau. Các run khác vẫn nhận cùng event đó độc lập. Look-back chỉ thấy event còn được giữ: janitor xóa event cũ hơn `Config.Retention.Event` (mặc định 30 ngày) mà không run, event đã dùng hay hàng đợi nào tham chiếu, nên wait vào sau hơn `Retention.Event` kể từ lúc tạo run chỉ nhận event còn trong hạn giữ. Definition có wait xa như vậy cần `Retention.Event` dài hơn (xem [runbook-engine.md](runbook-engine.md#retention)).

Event và timeout tranh nhau một wait: đúng một bên thắng, bên thua ghi `claim_lost`. Timer tới sớm (đồng hồ lệch) được hẹn lại tới đúng hạn. Timer của một lần chờ cũ (run đã nhận event, đã sang wait khác, hoặc đã quay lại cùng wait trong vòng lặp) chỉ ghi `claim_lost`. Hết hạn thì run sang `on_timeout`.

Look-back đọc theo trang 1000 event, tối đa 10 000 event mỗi lần. Số event phải đọc là số event cùng type được lưu từ lúc tạo run trừ `LookbackMargin`, bất kể `match` (match được đánh giá sau khi đọc). Chạm giới hạn thì log Warn `flowkit: wait look-back stopped at its row limit; the run waits for new events` và tăng metric `flowkit.lookback_truncated{def}`; run vẫn chờ event mới, còn event đã lưu nằm ngoài giới hạn bị bỏ sót. Cả look-back chạy trong một transaction. Mỗi lần đánh giá `match` được tối đa 100 ms, nên với `match` nặng trên 10 000 event, transaction có thể giữ connection và snapshot tới lúc job pump hết hạn (0,9 × `ClaimTimeout`). Khi đó job được retry và đọc lại từ đầu. Hãy giữ `match` rẻ, và giữ ít event cùng type trong khoảng look-back.

Route một event đọc các wait đang chờ type đó theo trang 1000 run, mỗi trang một transaction, rồi đánh giá `match` và claim từng wait trong transaction riêng. Toàn bộ việc này nằm trong job `route_event` có timeout 1 phút. Với rất nhiều run cùng chờ một type, nhất là khi `match` nặng, job có thể hết hạn trước khi đi hết các trang; retry sẽ làm lại từ đầu, bỏ qua các wait đã claim.

**`match` lỗi lúc chạy** (ví dụ field vắng không có `has()`) chỉ ảnh hưởng wait đó với event đó: run ghi step `match_error`, metric `flowkit.match_errors{def}` tăng, event bị coi như không khớp. Các run và wait khác vẫn nhận event bình thường. Mỗi bộ (wait, lần ghé, event) chỉ được ghi một `match_error`, và metric chỉ đếm bản ghi mới: route bị retry, hay look-back và route cùng gặp một event, đều không ghi lặp. Đây là kiểm tra trước khi ghi, không có ràng buộc unique, nên hai transaction đánh giá cùng event cùng lúc (hiếm) vẫn có thể ghi hai lần.

**Kill switch.** Definition bị `Disable` thì lượt pump kế tiếp dừng mà không đổi gì, run vẫn `running` ở đúng step; `Enable` cho chạy tiếp từ đó. Run `waiting` vẫn nhận event và timeout khi definition bị tắt, rồi dừng ở lượt pump sau.

**Kết thúc.** Step `end` cho state `completed` với terminal name là `name` của step. Run thất bại ngoài step `end` có state `failed` và terminal `$action_failed` hoặc `$failed`; run bị `cancel_previous` hủy có terminal `$cancelled_by_newer`; run bị `Runs().Cancel` hủy có state `cancelled`, terminal `$cancelled`. Khi kết thúc, run của definition `serial` nhả key cho trigger kế tiếp trong hàng đợi.

## Rủi ro dependency

flowkit dùng `cel.dev/cel-go`. Ứng dụng có dependency kéo bản cel-go ở module path cũ `github.com/google/cel-go` (ví dụ thư viện Kubernetes) sẽ có hai bản cel-go trong binary; chúng không xung đột nhưng làm binary lớn hơn.
