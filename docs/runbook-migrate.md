# Runbook: migrate schema flowkit

Áp dụng cho `flowkit migrate` và `migrate.Run`. Mỗi schema flowkit chứa cả bảng của flowkit lẫn bảng của River (`river_job`, `river_migration`, ...). Lệnh migrate chạy migration River trước, rồi migration flowkit, dưới một khóa hàng `flowkit_meta(k='migrate_lock')`.

## Yêu cầu phiên bản

- **Mọi module River phải thuộc minor `v0.49.x`.** Danh sách object River được phép (`internal/migrate/testdata/river-objects-v0.49.0.txt`) và golden object DB được ký cho minor này. River phát hành `github.com/riverqueue/river` cùng các module con (`riverdriver`, `riverdriver/riverpgxv5`, `rivershared`, `rivertype`) chung một version; ghim tất cả trong `go.mod` của ứng dụng:

  ```
  require (
      github.com/riverqueue/river v0.49.0
      github.com/riverqueue/river/riverdriver/riverpgxv5 v0.49.0
  )
  ```

  Khi khởi động, flowkit đọc `debug.ReadBuildInfo` và kiểm tra mọi module `github.com/riverqueue/river` và `github.com/riverqueue/river/...` (áp cả `replace`). Chỉ cần một module bị một dependency khác kéo ra ngoài `v0.49.x` qua MVS là `flowkit.New` từ chối khởi động, và lỗi nêu tên module lệch. Mọi patch `v0.49.x` đều được chấp nhận. Module có version riêng như `github.com/riverqueue/riverui` không thuộc phạm vi kiểm tra. Bản build không có build info, hoặc module River build từ mã nguồn local (`(devel)`, `replace` sang thư mục), chỉ ghi Warn và bỏ qua module đó.
- **Đổi bản River mà flowkit ghim nghĩa là ký lại danh sách allowlist.** Khi flowkit nâng River sang minor khác, người bảo trì phải sinh và ký lại danh sách object River cho minor mới, cập nhật golden object DB, rồi mới đổi `RequiredRiverPrefix`. Ứng dụng không tự nâng River vượt bản flowkit đang ghim.
- PostgreSQL 16 là bản được kiểm thử.

## 1. Sao lưu trước

Trên mọi database không phải đồ bỏ đi, sao lưu schema trước khi migrate:

```sh
pg_dump -n <schema> -Fc -f <schema>-$(date +%Y%m%d%H%M).dump
```

Kiểm tra file dump đọc được: `pg_restore -l <file> | head`.

## 2. Kết nối

`flowkit migrate` lấy chuỗi kết nối theo thứ tự:

1. `--dsn` — **không được chứa mật khẩu** (lộ qua `ps` và lịch sử shell). Có mật khẩu → lệnh từ chối, mã thoát 2.
2. Biến `FLOWKIT_DATABASE_URL`.
3. Biến chuẩn của libpq (`PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGSSLMODE`, ...) và `PGPASSWORD` hoặc `~/.pgpass`.

Lệnh không in chuỗi kết nối; lỗi chỉ nêu nguồn (`--dsn`, `FLOWKIT_DATABASE_URL` hoặc biến `PG*`).

## 3. Chạy migrate

```sh
export PGHOST=db.internal PGUSER=flowkit_admin PGDATABASE=app
flowkit migrate --schema flowkit
```

- Tên schema: chữ thường, số, `_`, tối đa 46 ký tự, không bắt đầu bằng số; cấm `public`, `information_schema`, `pg_*`. Tên sai bị từ chối trước khi chạm DB.
- Schema chưa tồn tại được tạo. Schema đã có object mà không có `flowkit_meta` bị từ chối (không phải schema của flowkit).
- Schema đã có dữ liệu flowkit và còn migration chưa áp → lệnh dừng, mã thoát 1, yêu cầu sao lưu rồi chạy lại với `--yes`:

  ```sh
  flowkit migrate --schema flowkit --yes
  ```

- Hai lệnh migrate chạy cùng lúc: một lệnh giữ khóa, lệnh kia chờ (tối đa 10 phút) rồi thấy không còn gì để áp. Chạy lại khi đã cập nhật là no-op.
- Mỗi migration flowkit chạy với `lock_timeout` bằng cùng giới hạn (mặc định 10 phút). Nếu một câu lệnh DDL phải chờ khóa bảng (ví dụ ứng dụng đang giữ transaction dài trên bảng đó) quá lâu, migration đó rollback và lệnh báo lỗi `55P03`; các migration trước đó vẫn giữ nguyên. Kết thúc transaction đang chặn rồi chạy lại.

Mã thoát: `0` thành công hoặc đã cập nhật; `1` lỗi khi chạy (kết nối, cần `--yes`, migration lỗi); `2` sai cách dùng (cờ, tên schema, `--dsn` có mật khẩu, chuỗi kết nối hỏng).

## 4. Kiểm tra version

Trong Go: `migrate.Status(ctx, pool, schema)` trả `SchemaVersion`, `MinLibSchema`, `RiverVersion`, `Pending`.

Bằng SQL:

```sql
SELECT k, v FROM <schema>.flowkit_meta ORDER BY k;          -- schema_version, min_lib_schema
SELECT line, version, created_at
FROM <schema>.river_migration ORDER BY line, version;       -- River đã áp tới đâu
```

`flowkit.New` chỉ chạy khi `min_lib_schema ≤ schema của thư viện ≤ schema_version`, mọi migration River đã áp, và tập object trong schema khớp danh sách nhúng trong bản build. Lệch → `ErrSchemaDrift` kèm gợi ý chạy `flowkit migrate`.

## 5. Migration River chạy dở

River áp mỗi migration trong một transaction riêng và ghi version vào `river_migration` sau từng bước, nên nếu lệnh bị ngắt giữa chừng (mất kết nối, bị kill):

1. Xem bước cuối đã áp: truy vấn `river_migration` ở mục 4.
2. Chạy lại `flowkit migrate --schema <schema>` (thêm `--yes` nếu schema có dữ liệu). Lệnh áp tiếp các bước River còn thiếu rồi tới migration flowkit.
3. Nếu khóa còn bị giữ bởi một phiên treo, lệnh mới chờ rồi báo lỗi khóa sau 10 phút. Tìm phiên giữ khóa:

   ```sql
   SELECT pid, state, xact_start, query FROM pg_stat_activity
   WHERE query ILIKE '%flowkit_meta%' AND pid <> pg_backend_pid();
   ```

   Chỉ dừng phiên đó (`SELECT pg_cancel_backend(<pid>)`) khi chắc chắn tiến trình migrate tương ứng đã chết.

Migration flowkit cũng mỗi file một transaction, ghi `schema_version` trong cùng transaction, nên không bao giờ dừng ở trạng thái nửa file.

## 6. Rolling upgrade

- Mỗi file migration flowkit khai báo `-- min_lib_schema: N`; runner ghi `flowkit_meta.min_lib_schema` = giá trị lớn nhất đã áp.
- Migration chỉ thêm (bảng, cột nullable, index) giữ nguyên `min_lib_schema`, nên instance bản cũ vẫn chạy trên schema mới trong lúc triển khai dần: migrate trước, rồi thay instance.
- Migration phá vỡ tương thích nâng `min_lib_schema`; instance cũ khi khởi động lại sẽ bị từ chối với `ErrSchemaDrift`. Khi đó phải dừng hết instance cũ trước khi migrate.
- Instance bản cũ trên schema mới hơn chấp nhận object lạ (do bản mới thêm), nhưng vẫn từ chối function, trigger, rule, kiểu dữ liệu hoặc giá trị mặc định của cột (`DEFAULT`, kể cả `CURRENT_TIMESTAMP`) không thuộc danh sách River đã ký. Bảng flowkit không dùng `DEFAULT`; ứng dụng luôn tự ghi giá trị.

## 7. Khôi phục

Khi migrate làm hỏng dữ liệu: dừng mọi instance, xóa schema hỏng và khôi phục từ bản sao lưu ở bước 1:

```sh
psql -c 'DROP SCHEMA <schema> CASCADE'
pg_restore -d <database> <file>.dump
```
