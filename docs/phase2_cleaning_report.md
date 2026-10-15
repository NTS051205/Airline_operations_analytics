# Cleaning và kiểm chứng Parquet

## 1. Mục tiêu

Chuyển CSV BTS thành dữ liệu phân tích có kiểu nhất quán, bảo toàn ý nghĩa nghiệp vụ và giải thích được mọi chênh lệch số dòng. Phạm vi hiện tại là **January–December 2025**.

Raw CSV → kiểm tra nguồn → phân loại Accepted/Rejected → chuẩn hóa kiểu → pre-write validation → Parquet → read-back validation.

Một dòng completed thiếu arrival không được giả định là on-time. Pipeline tách dòng đó để truy vết, đồng thời giữ cancelled/diverted cho cancellation/diversion rates. Định nghĩa trường và căn cứ chất lượng nằm ở [Data Dictionary](data_dictionary.md) và [Data Quality Report](data_quality_report.md).

## 2. Input, output và trách nhiệm

| Thành phần | Đường dẫn / vai trò |
| --- | --- |
| Raw | 12 CSV trong `data/raw/`, chỉ đọc |
| Profiling baseline | `artifacts/phase1_2025/profile_summary.json` |
| Diagnostic đã review | `artifacts/phase1_2025/arrival_missing_diagnostic.json`, truyền bằng `--approved-diagnostic` |
| Accepted Parquet | `data/processed/flights_cleaned_2025_reviewed/` |
| Rejected Parquet | `artifacts/phase2_2025_reviewed/rejected_records/` |
| Validation evidence | `artifacts/phase2_2025_reviewed/validation_summary.json` |
| Mốc regression Q1 | `data/processed/flights_cleaned/`, chỉ đọc |

[ingestion.py](../src/airline_analytics/ingestion.py) và [schema.py](../src/airline_analytics/schema.py) nhận diện file và đọc schema string. [cleaning.py](../src/airline_analytics/cleaning.py) định nghĩa transformation/phân loại; [validation.py](../src/airline_analytics/validation.py) kiểm tra hợp đồng; [build_cleaned_parquet.py](../scripts/build_cleaned_parquet.py) điều phối.

[diagnose_arrival_missing.py](../scripts/diagnose_arrival_missing.py) là tiện ích tái tạo diagnostic, không phải bước sửa dữ liệu. Cleaning yêu cầu evidence đã review và kiểm tra nguồn/lineage của diagnostic; vì vậy script này có vai trò trong khả năng tái hiện kết quả.

## 3. Business rules

### Accepted và Rejected dùng một điều kiện

`classify_arrival_records` tính `missing_fields` từ các trường nguồn sau chuẩn hóa blank. Một dòng có `CANCELLED = 0 AND DIVERTED = 0` và thiếu **bất kỳ** `ARR_TIME`, `ARR_DELAY`, `ARR_DEL15` nhận:

~~~text
rejection_reason = completed_missing_essential_arrival
~~~

`split_arrival_records` dùng hai nhánh bù nhau: rejection_reason NULL → accepted; khác NULL → rejected. Không dùng hai bộ điều kiện độc lập hoặc join dễ nhân/mất dòng.

Rejected output giữ 36 trường raw ở dạng string/NULL đã parse, `_source_file`, `_source_year`, `_source_month`, `rejection_reason` và mảng `missing_fields`. Đây là dữ liệu truy vết, không phải sửa raw CSV.

`ARR_DELAY_NEW` không nằm trong điều kiện reject bắt buộc. Completed thiếu trường này, đặc biệt chỉ thiếu trường này, được đếm riêng; không tự điền hoặc loại.

**Lỗi cast không được che bằng rejection:** checks chuyển kiểu chạy trên toàn bộ raw. Chuỗi arrival hiện diện nhưng không parse được là lỗi validation, không phải một NULL nguồn được phép âm thầm loại.

### Trạng thái chuyến bay

| CANCELLED | DIVERTED | flight_status |
| ---: | ---: | --- |
| 0 | 0 | completed |
| 1 | 0 | cancelled |
| 0 | 1 | diverted |
| Tổ hợp khác, kể cả NULL | | invalid |

Không đổi completed thành diverted/cancelled để xử lý missing. Invalid phải làm validation thất bại. Cancelled/diverted được giữ trong accepted dataset dù các arrival fields có NULL phù hợp nghiệp vụ.

### Các thao tác không áp dụng

- Không `dropDuplicates` hoặc tự chọn một dòng theo candidate key.
- Không cap/clip signed delay hoặc loại cực trị theo ngưỡng tùy ý.
- Không điền arrival labels, cancellation code hoặc cause minutes bằng 0.
- Không tạo surrogate key, warehouse hoặc transformation ngoài nhu cầu analytics.

## 4. Chuẩn hóa kiểu, NULL và lineage

| Nhóm | Cách xử lý | Lý do |
| --- | --- | --- |
| Blank/text | Trim đầu/cuối; blank thành NULL | Thống nhất biểu diễn thiếu, không sửa nội dung tên/mã |
| Ngày | Parse `M/d/yyyy h:mm:ss a`, lấy DateType | Không phụ thuộc inferSchema |
| Năm/tháng/thứ và numeric IDs | IntegerType | Hợp đồng kiểu rõ; ID không trở thành số đo |
| Delay, distance, cause minutes | DoubleType | Giữ dấu và giá trị đo |
| Bốn flags | Parse double; chỉ nhận 0/1; chuyển integer | Chấp nhận `"0.00"`, không cắt `"0.50"` thành 0 |
| HHmm | String, trim only | Giữ leading zeros; không tự zero-pad hay đổi `2400` sang timestamp |
| Lineage/status | Giữ `_source_file`, thêm `flight_status` | Truy vết nguồn và nhất quán population |

`clean_flights` tự nó không filter; runner truyền **accepted_raw** vào hàm này. Vì vậy không thể kết luận toàn pipeline bảo toàn mọi dòng trong cleaned chỉ bằng việc đọc hàm transform.

Accepted schema vẫn **38 cột**: 36 trường nguồn đã typed và hai cột bổ sung. `_source_year/_source_month` không nằm trong schema accepted. Kiểm tra introduced NULL xét từng dòng nguồn có giá trị nhưng đích NULL, không chỉ lấy chênh lệch hai tổng.

## 5. Storage và tài nguyên

Accepted dùng **Parquet, Snappy**, partition `YEAR/MONTH`. Parquet giữ schema và cho phép đọc cột cần thiết; partition tháng phù hợp bộ lọc kỳ báo cáo. Chưa có benchmark để khẳng định codec hoặc tốc độ tối ưu.

Writer hiện dùng `repartition(3, "YEAR", "MONTH")`. Ba là số Spark hash partitions cho bước ghi, **không phải giới hạn ba tháng** và không cam kết mỗi tháng có một task. Rejected dùng một part output nhỏ, không partition.

Raw/accepted/rejected được persist khi cần; raw được giải phóng trước ghi, accepted được giải phóng trước read-back. Máy chạy local có RAM 12 GB; cấu hình full-year được dùng là local[1], driver 3g. Đây không phải performance benchmark.

Output đã tồn tại bị từ chối; không overwrite/append âm thầm. Không tự xóa partial output khi job lỗi. Các output full-year tách biệt mốc Q1.

## 6. Validation và kết quả

Nguồn trực tiếp: `artifacts/phase2_2025_reviewed/validation_summary.json`.

| Chỉ số | Raw | Accepted | Rejected |
| --- | ---: | ---: | ---: |
| Tổng | 7.001.619 | 7.001.618 | 1 |
| Completed | 6.879.485 | 6.879.484 | 1 |
| Cancelled | 102.876 | 102.876 | 0 |
| Diverted | 19.258 | 19.258 | 0 |
| Invalid | 0 | 0 | 0 |
| January–March | 1.645.503 | 1.645.503 | 0 |
| September | 562.439 | 562.438 | 1 |

Các tháng khác không đổi số dòng. Evidence ghi **PASS**, pre-write **24/24**, read-back **17/17**, errors và failed checks rỗng.

### Pre-write

- `raw = accepted + rejected` tổng thể, theo tháng, trạng thái và tổ hợp file–tháng–status.
- Raw counts/status khớp profiling; accepted status sum khớp accepted row count.
- Đúng schema 38 cột, binary domains, HHmm, lineage và ARR_DEL15 row-level rules.
- **0 introduced cast NULL**, **0 invalid status**; accepted không còn completed thiếu ba trường arrival thiết yếu.
- Rejected schema/counts/nội dung khớp diagnostic đã review; không hardcode số 1 trong reusable transformation.
- Completed chỉ thiếu ARR_DELAY_NEW: **0**; số liệu trường này vẫn được báo cáo riêng.
- Q1 accepted so sánh schema và toàn bộ giá trị với MVP; Q1 rejected bằng 0.

### Read-back

Đọc lại cả accepted và rejected Parquet: đối chiếu schema, counts, file/month/status, missingness và các domain/label rules. Rejected so sánh toàn bộ giá trị; Q1 accepted tiếp tục đối chiếu toàn bộ 38 cột với mốc MVP bằng multiset comparison.

**Giới hạn:** không có full-row comparison của toàn bộ accepted cả năm trước/sau ghi. Không diễn giải counts/schema khớp thành bằng chứng mọi giá trị đã được so sánh. Kiểm tra toàn giá trị áp dụng cho rejected và Q1 regression; analytics có kiểm tra aggregate read-back riêng.

Mốc Q1 lịch sử tại `artifacts/phase2/validation_summary.json` ghi 15/15 pre-write và 9/9 read-back; đó là bộ checks cũ, không phải counts của lần chạy cả năm.

## 7. Ý nghĩa và giới hạn

Parquet write/read đã chạy thành công trên Windows với cấu hình Hadoop utilities đã dùng trong dự án. Native binaries và dữ liệu lớn không cần commit; hướng dẫn môi trường ở [README](../README.md).

Quy tắc rejection làm population analytics khác raw một dòng. KPI phải dùng accepted counts và công bố khác biệt này; không tuyên bố bảo toàn tất cả raw trong analytical dataset. Raw cộng rejected evidence vẫn cho phép truy vết quyết định.

PASS xác nhận các hợp đồng đã kiểm tra, không chứng minh mọi dữ liệu nguồn đúng, không xác minh nguyên nhân thiếu arrival và không đo tác động tài chính.

## Những điểm cần ghi nhớ

- Bảo toàn bằng `Raw = Accepted + Rejected`, không còn `Raw = Cleaned` cho cả năm.
- Rejection dựa trên ba trường thiết yếu, không dựa trên số hiệu chuyến cụ thể.
- Cast/domain checks vẫn chặn lỗi; rejection không phải cách bỏ validation.
- Giữ raw, source lineage, signed delays và NULL có điều kiện.
- Q1 được bảo toàn bằng regression; không ghi đè evidence lịch sử.
