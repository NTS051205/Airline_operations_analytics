# Data Cleaning và chuẩn bị Cleaned Parquet

## 1. Mục tiêu: nhất quán mà không làm thay đổi quần thể phân tích

Dữ liệu BTS nguồn có flag dạng `"0.00"`, ngày ở dạng chuỗi và những NULL phù hợp với chuyến hủy/chuyển hướng. Phase 2 chuyển nguồn này thành dữ liệu có kiểu rõ ràng, nhưng giữ nguyên ý nghĩa nghiệp vụ và số bản ghi.

Đầu ra phải dùng được cho cả phân tích tỷ lệ trễ **trên completed flights** và tỷ lệ hủy **trên toàn bộ flights**. Vì vậy cleaning không được xóa cancelled/diverted chỉ để làm bảng đầy đủ hơn.

~~~mermaid
flowchart LR
    A[Raw BTS CSV] --> B[Kiểm tra nguồn và đọc string]
    B --> C[Chuẩn hóa blank và parse kiểu]
    C --> D[Phân loại flight_status]
    D --> E[Pre-write validation]
    E --> F[Parquet Snappy theo YEAR/MONTH]
    F --> G[Read-back validation]
~~~

Ý nghĩa trường ở [Data Dictionary](data_dictionary.md); căn cứ không deduplicate hay loại outlier ở [Data Quality Report](data_quality_report.md). Tài liệu này giải thích các transformation thực sự triển khai và mức đảm bảo của read-back.

## 2. Đầu vào, đầu ra và trách nhiệm code

| Thành phần | Vai trò |
| --- | --- |
| `data/raw/reporting_carrier_ontime_2025_01.csv`, `..._02.csv`, `..._03.csv` | Nguồn raw; chỉ đọc, không ghi đè. |
| `artifacts/phase1/profile_summary.json` | Baseline counts theo tháng và trạng thái từ Phase 1. |
| `data/processed/flights_cleaned/` | Dataset Parquet có 38 cột, partition theo `YEAR/MONTH`, compression Snappy. |
| `artifacts/phase2/validation_summary.json` | Counts, schema, cast/NULL issues, checks và trạng thái từng giai đoạn. |
| [ingestion.py](../src/airline_analytics/ingestion.py), [schema.py](../src/airline_analytics/schema.py) | Tái sử dụng kiểm tra tên/kích thước/header/checksum và đọc CSV; không khai báo lại schema nguồn. |
| [cleaning.py](../src/airline_analytics/cleaning.py) | `normalized_text`, `safe_binary_flag`, `typed_column`, `clean_flights`. |
| [validation.py](../src/airline_analytics/validation.py) | Thu thập counts, kiểm tra kiểu, NULL mới phát sinh, domain và nhãn từng dòng. |
| [build_cleaned_parquet.py](../scripts/build_cleaned_parquet.py) | Điều phối baseline → transform → kiểm tra → ghi → đọc lại → báo cáo. |

Baseline được đọc từ JSON, không hardcode counts thành phép sửa dữ liệu cho khớp. Đường dẫn mặc định là đường dẫn project, không buộc người dùng khác phải có ổ đĩa Windows giống máy phát triển.

## 3. Chuẩn hóa kiểu và missing values

### 3.1. Chuẩn hóa văn bản trước khi chuyển kiểu

`normalized_text` trim khoảng trắng đầu/cuối và đổi NULL hoặc chuỗi rỗng sau trim thành NULL. Không sửa nội dung bên trong tên thành phố, không thay mã hãng, không tự điền giá trị thiếu.

NULL nguồn được giữ lại nếu phù hợp nghiệp vụ. “Không phát sinh NULL do cast” khác với “không còn NULL trong dữ liệu”.

| Nhóm | Transformation thực tế | Lý do |
| --- | --- | --- |
| `YEAR`, `MONTH`, `DAY_OF_WEEK` và numeric IDs | Cast `IntegerType` | So sánh, nhóm và hợp đồng kiểu nhất quán. ID vẫn là định danh, không phải số đo. |
| `FL_DATE` | `to_timestamp` với `M/d/yyyy h:mm:ss a` rồi `to_date` | Parse rõ định dạng nguồn, tránh phụ thuộc suy luận ngày tự động. |
| `DEP_DELAY`, `ARR_DELAY`, `ARR_DELAY_NEW`, `DISTANCE` và năm cause fields | Cast `DoubleType` | Giữ số đo và dấu của delay; hỗ trợ aggregate numeric. |
| Bốn flag | Parse double → chỉ nhận 0/1 → cast integer | Giữ `"0.00"`, `"1.00"` hợp lệ; không vô tình cắt phần lẻ thành nhãn hợp lệ. |
| Codes, flight number, tail number, tên thành phố | String sau chuẩn hóa blank | Bảo toàn định danh và văn bản. |
| Bốn trường HHmm | String, trim only | Không mất leading zeros; không tự diễn giải giờ thành timestamp. |

Ví dụ minh họa logic, không phải lỗi đã quan sát: `"0.00" → 0` là đúng; `"0.50"` không được cast trực tiếp thành 0. `safe_binary_flag` chỉ trả về integer khi số parse được thuộc `{0,1}`. Giá trị không hợp lệ trở thành NULL trong biểu thức transformation và được validation nhận diện là vi phạm, khiến job dừng trước khi ghi.

### 3.2. Đo NULL mới phát sinh

Với mỗi cột cần cast, báo cáo lưu:

- `source_nulls`: số NULL sau chuẩn hóa văn bản.
- `target_nulls`: số NULL sau chuyển kiểu.
- `introduced_nulls`: số dòng nguồn có giá trị nhưng đích thành NULL.

Điều kiện thứ ba là kiểm tra ở từng dòng, không chỉ lấy chênh lệch hai tổng. Kết quả đã lưu: `unexpected_cast_nulls_total = 0`.

Không điền `ARR_DEL15` thiếu thành 0; không điền cancellation code; không `COALESCE` delay causes về 0. Các thao tác này sẽ biến “không có/không áp dụng” thành một kết quả nghiệp vụ không được nguồn xác nhận.

### 3.3. Bảo toàn HHmm

`"0015"` phải giữ nguyên string bốn ký tự; `"2400"` được giữ nguyên, không đổi ngày bay. Validation chấp nhận HHmm hợp lệ trong `0000–2359` hoặc `2400`. Một chuỗi thiếu chữ số không được tự zero-pad cho qua kiểm tra.

Đây không phải bước dựng timestamp đa múi giờ. Nếu sau này cần tính thời gian bay từ actual times, phải bổ sung ngữ cảnh ngày/múi giờ phù hợp, không trừ trực tiếp hai chuỗi hoặc hai số HHmm.

## 4. Business rules và bảo toàn grain

### 4.1. Phân loại trạng thái

~~~text
CANCELLED = 0, DIVERTED = 0  → completed
CANCELLED = 1, DIVERTED = 0  → cancelled
CANCELLED = 0, DIVERTED = 1  → diverted
Mọi tổ hợp còn lại          → invalid
~~~

`flight_status` giúp downstream dùng một định nghĩa nhất quán. Nếu còn `invalid`, validation thất bại; pipeline không âm thầm chuyển nhóm này thành completed hay loại bỏ.

### 4.2. Kiểm tra nhãn arrival từng dòng

Hợp đồng của dataset hiện tại:

- Completed phải có `ARR_DEL15` bằng 0 hoặc 1.
- Non-completed phải có `ARR_DEL15 IS NULL`.
- Giá trị không NULL ngoài 0/1 là vi phạm domain.

Kết quả từng nhóm vi phạm đều bằng 0. Điều này mạnh hơn phép đối chiếu đơn thuần `missing ARR_DEL15 = cancelled + diverted`: hai tổng có thể bằng nhau ngay cả khi một số dòng bị gắn sai nhóm.

Không suy rộng rule nhãn này thành “mọi actual time của cancelled/diverted đều NULL”; các trường khởi hành/đến có coverage khác nhau.

### 4.3. Những thao tác chủ động không làm

- **Không deduplicate:** Phase 1 ghi 0 exact duplicate groups và 0 candidate-key collisions. Không cần xóa dữ liệu; một collision tương lai cũng cần điều tra trước.
- **Không cap/clip outlier:** giữ `ARR_DELAY = 3.407` phút vì chưa có bằng chứng sai. Giá trị âm được giữ vì biểu thị đến sớm.
- **Không loại cancelled/diverted:** cần chúng cho cancellation/diversion rates và đối chiếu tổng.
- **Không imputation:** không có cơ sở nghiệp vụ để điền nhãn, nguyên nhân hay tail number.
- **Không tạo surrogate key, star schema hoặc features ML:** không cần cho mục tiêu cleaning này.

`clean_flights` chỉ select/transform và bổ sung trạng thái, không filter dòng. Mỗi dòng cleaned tương ứng một dòng raw. `_source_file` giữ tên file nguồn để truy vết; `_source_year` và `_source_month` chỉ dùng nội bộ rồi bỏ khỏi output. Lineage theo file không thay thế khóa duy nhất của một chuyến.

## 5. Vì sao chọn Parquet, Snappy và partition theo tháng?

Parquet lưu dữ liệu theo cột và mang schema. Spark SQL có thể đọc các cột cần thiết mà không phải parse lại chuỗi CSV. Snappy là lựa chọn nén của pipeline nhằm cân bằng chi phí nén/giải nén và dung lượng; project **chưa có benchmark** so sánh codecs hoặc chứng minh mức tăng tốc cụ thể.

`partitionBy("YEAR", "MONTH")` tạo cấu trúc thư mục theo kỳ báo cáo, phù hợp truy vấn lọc tháng. Spark khôi phục các cột partition khi đọc từ thư mục gốc. Thứ tự cột sau read-back có thể khác vì `YEAR`/`MONTH` được đưa về cuối; schema check so sánh tên và kiểu, không yêu cầu thứ tự hoặc metadata nullable giống hệt.

Code dùng `repartition(3, "YEAR", "MONTH")` trước khi ghi để gom dữ liệu theo khóa tháng trong phạm vi MVP nhỏ. Đây là hash partition của Spark, không có nghĩa ba khóa tháng chắc chắn đi vào ba task khác nhau. Không mô tả số task hay số part files quan sát được như benchmark nếu chưa có evidence tương ứng.

Trade-off: partition quá nhỏ dễ sinh nhiều file nhỏ; partition quá lớn có thể làm đọc dư dữ liệu. Với ba tháng và mục tiêu local, partition tháng rõ ràng hơn việc thêm hãng/sân bay/ngày vào đường dẫn.

### An toàn output và tài nguyên

- Writer dùng `errorifexists`; cả output directory và validation JSON hiện có đều bị từ chối ghi đè.
- Không append âm thầm và không tự xóa thư mục dở dang khi job lỗi.
- Nếu check thất bại, ghi evidence lỗi khi có thể và raise exception, không sửa dữ liệu để đạt PASS.
- Raw và cleaned được persist bằng `MEMORY_AND_DISK`, giải phóng khi không còn cần; `spark.stop()` nằm trong `finally`.
- Thiết lập chạy đã được owner xác nhận: Spark `local[2]`, driver memory 4g trên máy RAM 12 GB; đây là môi trường local, không phải kiến trúc production.

## 6. Kiểm chứng trước và sau khi ghi

### 6.1. Pre-write: 15/15 checks PASS trong evidence

| Nhóm | Kiểm tra đã triển khai |
| --- | --- |
| Bảo toàn population | Raw không rỗng; raw = cleaned; counts theo tháng không đổi; counts trạng thái không đổi. |
| Trạng thái | Tổng các trạng thái bằng row count; không có invalid. |
| Hợp đồng dữ liệu | 38 tên/kiểu đúng; không introduced cast NULL hoặc lỗi HHmm; binary domains đúng; HHmm output hợp lệ; lineage hiện diện. |
| Nhãn | `ARR_DEL15` tuân thủ rule từng dòng. |
| Baseline Phase 1 | Total, month counts và status counts khớp kết quả profiling đã lưu. |

### 6.2. Read-back: 9/9 checks PASS trong evidence

Sau ghi, script đọc Parquet từ output và so sánh count, tháng, trạng thái với cleaned trước ghi. Đồng thời kiểm tra schema, status reconciliation, nhãn arrival, binary domains, HHmm và lineage.

**Giới hạn quan trọng:** Phase 2 không so sánh mọi giá trị của mọi dòng bằng `EXCEPT ALL` hai chiều. Counts bằng nhau không tự chứng minh toàn bộ nội dung giống nhau. Cam kết read-back của phase này là schema, counts và các rules nêu trên. Phase 3A có phép so sánh toàn giá trị cho các bảng aggregate; không gán ngược kiểm chứng đó cho Phase 2.

### 6.3. Kết quả trực tiếp từ JSON

Nguồn: `artifacts/phase2/validation_summary.json`, `generated_at_utc = 2026-10-10T04:05:06.869839+00:00`.

| Chỉ số | Raw | Cleaned | Parquet read-back |
| --- | ---: | ---: | ---: |
| Tổng dòng | 1.645.503 | 1.645.503 | 1.645.503 |
| Tháng 01 | 539.747 | 539.747 | 539.747 |
| Tháng 02 | 504.884 | 504.884 | 504.884 |
| Tháng 03 | 600.872 | 600.872 | 600.872 |
| Completed | 1.611.046 | 1.611.046 | 1.611.046 |
| Cancelled | 30.640 | 30.640 | 30.640 |
| Diverted | 3.817 | 3.817 | 3.817 |
| Invalid status | 0 | 0 | 0 |

| Kết quả | Giá trị |
| --- | --- |
| `overall_status`; pre-write; write; read-back | PASS |
| Số cột cleaned và read-back | 38 |
| Introduced cast NULLs | 0 |
| Vi phạm `ARR_DEL15` từng dòng | 0 ở cả ba loại kiểm tra |
| Failed checks; errors | Danh sách rỗng |
| Schema thiếu/thừa cột hoặc sai kiểu | Không phát hiện |

Số lượng 15 và 9 được đếm từ danh sách checks đã lưu, không phải các tests vừa chạy lại.

Owner đã xác nhận Parquet smoke test và full pipeline exit code 0 trên Windows 11, PySpark 3.5.8 sau khi cấu hình Hadoop 3.3.4 Windows utilities. JSON Phase 2 xác nhận write/read-back PASS; không dùng nó để khẳng định đã tự kiểm tra phiên bản binary hệ điều hành. Không cần commit Hadoop binaries vào repository.

[smoke_test_parquet.py](../scripts/smoke_test_parquet.py) dùng ba dòng mẫu với `"0015"`, `"2400"` và NULL để kiểm tra ghi/đọc string và partition. Đây là kiểm thử dữ liệu tổng hợp đã được owner chạy, không phải thống kê ba dòng đại diện toàn BTS.

## 7. Cleaning khác ML preprocessing như thế nào?

| Phase 2 đã triển khai | Phase 5 chưa triển khai |
| --- | --- |
| Chuẩn hóa kiểu, blank/NULL; xác định trạng thái; bảo toàn bản ghi và lineage | Chọn target population, prediction time và features không leakage |
| Ghi dữ liệu phân tích có schema nhất quán | Chia train/test theo chiến lược phù hợp |
| Giữ các giá trị nghiệp vụ, không tùy tiện điền missing | Encoding categorical và các transformation cần fit chỉ trên training data |
| Kiểm chứng hợp đồng dữ liệu và Parquet | Đánh giá mô hình bằng kết quả thực nghiệm chưa có |

Một scaler/encoder fit trên cả dataset có thể rò rỉ thông tin từ test vào train. Không cần đưa các bước đó vào cleaning để làm pipeline “đầy đủ hơn”.

## 8. Ý nghĩa và giới hạn

Phase 2 đã hoàn thành implementation và runtime validation; code/documentation đã được owner xác nhận push ở các commit `6c50b00` và `4a9e907`. Đây là thông tin lịch sử đã cung cấp, không phải Git vừa được thực thi để xác minh lại.

Các checks PASS cho phép dùng cleaned dataset làm đầu vào analytics với các hợp đồng đã định nghĩa. Chúng không chứng minh nguồn hoàn hảo, không xác minh mọi outlier và không cung cấp business KPI hay ML performance.

Dashboard ba trang hiện đã được xây dựng ở giai đoạn sau. Câu “Phase 2 không tạo Power BI hoặc ML output” chỉ giới hạn trách nhiệm pipeline này, không mô tả dashboard hiện tại là chưa bắt đầu.

## 9. Câu hỏi phỏng vấn gắn với cleaning

- **Vì sao không cast flag trực tiếp sang integer?** Cần chấp nhận biểu diễn `0.00/1.00` nhưng từ chối giá trị ngoài domain, không để phép cắt phần lẻ che lỗi.
- **Vì sao cần read-back khi write không báo lỗi?** Ghi thành công chưa chứng minh schema/counts/rules của dữ liệu đọc lại đúng.
- **Partition theo tháng có luôn tối ưu không?** Không; phù hợp grain truy vấn và phạm vi hiện tại, nhưng chưa có benchmark và cần xem lại khi quy mô thay đổi.

## Những điểm cần ghi nhớ

- Cleaning chuẩn hóa biểu diễn, không thay đổi population hoặc tự viết lại kết quả vận hành.
- NULL có điều kiện được giữ; 0 introduced NULL không có nghĩa dataset không có NULL.
- Raw, cleaned và read-back đều có 1.645.503 dòng; cleaned có 38 cột.
- Parquet Snappy theo YEAR/MONTH đã ghi/đọc thành công; không overwrite output.
- Phase 2 kiểm chứng hợp đồng hữu hạn và chưa thực hiện ML preprocessing.
