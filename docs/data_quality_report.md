# Data Profiling và chất lượng dữ liệu BTS 2025

## 1. Mục tiêu và phạm vi

Profiling xác định cấu trúc, miền giá trị, missingness và tính nhất quán của dữ liệu trước khi làm sạch. Với dữ liệu chuyến bay, NULL ở chuyến hủy khác NULL ở chuyến completed: điền tất cả bằng 0 sẽ biến nhãn chưa biết thành “đúng giờ”; xóa mọi dòng có NULL lại làm mất nhiều bản ghi hợp lệ.

Phạm vi chính là **January–December 2025**, gồm 12 CSV, 36 cột và **7.001.619 raw records**. Profiling chỉ đọc nguồn và xuất thống kê; không sửa dữ liệu. Ý nghĩa trường nằm ở [Data Dictionary](data_dictionary.md), quyết định Accepted/Rejected ở [Cleaning Report](phase2_cleaning_report.md).

### Nguồn bằng chứng

| Evidence local | Trách nhiệm |
| --- | --- |
| `artifacts/phase1_2025/profile_summary.json` | Counts, missingness, cast failures, duplicates, ranges và anomaly rules cả năm |
| `artifacts/phase1_2025/arrival_missing_diagnostic.json` | Chi tiết một completed flight thiếu arrival |
| `artifacts/phase2_2025_reviewed/validation_summary.json` | Kết quả áp dụng quyết định chất lượng và read-back |
| `artifacts/phase1/profile_summary.json` | Baseline lịch sử January–March; không thay cho kết quả cả năm |

Các số liệu dưới đây đọc từ evidence hiện có. Không có lần chạy profiling mới khi biên soạn tài liệu.

## 2. Các lớp kiểm tra được triển khai

| Lớp | Cách kiểm tra | Điều chứng minh và giới hạn |
| --- | --- | --- |
| Nhận diện file | Tên, kích thước byte, header đúng thứ tự và SHA-256 khớp metadata trong `schema.py` | Xác định đúng export đã review; không chứng minh nội dung đúng nghiệp vụ hoặc xác thực nguồn BTS bằng chữ ký |
| Parse CSV | Schema string, `FAILFAST`, lineage theo file/tháng | Giữ biểu diễn nguồn, phát hiện lỗi parse; không suy luận kiểu từ một phần mẫu |
| Khả năng cast | Nguồn không rỗng nhưng cast thành NULL; ngày dùng định dạng tường minh | Phát hiện chuyển đổi thất bại; không thay thế domain/finite-value checks |
| Missingness | NULL hoặc blank sau trim, theo cột/file và trạng thái | Phân biệt thiếu có điều kiện với thiếu cần điều tra |
| Duplicates | Toàn bộ 36 cột raw và khóa ứng viên | Phát hiện nhóm lặp theo định nghĩa đã chọn, không tự cho phép xóa |
| Anomalies | Miền giá trị, quan hệ lịch/nguồn, status, nhãn delay, HHmm và cause minutes | Kiểm chứng hợp đồng hữu hạn; không xác minh từng sự kiện vận hành |

[ingestion.py](../src/airline_analytics/ingestion.py) kiểm tra nguồn và đọc CSV; [profiling.py](../src/airline_analytics/profiling.py) thu thống kê; [quality_checks.py](../src/airline_analytics/quality_checks.py) phụ trách duplicates và anomalies. [profile_raw_data.py](../scripts/profile_raw_data.py) điều phối và ghi JSON.

Counts/missingness/status được tổng hợp theo file rồi cộng counts, không trung bình các tỷ lệ thiếu. Cache `MEMORY_AND_DISK` hỗ trợ tái sử dụng dữ liệu; chưa có benchmark để lượng hóa lợi ích.

## 3. Baseline raw cả năm

| Tháng 2025 | Raw records |
| --- | ---: |
| 01 | 539.747 |
| 02 | 504.884 |
| 03 | 600.872 |
| 04 | 583.950 |
| 05 | 605.648 |
| 06 | 611.575 |
| 07 | 631.428 |
| 08 | 602.378 |
| 09 | 562.439 |
| 10 | 605.844 |
| 11 | 570.550 |
| 12 | 582.304 |
| **Tổng** | **7.001.619** |

| Trạng thái raw | Số dòng |
| --- | ---: |
| Completed | 6.879.485 |
| Cancelled | 102.876 |
| Diverted | 19.258 |
| Invalid flags | 0 |

Nguồn trực tiếp: `row_counts`, `flight_status`; tổng status khớp row count. Đây là **raw baseline**: completed của cleaned giảm một dòng theo quyết định tại mục 5.

## 4. Missingness, duplicates và anomalous values

### Missingness theo population

| Trường raw | Missing count | Diễn giải |
| --- | ---: | --- |
| `TAIL_NUM` | 12.501 | Chưa xác định nguyên nhân thiếu; không tự điền |
| `DEP_TIME` | 98.170 | Coverage actual time phụ thuộc trạng thái |
| `DEP_DELAY`, `DEP_DEL15` | 98.591 mỗi cột | Không giả định cùng missing pattern với DEP_TIME |
| `ARR_TIME` | 104.608 | Cần kiểm tra completed riêng |
| `ARR_DELAY`, `ARR_DELAY_NEW`, `ARR_DEL15` | 122.135 mỗi cột | 122.134 non-completed và một completed thiếu arrival |
| `CANCELLATION_CODE` | 6.898.743 | Khớp số không hủy; NULL ở nhóm này không mặc định là lỗi |
| Mỗi delay-cause field | 5.466.981 | Thiếu trên toàn nguồn không thể diễn giải như tỷ lệ lỗi |
| Các cột còn lại | 0 | Theo định nghĩa missing đang dùng |

Nguồn trực tiếp: `missing_values.overall`. Quan hệ `102.876 + 19.258 + 1 = 122.135` là phép tính từ counts, không đủ thay kiểm tra từng dòng. Diagnostic xác nhận một dòng completed thiếu đồng thời ba trường thiết yếu.

Coverage từng cause trong tập delayed completed được kiểm chứng riêng ở [phân tích bổ sung](phase3b_findings.md). Không gán kiểm chứng downstream đó thành một phép kiểm tra đã chạy ở profiling.

### Duplicate metrics

Exact duplicates so sánh 36 cột raw, không đưa metadata nguồn vào khóa. Khóa ứng viên là:

~~~text
FL_DATE, OP_CARRIER_AIRLINE_ID, OP_CARRIER_FL_NUM,
ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID, CRS_DEP_TIME
~~~

| Chỉ số | Exact duplicate | Candidate-key collision |
| --- | ---: | ---: |
| Groups: nhóm có ít nhất hai dòng | 0 | 0 |
| Rows in groups: toàn bộ dòng trong nhóm | 0 | 0 |
| Excess rows: tổng count − 1 | 0 | 0 |

Không có dòng thiếu thành phần khóa. Ví dụ minh họa: ba bản sao tạo 1 group, 3 rows in groups, 2 excess rows. Candidate key không phải khóa chính chính thức; collision tương lai phải được review, không tự giữ một dòng bất kỳ.

### Cast và anomaly rules

- **0 cast failures** ở các cột được kiểm tra.
- **0 violations ở 28 confirmed-invalid rules:** năm/tháng/thứ và ngày/nguồn khớp; flags đúng miền; distance dương; HHmm hợp lệ; nhãn 15 phút khớp delay khi có cả hai; `ARR_DELAY_NEW = max(ARR_DELAY, 0)` trong tolerance; cause minutes không âm.
- Trong **11 suspicious business rules**, ba checks có giá trị 1: completed thiếu ARR_TIME, ARR_DELAY và ARR_DEL15. Diagnostic xác nhận đó là **cùng một dòng**, không phải ba chuyến.
- Các suspicious checks còn lại bằng 0: distance thiếu, origin bằng destination, completed thiếu giờ đi, cancellation code không phù hợp trạng thái và delayed completed thiếu cả năm causes.

“Confirmed invalid” nghĩa là vi phạm rule của project; “suspicious” yêu cầu review ngữ cảnh, không đồng nghĩa mọi NULL là lỗi thật.

### Outliers

`numeric_ranges.ARR_DELAY` ghi **−128 đến 4.336 phút** cho cả năm. Q1 lịch sử là −91 đến 3.407 phút. Âm là đến sớm; cực trị dương cần truy vết nhưng không đủ căn cứ gọi là sai.

Không cap, clip hoặc xóa theo ngưỡng tùy ý. Giữ cực trị tránh làm nhẹ mức gián đoạn quan sát được, nhưng signed average có thể bị ảnh hưởng và chưa giải thích nguyên nhân từng chuyến.

## 5. Quyết định cho completed thiếu arrival

[diagnose_arrival_missing.py](../scripts/diagnose_arrival_missing.py) xác nhận một dòng ngày **06/09/2025**, hãng **YX**, chuyến **5859**, **JFK → MVY**, thiếu ARR_TIME, ARR_DELAY, ARR_DEL15 và ARR_DELAY_NEW.

Quy tắc được áp dụng tổng quát, không hardcode chuyến này:

- Completed thiếu bất kỳ ARR_TIME/ARR_DELAY/ARR_DEL15 được tách vào rejected output.
- Giữ nguyên raw, status và giá trị thiếu để truy vết; không suy đoán arrival từ giờ khởi hành.
- Thiếu riêng ARR_DELAY_NEW chỉ được báo cáo, không tự loại. Evidence ghi 0 trường hợp chỉ thiếu trường này.
- Rejected phải khớp diagnostic đã review; phát sinh chênh lệch làm validation dừng.

Kết quả cleaning: **7.001.619 raw = 7.001.618 accepted + 1 rejected**. Q1 không có rejected và được bảo toàn qua regression. Quy tắc này giữ denominator arrival là completed **trong analytical dataset**, không phải toàn bộ raw completed.

## 6. Giới hạn và những điểm cần ghi nhớ

- Zero detected violations không có nghĩa dữ liệu hoàn hảo; full-year profiling thực tế có một trường hợp cần xử lý.
- Hash nhận diện export; cast/domain checks không xác minh danh mục mã hoặc mọi sự kiện ngoài BTS.
- Missingness phải gắn với trạng thái và điều kiện báo cáo; NULL khác 0.
- Exact duplicates dựa trên biểu diễn raw, không chứng minh không có duplicate ngữ nghĩa.
- Các kết quả một năm không chứng minh chất lượng nguồn ổn định qua nhiều năm.
- Giữ riêng raw và analytical population giúp kiểm toán KPI; không đổi baseline để che bản ghi bị loại.
