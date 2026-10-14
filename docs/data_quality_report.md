# Data Profiling và đánh giá chất lượng dữ liệu

## 1. Vì sao profiling phải đi trước cleaning?

Nguồn BTS có 34.457 giá trị thiếu ở `ARR_DEL15`. Nếu điền tất cả bằng 0, pipeline sẽ biến những chuyến không có nhãn đến nơi thành chuyến “không trễ”. Nếu xóa tất cả dòng có NULL, pipeline lại có thể mất cả chuyến hủy và các chuyến không cần báo cáo delay causes.

**Profiling** là quan sát cấu trúc, độ đầy đủ, miền giá trị và tính nhất quán của nguồn trước khi quyết định sửa dữ liệu. Phase 1 không làm cleaning, không loại dòng và không tính mô hình dự đoán. Mục tiêu là thiết lập baseline có thể kiểm chứng cho [Phase 2](phase2_cleaning_report.md) và mẫu số đúng cho [KPI](kpi_definitions.md).

Ý nghĩa từng trường và grain được trình bày ở [Data Dictionary](data_dictionary.md); tài liệu này tập trung vào chất lượng và giới hạn của các phép kiểm tra.

## 2. Đầu vào, bằng chứng và khả năng truy vết

Đầu vào là ba file `data/raw/reporting_carrier_ontime_2025_01.csv`, `..._02.csv` và `..._03.csv`. Cả ba có cùng 36 tên cột và thứ tự.

| Tháng | Kích thước byte ghi trong evidence | Raw records |
| --- | ---: | ---: |
| 01/2025 | 111.474.672 | 539.747 |
| 02/2025 | 104.767.501 | 504.884 |
| 03/2025 | 124.595.215 | 600.872 |
| Tổng | | 1.645.503 |

Nguồn kết quả trực tiếp: `artifacts/phase1/profile_summary.json`, được tạo lúc `2026-10-08T16:26:50.626115+00:00`. Đây là kết quả lần chạy đã lưu, không phải lần chạy mới khi biên soạn tài liệu.

[validate_input_files](../src/airline_analytics/ingestion.py) đối chiếu tên file, kích thước, header và SHA-256 với [schema.py](../src/airline_analytics/schema.py) trước khi Spark đọc dữ liệu. Ba checksum trong evidence có đủ 64 ký tự hexadecimal và khớp metadata đã ghi nhận; việc đọc tài liệu này không tính lại hash của raw CSV.

Checksum bảo đảm nhận diện đúng **nội dung byte đã chốt**, không chứng minh dữ liệu đúng nghiệp vụ. Một lần export lại hợp lệ vẫn có thể đổi checksum do thay đổi biểu diễn. Không tự sửa metadata cho khớp một file mới mà bỏ qua xác minh nguồn.

### Luồng triển khai

| Thành phần | Trách nhiệm |
| --- | --- |
| [profile_raw_data.py](../scripts/profile_raw_data.py) | Kiểm tra đầu vào, tạo SparkSession, cache dữ liệu, gọi profiling, ghi JSON và giải phóng tài nguyên. |
| [ingestion.py](../src/airline_analytics/ingestion.py) | Đọc CSV bằng schema string, chế độ `FAILFAST`, gắn thông tin file và tháng nguồn. |
| [profiling.py](../src/airline_analytics/profiling.py) | Counts, missingness, trạng thái, cast failures, miền giá trị và min/max. |
| [quality_checks.py](../src/airline_analytics/quality_checks.py) | Exact duplicates, candidate-key collisions và các anomaly rules. |

Đọc string trước giúp giữ lại cách biểu diễn nguồn để phát hiện lỗi thay vì để `inferSchema` tự chọn kiểu. Chế độ `FAILFAST` làm lỗi parse hiện ra, không phải cam kết kiểm tra mọi ngữ nghĩa của một dòng CSV.

## 3. Các lớp kiểm tra và ý nghĩa

### 3.1. Schema và khả năng chuyển kiểu

Schema validation kiểm tra tên, số lượng và thứ tự 36 cột. Kiểm tra cast đếm giá trị **không rỗng ở nguồn nhưng trở thành NULL khi chuyển kiểu**:

- Nhóm lịch/ID: thử cast integer.
- Độ trễ, distance và flag nguồn: thử cast double.
- `FL_DATE`: parse theo `M/d/yyyy h:mm:ss a`.

Kết quả: tất cả trường được kiểm tra có **0 cast failures**.

Giới hạn: cast thành công không chứng minh ID hợp lệ, mã được cấp bởi BTS, hoặc mọi phép chuyển đổi đều không làm mất độ chính xác. Kiểm tra cast-to-NULL cũng không thay thế kiểm tra NaN/infinity hay miền nghiệp vụ. Phase 1 kiểm tra flag dạng double; Phase 2 mới chuyển flag an toàn sang integer.

### 3.2. Missing values: tách thiếu có điều kiện khỏi thiếu cần điều tra

Missing được định nghĩa là NULL hoặc chuỗi chỉ gồm khoảng trắng sau `trim`. Counts và missingness được tính theo file; tổng toàn bộ lấy bằng **cộng counts của các file**, không lấy trung bình tỷ lệ thiếu giữa các tháng.

| Trường | Missing count trực tiếp từ JSON | Cách diễn giải |
| --- | ---: | --- |
| `TAIL_NUM` | 4.102 | Chưa có phân tích đủ để quy nguyên nhân thiếu cho từng trạng thái; không tự điền. |
| `DEP_TIME` | 29.560 | Giờ thực tế không nhất thiết có ở mọi chuyến hủy; kiểm tra riêng completed. |
| `DEP_DELAY`, `DEP_DEL15` | 29.671 mỗi cột | Số thiếu khác `DEP_TIME`; không giả định mọi trường khởi hành cùng missing pattern. |
| `ARR_TIME` | 31.252 | Không dùng một quy tắc “mọi chuyến non-completed đều thiếu mọi trường đến”. |
| `ARR_DELAY`, `ARR_DELAY_NEW`, `ARR_DEL15` | 34.457 mỗi cột | Bằng cancelled + diverted; phải xét thêm điều kiện từng dòng. |
| `CANCELLATION_CODE` | 1.614.863 | Khớp số chuyến không hủy; NULL ở nhóm này là có điều kiện nghiệp vụ. |
| Mỗi cột trong năm delay causes | 1.328.237 | Thiếu trên toàn nguồn không đồng nghĩa lỗi; cần xác định tập chuyến đủ điều kiện báo cáo. |
| Các cột còn lại | 0 | Không quan sát missing theo định nghĩa đã triển khai. |

Các đối chiếu sau là **phép tính từ counts trong JSON**, không phải metric độc lập mới:

- `30.640 + 3.817 = 34.457`: cancelled + diverted khớp missing `ARR_DEL15`.
- `1.645.503 − 30.640 = 1.614.863`: không hủy khớp missing cancellation code.
- `1.645.503 − 317.266 = 1.328.237`: ngoài tập delayed completed khớp missing của từng cause field.

Chỉ bằng nhau ở tổng chưa chứng minh cùng các dòng. Evidence Phase 1 có 0 completed thiếu `ARR_DEL15`, 0 cancelled thiếu cancellation code và 0 non-cancelled có cancellation code. Phase 2 bổ sung kiểm tra nhãn arrival ở từng dòng; Phase 3B xác nhận coverage của cause fields trên tập eligible. Không hồi tố các kiểm tra Phase 2–3B thành những gì đã chạy ở Phase 1.

### 3.3. Duplicates: ba số đo khác nhau

**Exact duplicate** so sánh toàn bộ 36 trường raw, không đưa metadata nguồn vào khóa. Nhờ vậy cùng một dòng ở hai file không bị che khuất bởi `_source_file` khác nhau. So sánh này giữ biểu diễn raw; không đồng nghĩa phát hiện mọi bản ghi tương đương về ngữ nghĩa sau chuẩn hóa.

**Candidate-key collision** dùng tổ hợp:

~~~text
FL_DATE, OP_CARRIER_AIRLINE_ID, OP_CARRIER_FL_NUM,
ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID, CRS_DEP_TIME
~~~

Đây là khóa ứng viên để điều tra, không phải cam kết khóa duy nhất chính thức. Dòng thiếu thành phần khóa được đếm riêng, không đưa vào collision của khóa đầy đủ.

| Chỉ số | Ý nghĩa | Exact duplicate | Candidate-key collision |
| --- | --- | ---: | ---: |
| Groups | Số nhóm có ít nhất hai dòng | 0 | 0 |
| Rows in groups | Toàn bộ dòng thuộc các nhóm đó | 0 | 0 |
| Excess rows | Tổng `count − 1` của từng nhóm | 0 | 0 |

`candidate_key_incomplete_rows = 0`. Ví dụ minh họa: ba bản sao của cùng một dòng tạo **1 group, 3 rows in groups và 2 excess rows**, không phải ba duplicate groups.

Không có bằng chứng cần xóa dòng nên Phase 2 không gọi `dropDuplicates`. Ngay cả khi có collision, tự giữ một dòng bất kỳ cũng có thể làm mất một bản ghi vận hành hợp lệ hoặc một bản cập nhật cần phân tích.

### 3.4. Confirmed invalid và suspicious business rules

Trong [quality_checks.py](../src/airline_analytics/quality_checks.py), các rules được chia làm hai nhóm. “Confirmed invalid” có nghĩa vi phạm **hợp đồng/rule của project**; chẳng hạn năm ngoài 2025 là sai phạm vi MVP, không có nghĩa chuyến bay đó không thể tồn tại.

| Nhóm rule thực tế | Điều kiện được kiểm tra | Ý nghĩa |
| --- | --- | --- |
| Phạm vi và lịch | Năm, tháng, thứ ngoài miền; không parse được ngày; năm/tháng/thứ không khớp ngày; tháng/năm nguồn không khớp | Bảo vệ kỳ phân tích và partition. |
| Trạng thái | Hai flag hủy và chuyển hướng cùng bằng 1; flag ngoài 0/1 | Không để trạng thái mâu thuẫn đi vào KPI. |
| Khoảng cách | `DISTANCE ≤ 0` khi có giá trị | Phát hiện giá trị không phù hợp hợp đồng distance. |
| Nhãn delay | `ARR_DEL15` và `DEP_DEL15` không khớp ngưỡng 15 phút khi cả label và delay có giá trị | Tránh khác định nghĩa giữa nhãn và số phút. |
| `ARR_DELAY_NEW` | Âm hoặc không bằng `max(ARR_DELAY, 0)`, với tolerance 0,0001 | Kiểm tra quan hệ giữa signed delay và nonnegative delay. |
| Giờ HHmm | Ngoài `0000–2359` hợp lệ hoặc `2400` | Không chấp nhận giờ/phút vô lý chỉ vì là chuỗi bốn chữ số. |
| Delay causes | Giá trị âm khi không NULL | Phút được quy cho nguyên nhân không được âm. |

Nhóm **suspicious_business_rules** yêu cầu xem xét nghiệp vụ, không tự quy tất cả thành dữ liệu chắc chắn sai:

- Distance bị thiếu; origin airport ID bằng destination airport ID.
- Completed thiếu `DEP_TIME`, `ARR_TIME`, `ARR_DELAY` hoặc `ARR_DEL15`.
- Cancelled thiếu cancellation code; non-cancelled lại có code.
- Cancelled có `ARR_TIME`; diverted có `ARR_DELAY`.
- Delayed completed thiếu đồng thời cả năm cause fields.

Evidence ghi **0 ở cả 28 confirmed-invalid rules và 11 suspicious rules**. Các phép so sánh thường kiểm tra trường hiện diện; NULL được xử lý qua missingness và rule có điều kiện riêng. Không lấy “0 lỗi định dạng” để suy ra “0 missing”.

Rule cuối chỉ phát hiện **thiếu cả năm** cause fields, không tự chứng minh từng field đều được báo cáo. Coverage từng cause là kiểm chứng bổ sung ở Phase 3B.

## 4. Kết quả baseline đã xác minh

| Trạng thái | Tháng 01 | Tháng 02 | Tháng 03 | Tổng |
| --- | ---: | ---: | ---: | ---: |
| Completed | 522.269 | 496.476 | 592.301 | 1.611.046 |
| Cancelled | 16.312 | 7.405 | 6.923 | 30.640 |
| Diverted | 1.166 | 1.003 | 1.648 | 3.817 |
| Invalid flags | 0 | 0 | 0 | 0 |
| Tổng | 539.747 | 504.884 | 600.872 | 1.645.503 |

`flight_status_reconciles_to_row_count = true`. JSON còn ghi 317.266 giá trị `ARR_DEL15 = 1` và 1.293.780 giá trị bằng 0. Phép tính `317.266 / 1.611.046 ≈ 19,69%` sử dụng completed làm mẫu số; đây là tỷ lệ suy ra từ counts đã xác minh, được triển khai thành KPI ở Phase 3A.

### Giá trị cực đoan không tự động là lỗi

`ARR_DELAY` có min **−91 phút**, max **3.407 phút**. Đến sớm tạo giá trị âm hợp lệ. Giá trị max rất lớn cần được truy vết nếu muốn giải thích một chuyến cụ thể, nhưng chưa có bằng chứng để gọi là sai dữ liệu.

Project không đặt ngưỡng tùy ý để cap, clip hoặc xóa giá trị này. Xóa outlier có thể làm giảm signed average và che khuất gián đoạn thật. Ngược lại, giữ nguyên không có nghĩa đã xác minh nguyên nhân của chuyến cực đoan; hồ sơ hiện có chỉ chứng minh giá trị xuất hiện và không bị các rules đã triển khai phát hiện vi phạm.

## 5. Hiệu quả triển khai và giới hạn kiểm chứng

`profile_file_level_metrics` gộp counts, missingness và status theo file trong một lần aggregation, sau đó cộng các thống kê nhỏ ở phía điều phối. Cast failures và numeric ranges được thu thập chung. Dữ liệu raw dùng `MEMORY_AND_DISK` để các kiểm tra có thể tái sử dụng; các nhóm candidate key được giải phóng sau khi dùng.

Các lựa chọn này giảm công việc lặp trong thiết kế, nhưng **không phải kết quả benchmark**. Exact duplicates và khóa ứng viên vẫn cần group dữ liệu theo hai tiêu chí khác nhau; không bỏ một phép kiểm tra chỉ để giảm số lần scan.

Zero detected violations không đồng nghĩa dữ liệu hoàn hảo:

- Chưa xác minh từng chuyến với hồ sơ khai thác ngoài BTS hoặc danh mục mã độc lập.
- Các rules chỉ bao phủ những quan hệ đã định nghĩa, không bao phủ mọi bất thường vận hành.
- Cast-to-NULL không kiểm chứng đầy đủ precision, mọi nonfinite value hoặc mọi chuyển đổi có thể mất thông tin.
- Exact duplicates dựa vào biểu diễn raw; không chứng minh không có duplicate ngữ nghĩa.
- Chưa giải thích nguyên nhân missing `TAIL_NUM` hay xác minh nghiệp vụ chuyến có delay cực đại.
- Ba tháng không đủ để kết luận chất lượng nguồn ổn định qua nhiều năm.

Bằng chứng này hỗ trợ quyết định **bảo toàn bản ghi, chuẩn hóa kiểu và NULL có điều kiện**, thay vì sửa dữ liệu theo cảm tính. Với KPI, lỗi missing/label làm sai cả tử số lẫn mẫu số. Với ML tương lai, nhãn sai và trường hậu nghiệm có thể tạo đánh giá mô hình sai lệch dù pipeline chạy thành công.

## 6. Câu hỏi phỏng vấn gắn với profiling

- **Vì sao profiling không dùng một tỷ lệ missing chung để kết luận chất lượng?** Từng cột có population áp dụng khác nhau; cancellation code và delay causes là ví dụ rõ.
- **Ba số đo duplicate khác nhau thế nào?** Cần trình bày được group, số dòng liên quan và số dòng dư; collision không tự cho phép xóa.
- **0 violations có đủ để tự động triển khai ML không?** Không; vẫn cần xác định thời điểm feature, target population và các kiểm chứng ngoài bộ rules hiện tại.

## Những điểm cần ghi nhớ

- Baseline là 1.645.503 dòng, không có cast failures hoặc duplicates được các phép kiểm tra hiện tại phát hiện.
- Missing phải được xét theo trạng thái và điều kiện báo cáo, không mặc định điền 0.
- Anomaly rule, nghi vấn nghiệp vụ và outlier là ba khái niệm cần phân biệt.
- Hash bảo vệ nhận diện file; validation bảo vệ một tập hợp hợp đồng dữ liệu hữu hạn.
- Profiling cung cấp căn cứ cho cleaning, không chứng minh mọi bản ghi đúng tuyệt đối.
