# Hiểu dữ liệu và từ điển trường BTS

## 1. Dữ liệu trả lời câu hỏi gì?

Một chuyến bay đến trễ, bị hủy và chuyển hướng là ba kết quả vận hành khác nhau. Nếu chỉ nhìn một cột thời gian trễ, người phân tích có thể nhầm chuyến không có thông tin đến nơi thành chuyến đúng giờ. Tài liệu này xác định ý nghĩa của từng nhóm trường trước khi áp dụng [quy tắc chất lượng](data_quality_report.md), [cleaning](phase2_cleaning_report.md) và [KPI](kpi_definitions.md).

Dataset **BTS Reporting Carrier On-Time Performance** chứa thông tin lịch bay, hoạt động thực tế và kết quả vận hành của các chuyến bay do hãng báo cáo. Phạm vi hiện tại là 12 file CSV tháng 01–12/2025. Đây là dữ liệu của chương trình báo cáo BTS, không đại diện cho toàn bộ hoạt động hàng không trên thế giới.

**Grain** là đơn vị mà một dòng dữ liệu đại diện: ở đây là một chặng bay được báo cáo trong một ngày, kể cả chặng bị hủy hoặc chuyển hướng. Một dòng không phải một hành khách, một máy bay hay toàn bộ hành trình nhiều chặng. `OP_CARRIER_FL_NUM` có thể lặp lại giữa ngày và hãng, nên không phải khóa duy nhất.

| Phạm vi | Raw records | Cleaned analytical records |
| --- | ---: | ---: |
| January–December 2025 | 7.001.619 | 7.001.618 |
| January–March 2025 — mốc MVP/regression | 1.645.503 | 1.645.503 |

Nguồn: `artifacts/phase1_2025/profile_summary.json` và `artifacts/phase2_2025_reviewed/validation_summary.json`. Một completed flight thiếu thông tin arrival thiết yếu được giữ riêng trong rejected output; raw không thay đổi. Số liệu Q1 là mốc lịch sử, không phải tổng cả năm.

## 2. Ba lớp schema cần phân biệt

1. **CSV nguồn:** 36 cột theo đúng header đã kiểm chứng. CSV không lưu một schema kiểu dữ liệu như Parquet.
2. **DataFrame profiling:** đọc cả 36 cột bằng `StringType`, không suy luận kiểu tự động. Các phép cast chỉ phục vụ kiểm tra khả năng chuyển đổi và thống kê. Bốn flag được kiểm tra dạng số thực vì nguồn có chuỗi như `"0.00"`.
3. **Cleaned Parquet:** 38 cột gồm 36 trường nguồn đã chuẩn hóa kiểu, `_source_file` và `flight_status`. Bốn flag lúc này là `IntegerType`, không còn là double.

Kiểu trong các bảng bên dưới là **kiểu cleaned đã được read-back xác minh**, không phải kiểu suy đoán từ tên cột. Tổng cộng: 14 `int`, 9 `double`, 1 `date` và 14 `string`. Hợp đồng này được khai báo trong [cleaning.py](../src/airline_analytics/cleaning.py) và đối chiếu với `read_back.schema.actual_types` của báo cáo Phase 2.

### 2.1. Ngày bay và lịch vận hành

| Cột | Kiểu cleaned | Ý nghĩa và cách sử dụng |
| --- | --- | --- |
| `YEAR` | `int` | Năm của ngày bay; phạm vi hiện tại là 2025. Cùng `MONTH` xác định kỳ phân tích. |
| `MONTH` | `int` | Tháng của ngày bay, phạm vi hiện tại là 1–12. Không nhóm chỉ theo tháng khi mở rộng nhiều năm. |
| `DAY_OF_WEEK` | `int` | Quy ước BTS: 1 = thứ Hai, …, 7 = Chủ nhật. Không dùng trực tiếp cách đánh số mặc định của Spark mà bỏ qua quy đổi. |
| `FL_DATE` | `date` | Ngày bay. Parse nguồn theo `M/d/yyyy h:mm:ss a` rồi lấy phần ngày; không phải timestamp khởi hành thực tế. |

Phase 1 kiểm tra năm/tháng/thứ khớp `FL_DATE` và tháng nguồn. Điều này giúp phát hiện file tháng bị gắn nhầm trước khi partition và tổng hợp.

### 2.2. Hãng, số hiệu chuyến bay và máy bay

| Cột | Kiểu cleaned | Ý nghĩa và lưu ý |
| --- | --- | --- |
| `OP_UNIQUE_CARRIER` | `string` | Mã hãng **khai thác** chuyến bay, là chiều phân tích airline; không tự đồng nhất với hãng bán vé. |
| `OP_CARRIER_AIRLINE_ID` | `int` | Mã định danh hãng của BTS/DOT. Là định danh, không phải đại lượng để lấy trung bình. |
| `TAIL_NUM` | `string` | Số đăng ký máy bay; có thể thiếu. Không phải định danh duy nhất của một chuyến. |
| `OP_CARRIER_FL_NUM` | `string` | Số hiệu chuyến trong hãng. Giữ dạng định danh, không dùng như số đo liên tục. |

Mã hãng xác định nhóm khai thác, không bảo đảm các nhóm có cùng quy mô, lịch bay hay mạng tuyến; so sánh hiệu suất cần cả count và rate.

### 2.3. Điểm đi, điểm đến và khoảng cách

| Cột | Kiểu cleaned | Ý nghĩa và lưu ý |
| --- | --- | --- |
| `ORIGIN_AIRPORT_ID` | `int` | Định danh sân bay xuất phát. |
| `ORIGIN_AIRPORT_SEQ_ID` | `int` | Định danh sân bay gắn với phiên bản thông tin theo thời gian; không thay thế tùy tiện cho airport ID. |
| `ORIGIN_CITY_MARKET_ID` | `int` | Định danh thị trường/thành phố xuất phát; có thể gom nhiều sân bay trong cùng thị trường. |
| `ORIGIN` | `string` | Mã sân bay xuất phát dùng trong các aggregate hiện tại. |
| `ORIGIN_CITY_NAME` | `string` | Tên thành phố xuất phát để diễn giải; không dùng thay khóa định danh. |
| `DEST_AIRPORT_ID` | `int` | Định danh sân bay đích theo lịch. |
| `DEST_AIRPORT_SEQ_ID` | `int` | Phiên bản định danh sân bay đích. |
| `DEST_CITY_MARKET_ID` | `int` | Định danh thị trường/thành phố đích. |
| `DEST` | `string` | Mã sân bay đích theo lịch; không coi là sân bay hạ cánh thực tế khi chuyến chuyển hướng. |
| `DEST_CITY_NAME` | `string` | Tên thành phố đích theo lịch. |
| `DISTANCE` | `double` | Khoảng cách chặng bay, đơn vị mile; không phải quãng đường bay thực tế đo từ vị trí máy bay. |

Tuyến có hướng `ORIGIN → DEST` khác tuyến ngược lại. Aggregate route sử dụng `YEAR, MONTH, ORIGIN, DEST` và gộp các hãng cùng tuyến trong tháng. Airport ID, city-market ID và mã sân bay mô tả các cấp khác nhau; trộn chúng có thể thay đổi grain.

### 2.4. Giờ theo lịch, giờ thực tế và độ trễ

| Cột | Kiểu cleaned | Ý nghĩa, đơn vị |
| --- | --- | --- |
| `CRS_DEP_TIME` | `string` | Giờ khởi hành theo lịch, dạng HHmm. |
| `DEP_TIME` | `string` | Giờ khởi hành thực tế, dạng HHmm; có thể NULL. |
| `CRS_ARR_TIME` | `string` | Giờ đến theo lịch, dạng HHmm. |
| `ARR_TIME` | `string` | Giờ đến thực tế, dạng HHmm; có thể NULL. |
| `DEP_DELAY` | `double` | Chênh lệch khởi hành thực tế so với lịch, phút có dấu. Âm là sớm. |
| `DEP_DEL15` | `int` | Flag khởi hành trễ ít nhất 15 phút: 1; dưới 15 phút: 0; không có nhãn: NULL. |
| `ARR_DELAY` | `double` | Chênh lệch đến thực tế so với lịch, phút có dấu. Là số đo dùng cho signed average. |
| `ARR_DELAY_NEW` | `double` | Độ trễ đến không âm: giá trị âm của `ARR_DELAY` được quy về 0 trong định nghĩa trường nguồn. |
| `ARR_DEL15` | `int` | Flag đến trễ ít nhất 15 phút. Dùng làm nhãn delayed/on-time trong KPI. |

**Scheduled** mô tả kế hoạch; **actual** mô tả điều đã xảy ra. Phân tích khung giờ Phase 3B sử dụng `CRS_DEP_TIME` để phân nhóm theo lịch, không phân nhóm theo giờ đã bị dịch chuyển do trễ.

Ví dụ minh họa định nghĩa, không phải kết quả mới: nếu `ARR_DELAY = -6` thì chuyến đến sớm 6 phút, `ARR_DELAY_NEW = 0` và `ARR_DEL15 = 0`. Một chuyến đến muộn 13 phút vẫn có `ARR_DEL15 = 0`. Vì thế “on-time” trong project nghĩa là **không đạt ngưỡng trễ 15 phút**, không có nghĩa mọi chuyến đều đến trước hoặc đúng giờ lịch.

#### Vì sao HHmm là string?

Giá trị `"0829"` phải giữ số 0 đầu. Chuyển thành integer `829` vừa mất biểu diễn chuẩn, vừa làm so sánh chuỗi không còn đáng tin. Phase 2 chỉ trim khoảng trắng, **không thêm số 0 để tự sửa giờ thiếu chữ số**.

Giờ ở dạng HHmm là giờ địa phương của điểm liên quan, chưa chứa đủ ngày và múi giờ để tính khoảng thời gian giữa hai sân bay. Không lấy `ARR_TIME - DEP_TIME` như phép trừ số thông thường. `"2400"` được chấp nhận trong kiểm tra định dạng nhưng không tự chuyển thành timestamp ngày hôm sau. Phase 3B quy nhóm giá trị này vào Night, giữ nguyên ngày/tháng bay; evidence cả năm ghi **1 trường hợp trong tháng 07/2025**; mốc Q1 có 0. Cả năm không có scheduled time Missing hoặc Invalid.

### 2.5. Kết quả vận hành và lý do hủy

| Cột | Kiểu cleaned | Ý nghĩa |
| --- | --- | --- |
| `CANCELLED` | `int` | 1 = chuyến bị hủy, 0 = không bị hủy. |
| `DIVERTED` | `int` | 1 = chuyến chuyển hướng, 0 = không chuyển hướng. |
| `CANCELLATION_CODE` | `string` | Mã lý do hủy: A = hãng, B = thời tiết, C = National Air System, D = an ninh. NULL ở chuyến không hủy là có điều kiện nghiệp vụ. |

| `CANCELLED` | `DIVERTED` | `flight_status` | Số dòng xác minh |
| ---: | ---: | --- | ---: |
| 0 | 0 | `completed` | 6.879.484 |
| 1 | 0 | `cancelled` | 102.876 |
| 0 | 1 | `diverted` | 19.258 |
| Tổ hợp khác, kể cả thiếu flag | | `invalid` | 0 |

“Completed” ở đây là định nghĩa phân tích của project: không hủy và không chuyển hướng. Không mở rộng thành nhận định rằng một chuyến diverted không bao giờ hoàn tất hành trình sau đó. Bảng trên đếm cleaned records cả năm. Raw có 6.879.485 completed; một dòng thiếu arrival thiết yếu được tách riêng mà không đổi status. Cleaning không loại cancelled/diverted.

### 2.6. Phút trễ được quy cho từng nhóm nguyên nhân

Cả năm cột dưới đây là `double`, đơn vị **phút**, không phải flag hay số chuyến.

| Cột | Nhóm nguyên nhân được báo cáo |
| --- | --- |
| `CARRIER_DELAY` | Các vấn đề thuộc hoạt động của hãng. |
| `WEATHER_DELAY` | Phần phút trễ được báo cáo ở nhóm thời tiết. |
| `NAS_DELAY` | National Air System: các yếu tố thuộc hệ thống hàng không quốc gia. |
| `SECURITY_DELAY` | An ninh. |
| `LATE_AIRCRAFT_DELAY` | Máy bay phục vụ chặng này đến muộn từ chặng trước. |

Các nhóm là phân loại báo cáo của BTS, không phải kết quả suy luận nhân quả do project tạo ra. Không coi `WEATHER_DELAY` là phép đo đầy đủ mọi ảnh hưởng trực tiếp/gián tiếp của thời tiết.

Phase 3B xét cause minutes trên `completed AND ARR_DEL15 = 1`. Một chuyến có thể đóng góp phút cho nhiều nhóm. Tỷ trọng phút không phải tỷ lệ chuyến bị ảnh hưởng; xem [phân tích delay causes](phase3b_findings.md).

### 2.7. Hai cột bổ sung và lineage

| Cột | Kiểu | Trách nhiệm |
| --- | --- | --- |
| `_source_file` | `string` | Tên file CSV nguồn, giúp truy ngược bản ghi. Không phải khóa duy nhất. |
| `flight_status` | `string` | Phân loại theo hai flag trong bảng trên, tránh mỗi phân tích tự định nghĩa trạng thái khác nhau. |

Ingestion còn dùng `_source_year` và `_source_month` nội bộ để đối chiếu nguồn. Hai trường này không được lưu trong cleaned schema 38 cột.

## 3. NULL phải được hiểu trong ngữ cảnh

**Conditional NULL** là giá trị thiếu phù hợp với điều kiện nghiệp vụ. NULL không đồng nghĩa 0.

- Raw có **122.135** giá trị thiếu ở mỗi cột `ARR_DELAY`, `ARR_DELAY_NEW`, `ARR_DEL15`. Trong đó 122.134 bằng cancelled + diverted; một completed flight còn lại được diagnostic xác định và tách khỏi analytical dataset.
- Completed thiếu bất kỳ `ARR_TIME`, `ARR_DELAY` hoặc `ARR_DEL15` bị phân loại rejected. Đây là quy tắc chất lượng của analytical dataset, không phải thay đổi trạng thái chuyến bay.
- Thiếu riêng `ARR_DELAY_NEW` chỉ được thống kê, không tự điền hoặc loại. Evidence ghi 0 completed chỉ thiếu trường này.
- `CANCELLATION_CODE` NULL ở chuyến không hủy là phù hợp. Điền mã giả sẽ làm sai ý nghĩa dữ liệu.
- Raw thiếu **5.466.981** dòng ở mỗi cause field. Trong population eligible của phân tích causes, cả năm trường đều được báo cáo cho 1.534.638 delayed completed flights, bao gồm giá trị 0.
- `TAIL_NUM` thiếu **12.501** dòng raw; chưa có căn cứ quy toàn bộ missing này cho một nguyên nhân vận hành.

Sau cleaning, completed có đủ ba trường arrival thiết yếu và nhãn 0/1. Không áp dụng yêu cầu đó cho cancelled/diverted. Xem [quy tắc Accepted/Rejected](phase2_cleaning_report.md) và [coverage của delay causes](phase3b_findings.md).

## 4. Căn cứ và giới hạn

Các định nghĩa kiểu và cách xử lý đối chiếu [schema.py](../src/airline_analytics/schema.py), [ingestion.py](../src/airline_analytics/ingestion.py), [cleaning.py](../src/airline_analytics/cleaning.py) và SQL 000–010. Schema 38 cột được xác minh trong `artifacts/phase2_2025_reviewed/validation_summary.json → read_back.schema`; missingness raw thuộc JSON profiling, còn cause coverage và HHmm boundary thuộc JSON Phase 3B cả năm.

Bộ dữ liệu không cung cấp số hành khách, chi phí chậm chuyến hay tác động doanh thu. Flight counts không thể thay thế các đại lượng này. Dictionary cũng không chứng minh mọi mã hãng/sân bay đã được đối chiếu với danh mục bên ngoài.

## Những điểm cần ghi nhớ

- Một dòng là một chặng bay được báo cáo; raw có 36 cột, cleaned có 38.
- Phân biệt lịch bay, thời gian thực tế, trạng thái và nhãn trễ.
- HHmm là string; NULL khác 0; ID dạng số không phải đại lượng đo.
- Luôn xác định population trước khi diễn giải missingness hoặc tính KPI.
- Counts trong dictionary là cả năm; Q1 chỉ là mốc regression được ghi rõ.
