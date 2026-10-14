# Phân tích hiệu suất theo thứ, khung giờ và nhóm nguyên nhân trễ

## 1. Câu hỏi nghiệp vụ và phạm vi bằng chứng

Arrival Delay Rate toàn kỳ cho biết mức độ trễ chung, nhưng chưa chỉ ra nhóm lịch bay nào cần xem xét sâu hơn. Phase 3B bổ sung ba góc nhìn: ngày trong tuần, giờ khởi hành theo lịch và phần phút trễ được báo cáo cho từng nhóm nguyên nhân.

Các phân tích mô tả **những gì đã quan sát trong tháng 01–03/2025**. Chúng không chứng minh nguyên nhân, không dự báo chuyến tiếp theo và chưa đo tác động của một biện pháp cải thiện vận hành.

Nguồn kiểm chứng:

- `artifacts/phase3b/validation_summary.json`, được tạo lúc `2026-10-10T06:38:06.713552+00:00`.
- Các CSV data part trong `artifacts/phase3b/day_of_week/`, `departure_time_band/` và `delay_causes/`, đã được đọc để đối chiếu findings.
- [run_phase3b.py](../scripts/run_phase3b.py) và SQL 008–010 để xác định logic thực tế.

Counts, phút và rate/share chưa làm tròn trong CSV là **kết quả trực tiếp**. Các phần trăm dưới đây là cách trình bày fraction nhân 100 và làm tròn hai chữ số. Tổng ba tháng và coverage percentage được ghi rõ là **phép tính suy ra**. Không lấy tỷ lệ đã làm tròn trên dashboard để suy ngược numerator.

Đây là tài liệu của lần chạy đã được owner xác nhận thành công, không ghi nhận một lần chạy mới. Phase 3B đã hoàn thành implementation và runtime validation. Dashboard Power BI ba trang hiện đã được xây dựng và đang hoàn thiện; Machine Learning chưa triển khai. Trạng thái “Power BI chưa bắt đầu” trong bản báo cáo lịch sử không còn là trạng thái hiện tại.

## 2. Input, output và cách tổ chức xử lý

Input duy nhất là `data/processed/flights_cleaned/`. Runner không đọc lại raw CSV, không chạy lại Phase 1–2 và không xây lại bốn bảng Phase 3A.

| SQL | Grain: một dòng đại diện cho | Output dưới `artifacts/phase3b/` |
| --- | --- | --- |
| [008_day_of_week.sql](../sql/analytics/008_day_of_week.sql) | `YEAR, MONTH, DAY_OF_WEEK` | `day_of_week/` |
| [009_departure_time_band.sql](../sql/analytics/009_departure_time_band.sql) | `YEAR, MONTH, time_band` | `departure_time_band/` |
| [010_delay_causes.sql](../sql/analytics/010_delay_causes.sql) | `YEAR, MONTH, cause_category` | `delay_causes/` |

Runner tạo temporary view `flights_cleaned` và cache 11 cột cần dùng bằng `MEMORY_AND_DISK`. SQL chịu trách nhiệm phân nhóm/tính toán; Python điều phối validation và xuất CSV. Chỉ các aggregate nhỏ được `collect` để kiểm tra, không thu toàn bộ flight-level dataset về driver và không dùng `toPandas()`.

Mỗi output CSV là một **thư mục Spark** chứa data part và metadata, không phải file mang tên thư mục. CSV phục vụ trao đổi kết quả nhỏ; không thay thế cleaned Parquet. Do CSV không lưu kiểu, read-back dùng schema SQL tường minh và cùng quy ước chuỗi `NULL`.

### Counts nguồn dùng để đối chiếu

Nguồn trực tiếp: `input.by_month` trong JSON.

| Tháng 2025 | Total flights | Completed flights | Delayed completed flights |
| --- | ---: | ---: | ---: |
| 01 | 539.747 | 522.269 | 98.130 |
| 02 | 504.884 | 496.476 | 103.102 |
| 03 | 600.872 | 592.301 | 116.034 |
| Tổng ba tháng, suy ra | 1.645.503 | 1.611.046 | 317.266 |

Tổng input 1.645.503 cũng được JSON ghi trực tiếp tại `input.row_count`. Counts cancelled 30.640 và diverted 3.817 thuộc evidence Phase 1–2, không phải hai phép đo tách riêng mới trong JSON Phase 3B.

Định nghĩa population và cách tính KPI chung nằm ở [KPI Methodology](kpi_definitions.md); ý nghĩa trường ở [Data Dictionary](data_dictionary.md).

## 3. Day-of-Week Performance

### 3.1. Câu hỏi và phương pháp

**Ngày nào trong tuần có tỷ lệ đến trễ cao hơn trong từng tháng?**

SQL nhóm theo năm, tháng và `DAY_OF_WEEK`, giữ quy ước BTS: 1 = thứ Hai, …, 7 = Chủ nhật. Một nhóm thứ Hai/tháng là toàn bộ các ngày thứ Hai trong tháng đó, không phải một ngày lịch duy nhất.

~~~sql
COUNT(CASE WHEN flight_status = 'completed'
           AND ARR_DEL15 = 1 THEN 1 END)
~~~

Biểu thức trên đếm delayed completed flights. CASE trả NULL cho dòng không thỏa; COUNT bỏ NULL. Mẫu số được đếm riêng bằng điều kiện completed, không bao gồm cancelled/diverted:

~~~text
Arrival Delay Rate của nhóm = delayed completed / completed
~~~

`NULLIF(completed_flights, 0)` trả tỷ lệ NULL khi không có completed. Total vẫn bao gồm cả ba trạng thái hợp lệ. Nhóm không xuất hiện không được tạo thêm dòng 0 giả.

Output có bảy cột: ba khóa int, ba counts bigint (`total_flights`, `completed_flights`, `delayed_arrival_flights`), và `arrival_delay_rate` double.

Validation cộng counts của bảy nhóm để khớp counts nguồn từng tháng. Phase 3B kiểm tra miền 1–7, không tính lại thứ từ `FL_DATE`; kiểm tra nhất quán ngày/thứ đã thuộc Phase 1.

### 3.2. Kết quả theo tháng

Đối chiếu cả bảy rate chưa làm tròn trong mỗi tháng cho các nhóm cao nhất/thấp nhất:

| Tháng | Vị trí theo rate | Thứ | Delayed / completed | Arrival Delay Rate |
| --- | --- | --- | ---: | ---: |
| 01 | Cao nhất | Thứ Hai | 16.002 / 70.684 | 22,64% |
| 01 | Thấp nhất | Thứ Tư | 10.586 / 79.713 | 13,28% |
| 02 | Cao nhất | Thứ Năm | 21.753 / 76.272 | 28,52% |
| 02 | Thấp nhất | Thứ Ba | 10.618 / 65.833 | 16,13% |
| 03 | Cao nhất | Chủ nhật | 25.738 / 98.176 | 26,22% |
| 03 | Thấp nhất | Thứ Ba | 9.429 / 72.141 | 13,07% |

Nguồn: CSV `day_of_week/`. Ngày có rate cao nhất thay đổi giữa các tháng; không đủ căn cứ gọi một thứ là “luôn tệ nhất”.

### 3.3. Vì sao count và rate cho thứ hạng khác nhau?

Thứ Sáu tháng 01 có **17.196 / 88.355 = 19,46%**, nhiều delayed flights hơn thứ Hai (16.002), nhưng rate thấp hơn (22,64%). Count cho biết khối lượng gián đoạn; rate điều chỉnh theo số completed trong nhóm.

Số lần xuất hiện của từng thứ, lịch khai thác và quy mô nhóm có thể khác nhau. Không dùng trung bình các weekday percentages để tính rate tháng; phải cộng delayed rồi chia tổng completed.

Các findings giúp chọn nhóm cần tìm hiểu tiếp, không khẳng định thứ trong tuần gây trễ. Chưa điều chỉnh khác biệt hãng, sân bay, tuyến, lịch bay hoặc sự kiện thời tiết; chưa kiểm định ý nghĩa thống kê.

## 4. Scheduled Departure Time Bands

### 4.1. Vì sao sử dụng scheduled time?

**Tỷ lệ đến trễ khác nhau thế nào giữa các khung giờ khởi hành theo lịch?**

`CRS_DEP_TIME` mô tả lịch dự kiến. Nếu dùng `DEP_TIME` thực tế, một chuyến bị trễ có thể chuyển sang nhóm giờ muộn hơn chính vì đã trễ, làm thay đổi câu hỏi phân tích. Phase 3B giữ cách phân nhóm theo lịch, không thay đổi nguồn.

Giờ là giờ địa phương tại sân bay xuất phát, không phải một trục UTC chung cho mọi sân bay.

| Nhóm | Điều kiện |
| --- | --- |
| Night | `0000–0559`, cộng giá trị chính xác `2400` |
| Morning | `0600–1159` |
| Afternoon | `1200–1759` |
| Evening | `1800–2359` |
| Missing | `CRS_DEP_TIME IS NULL` |
| Invalid | Có giá trị nhưng không đúng HHmm bốn chữ số và không phải `2400` |

SQL kiểm tra định dạng trước khi so sánh chuỗi. Với HHmm chuẩn có leading zeros, thứ tự chuỗi phù hợp thứ tự giờ trong ngày. Chuỗi sai định dạng không được tự sửa để lọt vào nhóm hợp lệ. Blank còn sót được xếp Invalid; cleaned input đã chuẩn hóa blank thành NULL.

`2400` được xếp Night **chỉ để grouping**, không đổi ngày/tháng chuyến bay. `scheduled_2400_flights` theo dõi riêng trường hợp này.

### 4.2. Kết quả và numerator/denominator

Output có tám cột: YEAR/MONTH int, time_band string, ba counts như phân tích weekday, diagnostic count `scheduled_2400_flights` bigint và rate double.

| Tháng | Khung giờ | Delayed | Completed | Delayed / completed |
| --- | --- | ---: | ---: | ---: |
| 01 | Night | 1.811 | 14.430 | 12,55% |
| 01 | Morning | 32.214 | 207.726 | 15,51% |
| 01 | Afternoon | 39.551 | 191.227 | 20,68% |
| 01 | Evening | 24.554 | 108.886 | 22,55% |
| 02 | Night | 1.781 | 14.282 | 12,47% |
| 02 | Morning | 32.082 | 195.866 | 16,38% |
| 02 | Afternoon | 42.221 | 180.233 | 23,43% |
| 02 | Evening | 27.018 | 106.095 | 25,47% |
| 03 | Night | 1.732 | 19.989 | 8,66% |
| 03 | Morning | 30.416 | 228.314 | 13,32% |
| 03 | Afternoon | 47.542 | 206.404 | 23,03% |
| 03 | Evening | 36.344 | 137.594 | 26,41% |

Nguồn: CSV `departure_time_band/`. Trong cả ba tháng, Evening có rate cao nhất và Night thấp nhất. Night có ít completed hơn đáng kể; cần đọc volume cùng rate.

Quan sát này chưa chứng minh khởi hành muộn gây trễ, cũng chưa đủ để khuyến nghị đổi lịch khai thác. Cơ cấu tuyến/hãng/sân bay và các yếu tố khác giữa các khung giờ chưa được kiểm soát.

### 4.3. Missing, Invalid và 2400

JSON ghi `missing_times = 0`, `invalid_times = 0` và `midnight_2400 = 0` trong **từng tháng**. Vì vậy chỉ bốn nhóm thông thường xuất hiện mỗi tháng, tổng 12 rows; tất cả `scheduled_2400_flights` trong CSV bằng 0.

Code có nhánh giữ Missing/Invalid và xử lý 2400, nhưng lần chạy dữ liệu thực này **không kiểm nghiệm các nhánh với số dòng khác 0**. PASS không được diễn giải thành bằng chứng bao phủ mọi edge case.

Nếu có Missing/Invalid, chúng vẫn góp vào reconciliation; không bị bỏ để làm biểu đồ đẹp hơn. Mẫu số completed bằng 0 sẽ tạo rate NULL; dữ liệu output lần này không có mẫu số bằng 0.

## 5. Delay Cause Breakdown

### 5.1. Câu hỏi và population

**Trong các chuyến completed có `ARR_DEL15 = 1`, nhóm nguyên nhân báo cáo nào chiếm nhiều phút trễ nhất?**

| Nhóm | Ý nghĩa báo cáo |
| --- | --- |
| `CARRIER_DELAY` | Các vấn đề thuộc hoạt động hãng |
| `WEATHER_DELAY` | Phần phút được quy vào nhóm thời tiết |
| `NAS_DELAY` | Các yếu tố thuộc National Air System |
| `SECURITY_DELAY` | An ninh |
| `LATE_AIRCRAFT_DELAY` | Máy bay phục vụ chặng đến muộn từ chặng trước |

Đây là các nhóm được báo cáo trong BTS, không phải nguyên nhân do project suy luận từ mô hình. Không coi riêng WEATHER_DELAY là toàn bộ ảnh hưởng trực tiếp/gián tiếp của thời tiết.

### 5.2. Logic SQL: từ cột nguyên nhân đến tỷ trọng phút

Query có bốn bước có trách nhiệm riêng:

1. **`eligible`:** lọc completed và label 1; tạo flags có ít nhất một/tất cả cause fields được báo cáo.
2. **`expanded`:** `STACK(5, …)` chuyển năm cột thành năm dòng category cho mỗi eligible flight, giữ cả giá trị NULL.
3. **`category_totals`:** GROUP BY YEAR/MONTH/category để đếm coverage và cộng phút.
4. **`denominators`:** `SUM(attributed_minutes) OVER (PARTITION BY YEAR, MONTH)` tạo mẫu số chung của tháng mà không làm mất dòng category.

~~~text
attributed_minutes(category, month)
    = SUM(phút được báo cáo của category trên eligible flights)

cause_minute_share
    = attributed_minutes / NULLIF(monthly_attributed_minutes, 0)
~~~

Window SUM ở đây cần thiết vì phải vừa giữ grain category-month vừa có tổng tháng trên mỗi dòng. Đó là tổng phút của năm category, **không phải số chuyến trễ** và không phải signed sum ARR_DELAY.

### 5.3. Schema và nguy cơ đếm trùng

Output gồm YEAR/MONTH int, cause_category string và bảy measures sau:

| Measure | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `eligible_delayed_flights` | bigint | Số completed có label 1 trong tháng |
| `reported_cause_flights` | bigint | Eligible có cause field này không NULL, kể cả giá trị 0 |
| `flights_with_any_reported_cause` | bigint | Eligible có ít nhất một cause field không NULL |
| `flights_with_all_reported_causes` | bigint | Eligible có cả năm cause fields không NULL |
| `attributed_minutes` | double | Tổng phút category, tử số |
| `monthly_attributed_minutes` | double | Tổng phút năm category trong tháng, mẫu số |
| `cause_minute_share` | double | Tỷ trọng dạng fraction, NULL nếu không xác định |

Eligible count, any/all coverage counts và mẫu số tháng lặp lại trên năm dòng category. **Không cộng những số tháng này qua năm dòng**. Một chuyến có thể đóng góp phút cho nhiều nhóm.

Muốn tính tỷ lệ chuyến có phút dương của một nguyên nhân cần một numerator khác: số chuyến có cause minutes > 0. `reported_cause_flights` không phải numerator đó vì bao gồm reported zero. Project không thay minute share bằng tỷ lệ chuyến.

### 5.4. NULL, reported zero và coverage

- NULL: không có giá trị được báo cáo; không tự điền 0.
- 0: đã báo cáo và không có phút ở category đó; vẫn được COUNT(cause_minutes) đếm.
- `SUM` bỏ từng NULL. Category không có giá trị nào → attributed_minutes NULL và reported count 0.
- Có reported values nhưng tất cả bằng 0 → tổng phút 0; khác trường hợp không có báo cáo.
- Tổng tháng NULL hoặc 0 → share NULL, không trình bày như 0% có ý nghĩa.
- Tháng có eligible flights vẫn có năm category rows; tháng không có eligible không tạo năm dòng giả.

Shares cộng gần 100% khi mẫu số dương không tự chứng minh coverage đầy đủ: đó cũng có thể chỉ là phân bổ của phần phút đã được báo cáo.

### 5.5. Kết quả từng tháng

Mẫu số trực tiếp từ CSV: tháng 01 **6.943.994 phút**, tháng 02 **7.411.011 phút**, tháng 03 **8.378.368 phút**. Mỗi ô dưới đây thể hiện tử số phút và tỷ trọng trên mẫu số tháng tương ứng.

| Category | Tháng 01: phút (share) | Tháng 02: phút (share) | Tháng 03: phút (share) |
| --- | ---: | ---: | ---: |
| CARRIER_DELAY | 2.510.886 (36,16%) | 2.513.734 (33,92%) | 2.761.618 (32,96%) |
| LATE_AIRCRAFT_DELAY | 2.410.759 (34,72%) | 2.735.511 (36,91%) | 3.281.080 (39,16%) |
| NAS_DELAY | 1.381.827 (19,90%) | 1.666.833 (22,49%) | 1.832.804 (21,88%) |
| WEATHER_DELAY | 629.972 (9,07%) | 486.199 (6,56%) | 492.193 (5,87%) |
| SECURITY_DELAY | 10.550 (0,15%) | 8.734 (0,12%) | 10.673 (0,13%) |

Ví dụ: `3.281.080 / 8.378.368 ≈ 39,16%` là **tỷ trọng phút trễ** của late-aircraft trong tháng 03, không phải 39,16% chuyến bị trễ do nhóm đó.

Carrier và late-aircraft là hai nhóm lớn nhất trong cả ba tháng. Carrier đứng đầu tháng 01; late-aircraft đứng đầu tháng 02 và 03. Đây là gợi ý nơi cần điều tra vận hành sâu hơn, không chứng minh một biện pháp cụ thể sẽ giảm bao nhiêu chuyến trễ.

### 5.6. Coverage quan sát được

| Tháng | Reported count của từng category / eligible | Any reported | All five reported |
| --- | ---: | ---: | ---: |
| 01 | 98.130 / 98.130 | 98.130 | 98.130 |
| 02 | 103.102 / 103.102 | 103.102 | 103.102 |
| 03 | 116.034 / 116.034 | 116.034 | 116.034 |

Coverage không NULL là **100% mỗi tháng**, suy ra từ các counts trực tiếp trên, bao gồm reported zero. Không có nghĩa cả năm nguyên nhân đều có phút dương trên mọi chuyến, hoặc báo cáo đã giải thích đầy đủ mọi nguyên nhân thực tế.

Phân tích không đọc `ARR_DELAY` để ép tổng cause minutes bằng signed delay. Hai đại lượng khác population và cách đo; không sử dụng reconciliation giữa chúng nếu chưa có định nghĩa nghiệp vụ phù hợp.

## 6. Validation đã thực hiện và mức đảm bảo

| Kết quả trong evidence | Giá trị |
| --- | --- |
| `overall_status` và input status | PASS |
| Input rows | 1.645.503 |
| Runtime | Spark 3.5.8, `local[2]` |
| Checks | 30/30 có `passed = true` |
| `failed_checks`, `errors` | Danh sách rỗng |
| `failure_stage` | NULL |
| Day-of-week: output/read-back | 21 / 21 rows, PASS |
| Departure bands: output/read-back | 12 / 12 rows, PASS |
| Delay causes: output/read-back | 15 / 15 rows, PASS |

30/30 là đếm từ checks trong JSON đã lưu. Exit code 0 là owner xác nhận, không phải field trong JSON.

Các lớp validation của [runner](../scripts/run_phase3b.py):

1. Kiểm tra 11 cột đầu vào cần thiết và kiểu, nguồn không rỗng, đúng ba tháng MVP; domain weekday/status/label hợp lệ. Không gọi đây là kiểm tra lại toàn bộ 38 cột.
2. Cause values khi có phải hữu hạn và không âm; tổng tháng hữu hạn.
3. Grain duy nhất, nhãn nhóm hợp lệ; `0 ≤ delayed ≤ completed ≤ total`; rate đúng công thức, domain 0–1 và NULL khi mẫu số 0.
4. Tổng counts weekday/time band theo tháng khớp phép aggregate nguồn riêng. Boundary counts riêng đối chiếu bốn time bands và Missing/Invalid/2400.
5. Năm cause categories ở tháng có eligible; coverage, phút và mẫu số khớp nguồn; tổng shares gần 1 khi mẫu số dương.
6. Ghi CSV rồi đọc lại với schema tường minh, header checking, `FAILFAST` và quy ước NULL thống nhất. So sánh cả số dòng và các giá trị cùng multiplicity bằng `Counter` trên aggregate nhỏ.

Counts và CSV row values/multiplicities so sánh chính xác. Kiểm tra công thức số thực dùng relative tolerance `1E-12` và absolute tolerance `1E-9`; không làm tròn SQL để cưỡng ép khớp.

Output directory phải là thư mục mới dưới `artifacts/` và không chồng lấn input. Runner từ chối directory đã tồn tại, không overwrite/append hoặc tự xóa partial outputs. Check thất bại làm raise exception; các bước chưa tới không được báo PASS.

Lần chạy thực tế có mọi completed denominator và monthly-minute denominator dương, không có Missing/Invalid/2400 và không thiếu cause field trong eligible population. Do đó không tuyên bố đã thử runtime mọi nhánh xử lý ngoại lệ. Validation PASS không phải chứng nhận dữ liệu hoàn hảo hay production benchmark.

## 7. Tổng hợp findings và giới hạn diễn giải

Ba kết luận mô tả có đủ evidence:

- **Theo thứ:** nhóm có rate cao nhất thay đổi giữa tháng 01, 02, 03. Count cao nhất không nhất thiết rate cao nhất.
- **Theo scheduled time:** Evening có rate cao nhất và Night thấp nhất trong cả ba tháng quan sát; quy mô các nhóm khác nhau.
- **Theo cause minutes:** Carrier và late-aircraft chiếm hai phần lớn nhất mỗi tháng; late-aircraft đứng đầu trong tháng 02–03.

Giới hạn chung:

- Chỉ ba tháng, chưa đủ kết luận mùa vụ lặp lại hoặc xu hướng dài hạn.
- Không kiểm soát cơ cấu hãng/tuyến/sân bay, không thực hiện causal inference hoặc kiểm định thống kê.
- Arrival delay rates chỉ xét completed, không bao quát mức gián đoạn do cancelled/diverted hay tác động lên hành khách.
- Cause-minute share là phân bố phút được quy theo báo cáo, không phải xác suất một chuyến bị trễ.
- Không có chi phí, doanh thu hoặc số hành khách để lượng hóa business impact tài chính.
- Các findings là snapshot phạm vi đã nêu. Text Key Findings trên dashboard là static, không tự đổi theo Month Slicer; không diễn giải nó như nhận xét động cho mọi filter context.
- Dashboard đã được xây dựng nhưng tài liệu này không chứng minh mọi DAX measure hoặc tương tác slicer đã được đối chiếu. Không có ML performance results.

Hướng dẫn chạy được giữ tại [README](../README.md), không lặp nguyên khối setup trong tài liệu findings. Không cần chạy lại pipeline để sử dụng các bằng chứng hiện có.

## 8. Câu hỏi phỏng vấn gắn với phân tích

- **Vì sao dùng CRS_DEP_TIME thay DEP_TIME?** Để phân nhóm theo lịch dự kiến; actual time có thể đã bị dịch chuyển bởi sự trễ đang nghiên cứu.
- **Vì sao 100% reporting coverage không đồng nghĩa mọi chuyến chịu mọi nguyên nhân?** COUNT không NULL bao gồm reported zero; cần numerator phút dương khác nếu hỏi tỷ lệ chuyến bị ảnh hưởng.
- **Vì sao window SUM phù hợp ở delay causes?** Cần tổng tháng làm mẫu số trong khi giữ grain category-month.
- **30/30 checks PASS chứng minh gì?** Kết quả thỏa các hợp đồng đã kiểm tra trên dữ liệu quan sát; không chứng minh nhân quả hay bao phủ các edge cases không xuất hiện.

## Những điểm cần ghi nhớ

- So sánh count và rate cùng nhau; rate luôn đi kèm population và denominator.
- Weekday/time bands dùng completed làm mẫu số, không tự loại dòng khỏi total.
- Scheduled time là giờ địa phương; Missing, Invalid và 2400 đều có 0 dòng quan sát trong lần chạy này.
- Delay-cause share đo phút, không đo tỷ lệ chuyến; NULL khác reported zero.
- Kết quả validation và findings là evidence lịch sử đã lưu, không phải pipeline vừa chạy lại.
