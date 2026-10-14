# Phương pháp KPI và Spark SQL Business Analytics

## 1. Từ câu hỏi vận hành đến định nghĩa có thể kiểm chứng

Tháng 03 có nhiều chuyến đến trễ hơn tháng 02, nhưng tỷ lệ đến trễ lại thấp hơn. Hai kết luận không mâu thuẫn: count đo khối lượng, rate đo tỷ trọng trên một population xác định. Một dashboard đáng tin cần cả hai, cùng grain và mẫu số rõ ràng.

Phase 3A trả lời bốn nhóm câu hỏi: hiệu suất theo tháng, theo hãng khai thác, theo sân bay xuất phát và theo tuyến có hướng. Input duy nhất là `data/processed/flights_cleaned/`; pipeline không đọc lại raw CSV, không chạy lại cleaning.

Nguồn kiểm chứng: `artifacts/phase3/validation_summary.json`, thời điểm `2026-10-10T05:35:56.597113+00:00`. Counts và trạng thái validation bên dưới là kết quả trực tiếp đã lưu. Các tỷ lệ trình bày được tính từ counts đó và làm tròn hai chữ số, không suy ngược từ dashboard.

Phase 3A và 3B đã hoàn thành; dashboard Power BI ba trang đã được xây dựng và đang hoàn thiện. Machine Learning chưa triển khai. Phạm vi “không làm Power BI ở Phase 3A” là ranh giới trách nhiệm của bước SQL, không có nghĩa dashboard hiện tại chưa tồn tại.

## 2. Population và ký hiệu

Trong cùng một phạm vi lọc năm/tháng/hãng/sân bay/tuyến:

| Ký hiệu | Cột aggregate | Population hoặc đại lượng |
| --- | --- | --- |
| N | `total_flights` | Tất cả dòng hợp lệ: completed, cancelled và diverted |
| C | `completed_flights` | `flight_status = 'completed'` |
| X | `cancelled_flights` | `flight_status = 'cancelled'` |
| V | `diverted_flights` | `flight_status = 'diverted'` |
| D | `delayed_arrival_flights` | Completed và `ARR_DEL15 = 1` |
| O | `on_time_arrival_flights` | Completed và `ARR_DEL15 = 0` |
| A | `arrival_delay_observed_flights` | Completed có `ARR_DELAY` không NULL |
| S | `arrival_delay_minutes_sum` | Tổng `ARR_DELAY` có dấu trên completed |

Hai quan hệ bắt buộc là `N = C + X + V` và `C = D + O`. Completed thiếu hoặc sai nhãn `ARR_DEL15` làm input validation thất bại; không tự điền 0. Non-completed phải có nhãn arrival NULL.

Trong dữ liệu hiện tại `A = C = 1.611.046`. Dù vậy code vẫn giữ A riêng: SQL `AVG` loại NULL của `ARR_DELAY`, trong khi rate theo nhãn dựa trên C. Nếu tương lai một completed có delay NULL nhưng label hợp lệ, hai mẫu số có thể khác. Không bỏ count A chỉ vì hiện tại chúng bằng nhau.

## 3. Định nghĩa từng KPI

### 3.1. Total Flights — quy mô hoạt động được báo cáo

- **Câu hỏi:** Có bao nhiêu chuyến trong phạm vi đang xét?
- **Định nghĩa/công thức:** `N = COUNT(*)`, đếm mọi dòng của nhóm sau input validation. Đây là count; tử số là số dòng, không có mẫu số.
- **Ví dụ trực tiếp:** 1.645.503 chuyến trong tháng 01–03/2025.
- **NULL và nhóm rỗng:** `COUNT(*)` không bỏ dòng vì cột khác NULL. Pipeline không tạo các tổ hợp chiều không xuất hiện; nguồn rỗng bị từ chối.
- **Sai lầm và giới hạn:** `COUNT(ARR_DELAY)` không phải total, vì bỏ nhiều chuyến non-completed. N không phải số hành khách, không phải số chuyến thực sự cất cánh và không đo doanh thu.

### 3.2. Completed Flights — population cho arrival performance

- **Câu hỏi:** Có bao nhiêu chuyến không hủy và không chuyển hướng theo định nghĩa project?
- **Định nghĩa/công thức:** `C = COUNT(CASE WHEN flight_status = 'completed' THEN 1 END)`. Count không có mẫu số; filter tương ứng `CANCELLED = 0 AND DIVERTED = 0`.
- **Ví dụ trực tiếp:** 1.611.046 chuyến.
- **NULL và nhóm không có completed:** Count bằng 0; status thiếu/không hợp lệ bị chặn tại input, không được coi là completed.
- **Sai lầm và giới hạn:** `CANCELLED = 0` chưa đủ vì còn diverted. Không dùng completed count để kết luận chuyến diverted không bao giờ hoàn thành hành trình sau đó.

### 3.3. Delayed Arrival Flights — khối lượng đến trễ đáng kể

- **Câu hỏi:** Có bao nhiêu chuyến hoàn thành đến trễ ít nhất 15 phút?
- **Định nghĩa/công thức:** `D = COUNT(CASE WHEN flight_status = 'completed' AND ARR_DEL15 = 1 THEN 1 END)`. Đây là count, không có mẫu số.
- **Ví dụ trực tiếp:** 317.266 chuyến.
- **NULL:** Không đếm nhãn NULL; completed có nhãn NULL sẽ bị validation phát hiện, không âm thầm biến thành on-time.
- **Sai lầm và giới hạn:** `ARR_DELAY > 0` là ngưỡng khác, không thay cho label 15 phút. D thể hiện khối lượng trễ, chưa điều chỉnh cho quy mô và không đo mức độ nghiêm trọng của mỗi chuyến.

### 3.4. Arrival Delay Rate — tỷ lệ trễ trong chuyến hoàn thành

- **Câu hỏi:** Trong các chuyến completed, bao nhiêu phần đến trễ ít nhất 15 phút?
- **Công thức:** `D / NULLIF(C, 0)`. Tử số D, mẫu số C; cả hai thuộc cùng nhóm và phạm vi lọc.
- **Ví dụ suy ra từ counts:** `317.266 / 1.611.046 ≈ 19,69%`.
- **NULL/0:** C = 0 → NULL, nghĩa là chưa xác định, không phải 0% trễ.
- **Sai lầm và giới hạn:** Chia D cho N làm tỷ lệ giảm do đưa cancelled/diverted không có nhãn arrival vào mẫu số. Rate này mô tả arrival performance có điều kiện trên completed; không bao quát mọi gián đoạn, nên phải đọc cùng cancellation/diversion rates.

### 3.5. On-Time Arrival Rate — bổ sung cho tỷ lệ trễ

- **Câu hỏi:** Bao nhiêu phần completed không đạt ngưỡng trễ 15 phút?
- **Định nghĩa/công thức:** `O / NULLIF(C, 0)`, với O đếm completed có `ARR_DEL15 = 0`.
- **Ví dụ:** O = 1.293.780 trực tiếp từ JSON; `1.293.780 / 1.611.046 ≈ 80,31%` là tỷ lệ suy ra.
- **NULL/0:** C = 0 → NULL. Không điền missing label bằng 0. Quan hệ bổ sung với arrival delay rate chỉ đáng tin khi `C = D + O`.
- **Sai lầm và giới hạn:** “On-time” bao gồm đến sớm và đến muộn dưới 15 phút; không đồng nghĩa `ARR_DELAY ≤ 0`. Không lấy 100% trừ một rate được tính trên population khác.

### 3.6. Cancellation Rate — tỷ lệ không thực hiện chuyến do hủy

- **Câu hỏi:** Trong toàn bộ chuyến được báo cáo, tỷ lệ bị hủy là bao nhiêu?
- **Công thức:** `X / NULLIF(N, 0)`. X đếm `flight_status = 'cancelled'`; N gồm cả completed, cancelled, diverted.
- **Ví dụ:** X = 30.640 trực tiếp; `30.640 / 1.645.503 ≈ 1,86%` là phép tính từ counts.
- **NULL/0:** Không có chuyến hủy trong một nhóm có N > 0 → 0%; N = 0 → NULL. Không dựa vào sự hiện diện của cancellation code để thay định nghĩa trạng thái.
- **Sai lầm và giới hạn:** Nếu lọc chỉ completed trước khi tính, sẽ mất toàn bộ numerator. Chia X cho C không còn là tỷ trọng hủy trong toàn bộ chuyến. KPI không xác định tác động tài chính hoặc lý do hủy nếu chưa phân tích code.

### 3.7. Diversion Rate — tỷ lệ chuyển hướng

- **Câu hỏi:** Bao nhiêu phần chuyến trong toàn bộ nguồn bị chuyển hướng?
- **Công thức:** `V / NULLIF(N, 0)`; V đếm `flight_status = 'diverted'`, N là total cùng nhóm.
- **Ví dụ:** V = 3.817 trực tiếp; `3.817 / 1.645.503 ≈ 0,23%` là tỷ lệ suy ra.
- **NULL/0:** N = 0 → NULL; có chuyến nhưng không diverted → 0.
- **Sai lầm và giới hạn:** Không nhập diverted vào cancelled và không gán nhãn on-time khi thiếu arrival label. KPI chỉ mô tả trạng thái chuyển hướng, không đo độ trễ cuối hành trình hay nguyên nhân.

### 3.8. Average Arrival Delay Minutes — chênh lệch đến trung bình có dấu

- **Câu hỏi:** Chuyến completed đến sớm/muộn trung bình bao nhiêu phút so với lịch?
- **Công thức thực tế:** `AVG(CASE WHEN flight_status = 'completed' THEN ARR_DELAY END)`. Khi rollup, dùng `S / NULLIF(A, 0)`.
- **Tử số/mẫu số:** S là tổng signed delay của completed có giá trị; A là số quan sát không NULL tương ứng, không phải chỉ số chuyến trễ.
- **Ví dụ suy ra:** Tổng ba signed sums trong JSON là `1.961.963 + 2.839.311 + 3.228.954 = 8.030.228` phút; `8.030.228 / 1.611.046 ≈ 4,98` phút.
- **NULL/0:** `AVG` bỏ NULL; không có quan sát → NULL. `SUM` không có giá trị cũng giữ NULL, không tự điền 0.
- **Sai lầm và giới hạn:** Lọc `ARR_DELAY > 0` hoặc dùng `ARR_DELAY_NEW` sẽ đổi sang câu hỏi khác. Signed average có thể âm và không bị giới hạn 0–1. Các chuyến đến sớm làm giảm average; cực trị 3.407 phút được giữ và có thể ảnh hưởng average. 4,98 phút không phải mức trễ trung bình của riêng 317.266 chuyến delayed.

## 4. Tổng hợp đúng: ratio of sums và weighted average

Mỗi tháng có completed count khác nhau. Vì vậy tỷ lệ cả quý phải là:

~~~text
(98.130 + 103.102 + 116.034)
---------------------------------  ≈ 19,69%
(522.269 + 496.476 + 592.301)
~~~

Không lấy `AVG(arrival_delay_rate)` của ba tháng: cách đó cho mỗi tháng trọng số bằng nhau, thay vì mỗi chuyến completed trọng số bằng nhau. Cũng không cộng các tỷ lệ.

SQL pattern dùng khi tổng hợp lại:

~~~sql
SUM(delayed_arrival_flights)
    / NULLIF(SUM(completed_flights), 0)

SUM(arrival_delay_minutes_sum)
    / NULLIF(SUM(arrival_delay_observed_flights), 0)
~~~

Công thức thứ hai là average có trọng số theo **số quan sát delay**, không phải trung bình các average từng hãng/tháng. Tất cả aggregate giữ counts, signed sum và observation count để tái tính được khi thay đổi phạm vi lọc.

## 5. Grain và schema của bốn outputs

Dữ liệu đã ghi và đọc lại dưới `data/analytics/`:

| Bảng | Một dòng đại diện cho | Số cột | Số dòng pre-write = read-back |
| --- | --- | ---: | ---: |
| `monthly_performance` | `YEAR, MONTH` | 15 | 3 |
| `airline_monthly_performance` | `YEAR, MONTH, OP_UNIQUE_CARRIER` | 16 | 42 |
| `origin_airport_monthly_performance` | `YEAR, MONTH, ORIGIN` | 16 | 991 |
| `route_monthly_performance` | `YEAR, MONTH, ORIGIN, DEST` | 17 | 16.997 |

Số dòng là trực tiếp từ JSON; số cột được đếm từ schema maps trong JSON. 42 là số **airline-month rows**, không phải 42 hãng; tương tự 16.997 không phải số tuyến duy nhất toàn quý.

Mỗi bảng có 13 measures: bảy `bigint` counts, hai `double` cho signed sum/average và bốn `double` rates. `YEAR/MONTH` là int; codes là string. Rates lưu dạng fraction 0–1 chưa làm tròn; phần trăm là định dạng trình bày.

Origin-airport performance là kết quả đến nơi của các chuyến **xuất phát** tại sân bay đó, không quy nguyên nhân trễ cho sân bay. Route là tuyến có hướng, gộp hãng trong từng tháng; A → B khác B → A.

### Nguy cơ double counting

Bốn bảng là bốn cách nhìn cùng nguồn, không phải bốn tập flights độc lập:

- Không `UNION ALL` bốn bảng rồi cộng counts.
- Không join airline aggregate với route aggregate chỉ theo tháng: quan hệ many-to-many sẽ nhân dòng.
- Grain route hiện tại không có carrier; không giả định có thể phân tích carrier-by-route bằng cách ghép hai aggregate đã mất chiều chi tiết.
- Khi gộp nhiều tháng, giữ YEAR và tổng hợp lại numerator/denominator. Một dòng route-month không tự trở thành ranking gộp cả quý.

## 6. SQL và Python được tổ chức thế nào?

[000_kpi_summary.sql](../sql/analytics/000_kpi_summary.sql) dùng **CTE** (Common Table Expression, kết quả trung gian có tên trong một query) để tách bước đếm khỏi bước tính tỷ lệ.

Phần counts sử dụng **GROUPING SETS** với bốn grain rõ ràng. Cùng một định nghĩa completed/delayed được dùng cho bốn cấp tổng hợp, giảm nguy cơ copy logic không nhất quán. `GROUPING(column)` xác định chiều có được rollup hay không; không đoán cấp aggregate chỉ từ một giá trị NULL.

Các file [001](../sql/analytics/001_monthly_performance.sql), [002](../sql/analytics/002_airline_monthly_performance.sql), [003](../sql/analytics/003_origin_airport_monthly_performance.sql), [004](../sql/analytics/004_route_monthly_performance.sql) chỉ chọn level và các cột cần xuất. Chúng không áp ngưỡng volume, nên reconciliation vẫn bao phủ toàn bộ nguồn.

`COUNT(CASE WHEN … THEN 1 END)` chỉ đếm dòng thỏa điều kiện vì CASE còn lại trả NULL và COUNT bỏ NULL. `CAST(... AS DOUBLE) / NULLIF(denominator, 0)` giữ phép chia dạng tỷ lệ và tránh chia 0. Việc không điền kết quả NULL bằng 0 giữ được khác biệt giữa “không có population” và “population có nhưng không trễ”.

[build_analytics.py](../scripts/build_analytics.py) điều phối SparkSession, đọc Parquet, chạy SQL, gọi [analytics_validation.py](../src/airline_analytics/analytics_validation.py), ghi/đọc lại và preview. Python không thay phần KPI bằng một loạt DataFrame aggregation riêng.

`flights_cleaned`, `kpi_summary` và các output views là temporary views trong session. Shared summary được cache `MEMORY_AND_DISK` rồi giải phóng. Chỉ bốn outputs lưu Parquet Snappy, không directory partition và `coalesce(1)` cho mỗi bảng aggregate nhỏ. Không có warehouse/catalog hay framework mới; output và report đã tồn tại bị từ chối ghi đè.

GROUPING SETS có thể chia sẻ cách tính, nhưng vẫn tạo nhiều nhóm và cần shuffle. Project chưa đo benchmark để khẳng định tăng tốc cụ thể.

## 7. Ba truy vấn phân tích tái sử dụng aggregate

### 7.1. Airline ranking

**Câu hỏi:** Trong cùng tháng, hãng đủ volume nào có arrival delay rate thấp hơn?

[005_airline_ranking.sql](../sql/analytics/005_airline_ranking.sql) dùng CTE eligible và `DENSE_RANK() OVER (PARTITION BY YEAR, MONTH ORDER BY arrival_delay_rate ASC)`. **Hạng 1 là tỷ lệ thấp nhất**; ties cùng hạng, không tạo khoảng trống thứ hạng.

Xếp hạng dùng rate chưa làm tròn. `LEFT JOIN` đưa rank trở lại toàn bộ airline-months; nhóm dưới ngưỡng vẫn hiển thị counts/rate nhưng rank NULL. Carrier code chỉ ổn định thứ tự hiển thị, không phá tie của rank.

Default SQL là 30 completed flights/tháng, điều chỉnh bằng `--min-completed-flights`. Đây là heuristic, không bảo đảm ý nghĩa thống kê. Rate thấp không tự chứng minh hãng vận hành tốt hơn sau khi điều chỉnh route mix, thời tiết hoặc lịch bay.

### 7.2. Month-over-Month

**Câu hỏi:** Arrival delay rate của từng hãng thay đổi thế nào so với tháng lịch liền trước?

[006_airline_mom.sql](../sql/analytics/006_airline_mom.sql) dùng `LAG()` partition theo hãng, order theo YEAR/MONTH để lấy tháng, counts và rate trước đó. LAG trả **dòng quan sát trước**, chưa chắc tháng liền trước. Vì thế query còn kiểm tra `previous_month = ADD_MONTHS(month_start, -1)`.

~~~text
Chênh lệch điểm phần trăm = 100 × (rate hiện tại − rate trước)
Thay đổi tương đối (%)   = 100 × (rate hiện tại − rate trước) / rate trước
~~~

Ví dụ minh họa thuần công thức, không phải kết quả hãng thực tế: từ 10% lên 12% là **+2 điểm phần trăm**, nhưng **+20% tương đối**.

Tháng đầu hoặc thiếu tháng liền trước → chênh lệch NULL. Rate trước bằng 0 vẫn có thể tính điểm phần trăm, nhưng không tính relative change. Cờ `both_months_meet_volume_heuristic` cảnh báo quy mô; query không lọc lịch sử trước LAG làm mất tháng cần so sánh. Ba tháng không chứng minh xu hướng dài hạn.

### 7.3. Tuyến có observed delay rate cao

**Câu hỏi:** Tuyến có hướng nào có tỷ lệ trễ cao trong tháng, với đủ số completed?

[007_high_delay_routes.sql](../sql/analytics/007_high_delay_routes.sql) lọc volume trên route aggregate rồi `DENSE_RANK` theo rate giảm dần trong từng tháng. Khác airline query, hạng 1 ở đây là rate cao nhất.

Dùng `WHERE` vì nguồn đã ở đúng grain route-month. Thêm GROUP BY/HAVING cùng grain chỉ để thể hiện cú pháp không mang lại ý nghĩa mới. Counts và rate đi cùng rank; không tạo ngưỡng rủi ro tuyệt đối hoặc mô hình dự báo.

| Phạm vi | Volume heuristic và chiều so sánh |
| --- | --- |
| SQL preview 005–007 | Default 30 completed/tháng; airline thấp nhất trước, route cao nhất trước. |
| Dashboard airline ranking hiện được owner mô tả | 1.000 completed trong phạm vi lọc; hiển thị nhóm có delay rate cao. |
| Dashboard route table | 300 completed cho từng `YEAR, MONTH, ORIGIN, DEST`; không phải ngưỡng gộp quý. |

Các ngưỡng phục vụ cách trình bày khác nhau, không thay đổi count/rate gốc. Không so hai rankings mà bỏ qua ngưỡng, filter context và chiều xếp hạng.

### Ranh giới evidence của business previews

JSON ghi cả ba query là `PREVIEW_EXECUTED`. Runner mặc định hiển thị tối đa 20 dòng mỗi query, không lưu ba persisted tables mới. JSON không giữ các dòng ranking hoặc AA MoM; `by_month` của airline/route trong report chỉ là rollup toàn bộ nhóm.

Vì vậy chưa đưa ví dụ số cụ thể cho airline ranking, AA MoM hay January routes nếu chưa có các dòng preview tương ứng. Cần month, keys, counts, unrounded rate và rank/change để đối chiếu. Một preview thành công không đồng nghĩa đã review toàn bộ kết quả ba tháng.

## 8. Kết quả và validation đã ghi nhận

| Bằng chứng | Kết quả |
| --- | --- |
| Input | 1.645.503 dòng, schema 38 cột |
| Preflight, input, pre-write, write, read-back | PASS |
| Totals/months/status đối chiếu Phase 2 | Khớp |
| Unique/non-NULL grain và counts không âm | PASS trong các checks đã lưu |
| Rate đúng denominator/formula/NULL handling | PASS |
| Read-back cả bốn bảng | Schema/counts khớp; `all_values_unchanged = true` |
| Failed checks; errors | Không có trong evidence |
| Runtime | Spark 3.5.8, `local[2]`, 8 shuffle partitions |

Exit code 0 do owner xác nhận; JSON không chứa process exit code. Các bảng output và số dòng tương ứng được ghi ở mục 5.

Pre-write đối chiếu counts và signed sums của từng bảng airline/origin/route theo tháng với monthly table. Read-back chạy lại hợp đồng aggregate và so sánh **EXCEPT ALL hai chiều**, căn chỉnh cột trước so sánh. ALL giữ multiplicity nên kiểm tra được cả số lần xuất hiện của giá trị, không chỉ tập distinct.

Counts và dữ liệu read-back so sánh chính xác. Các công thức floating-point dùng tolerance `1E-12 × max(1, |expected|)`; rollup signed sums dùng relative tolerance `1E-12` và absolute tolerance `1E-8`. Đây là xử lý sai số cộng số thực, không phải làm tròn hay sửa rate.

PASS chỉ bao phủ hợp đồng đã triển khai. Reconciliation có thể không phát hiện một sai lầm nghiệp vụ được lặp nhất quán trong mọi aggregate; vẫn cần đọc SQL và hiểu denominator. Không có cam kết dữ liệu hoàn hảo hoặc production benchmark.

### Findings theo tháng có đủ counts

Nguồn trực tiếp: `read_back.tables.monthly_performance.by_month`. Rates là phép tính từ các counts bên cạnh.

| Tháng 2025 | Total N | Completed C | Delayed D | D/C | Cancelled X | X/N |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 01 | 539.747 | 522.269 | 98.130 | 18,79% | 16.312 | 3,02% |
| 02 | 504.884 | 496.476 | 103.102 | 20,77% | 7.405 | 1,47% |
| 03 | 600.872 | 592.301 | 116.034 | 19,59% | 6.923 | 1,15% |

Tháng 02 có tỷ lệ trễ cao nhất trong ba tháng; tháng 03 có nhiều delayed flights nhất nhưng rate thấp hơn tháng 02. Cancellation rate giảm qua ba tháng quan sát. Những so sánh này chưa xác định nguyên nhân, không chứng minh cải tiến vận hành dài hạn.

## 9. Giữ nhất quán giữa Spark SQL và Power BI

Một measure DAX tương đương về phương pháp có thể là:

~~~dax
Arrival Delay Rate =
DIVIDE(
    SUM(monthly_performance[delayed_arrival_flights]),
    SUM(monthly_performance[completed_flights])
)
~~~

Đây là **mẫu diễn giải**, không phải tuyên bố đã kiểm tra mọi measure của PBIX hiện tại. Không có alternate result bằng 0 để che mẫu số bằng 0. Với average, dùng tổng signed minutes chia tổng observed count. Chọn aggregate phù hợp chiều của visual; monthly table không chứa carrier để đáp ứng mọi carrier filter.

Để owner kiểm chứng dashboard cần đối chiếu counts và rate trong cùng filter context: không lọc, từng tháng, các nhóm có ngưỡng volume và các trường hợp không có dữ liệu. Format percentage không được nhân 100 hai lần. Không lấy average của cột rate trong visual.

Text Key Findings hiện là static, không tự cập nhật theo Month Slicer. Các nhận xét viết cho toàn tháng 01–03 phải ghi phạm vi đó, không diễn giải như kết quả động của tháng vừa chọn. Tỷ trọng delay-cause minutes ở [Phase 3B](phase3b_findings.md) cũng không được gắn nhãn thành tỷ lệ delayed flights.

## 10. Câu hỏi phỏng vấn gắn với phương pháp

- **Vì sao phải giữ counts khi đã có rate?** Để rollup có trọng số đúng và nhìn thấy quy mô nhóm.
- **Vì sao dùng GROUPING thay vì kiểm tra dimension IS NULL?** NULL do rollup khác NULL nguồn; GROUPING cho biết cấp tổng hợp rõ ràng.
- **Vì sao LAG chưa đủ cho MoM?** Dòng trước có thể cách nhiều tháng; cần kiểm tra tính liền kề theo lịch.
- **Vì sao không cộng bốn bảng để ra tổng flights?** Mỗi bảng tổng hợp lại cùng population ở một grain khác.

## Những điểm cần ghi nhớ

- Arrival rates dùng completed; cancellation/diversion rates dùng total.
- On-time nghĩa là dưới ngưỡng 15 phút; average giữ dấu của đến sớm.
- Counts cộng được trong cùng grain tương thích; rates/averages phải tính lại từ thành phần.
- Rank, ngưỡng volume và filter context là các phần của định nghĩa phân tích.
- Validation SQL đã PASS trong evidence; mọi DAX/filter interaction và toàn bộ business previews chưa được chứng minh chỉ bằng JSON đó.
