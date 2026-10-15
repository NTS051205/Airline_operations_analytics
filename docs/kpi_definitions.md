# Phương pháp KPI và Spark SQL Analytics

## 1. Câu hỏi nghiệp vụ và phạm vi

Một sân bay có nhiều chuyến trễ chưa chắc có tỷ lệ trễ cao nhất. **Count** đo quy mô ảnh hưởng; **rate** đo tỷ trọng trong một population xác định. Bộ KPI giữ cả hai để so sánh theo tháng, hãng, sân bay xuất phát và tuyến có hướng.

Phạm vi hiện tại: **January–December 2025**, chỉ đọc `data/processed/flights_cleaned_2025_reviewed/`. Analytics không đọc raw hoặc đưa rejected records trở lại. Nguồn kiểm chứng là `artifacts/phase3_2025_reviewed/validation_summary.json`; Q1 được giữ làm mốc regression.

## 2. Population và kết quả cả năm

| Ký hiệu | Cột aggregate | Định nghĩa | Kết quả |
| --- | --- | --- | ---: |
| N | `total_flights` | Tất cả accepted records | 7.001.618 |
| C | `completed_flights` | flight_status = completed | 6.879.484 |
| X | `cancelled_flights` | flight_status = cancelled | 102.876 |
| V | `diverted_flights` | flight_status = diverted | 19.258 |
| D | `delayed_arrival_flights` | Completed và ARR_DEL15 = 1 | 1.534.638 |
| O | `on_time_arrival_flights` | Completed và ARR_DEL15 = 0 | 5.344.846 |
| A | `arrival_delay_observed_flights` | Completed có ARR_DELAY không NULL | 6.879.484 |
| S | `arrival_delay_minutes_sum` | Tổng ARR_DELAY có dấu trên completed | 58.506.602 phút |

Nguồn trực tiếp: `pre_write.baseline.annual_summary`. Rates bên dưới cũng được lưu ở đây; phép chia minh họa giải thích cách tái tạo từ numerator/denominator, làm tròn hai chữ số khi trình bày.

Các invariant bắt buộc:

~~~text
N = C + X + V
C = D + O
~~~

Completed của analytical dataset phải có ARR_TIME, ARR_DELAY và ARR_DEL15 hợp lệ. Một raw completed thiếu arrival đã được tách ở cleaning; đây không phải phương án giữ missing label rồi đổi denominator. A vẫn được giữ riêng để biểu diễn đúng population của signed average, dù evidence hiện tại có A = C.

## 3. Định nghĩa KPI

| KPI / câu hỏi | Công thức và bộ lọc | Ví dụ cả năm | NULL, cách tính sai và giới hạn |
| --- | --- | --- | --- |
| **Total Flights:** quy mô bao nhiêu? | N = COUNT(*) trên accepted | 7.001.618 | COUNT(ARR_DELAY) sẽ bỏ dòng thiếu; N không phải hành khách hoặc số chuyến thực sự cất cánh |
| **Completed Flights:** bao nhiêu chuyến không hủy/chuyển hướng? | C = count status completed | 6.879.484 | Chỉ CANCELLED = 0 chưa đủ vì còn diverted; nhóm không có completed cho count 0 |
| **Delayed Arrival Flights:** bao nhiêu chuyến đến trễ ≥15 phút? | D = count completed, ARR_DEL15 = 1 | 1.534.638 | ARR_DELAY > 0 là ngưỡng khác; NULL không được gán là on-time |
| **On-Time Arrival Flights:** bao nhiêu completed dưới ngưỡng 15 phút? | O = count completed, ARR_DEL15 = 0 | 5.344.846 | Bao gồm đến sớm và đến muộn dưới 15 phút |
| **Arrival Delay Rate:** tỷ trọng completed đến trễ? | D / NULLIF(C, 0) | 1.534.638 / 6.879.484 = **22,31%** | C = 0 → NULL; chia cho total sẽ đưa cancelled/diverted vào sai population |
| **On-Time Arrival Rate:** tỷ trọng completed không trễ ≥15 phút? | O / NULLIF(C, 0) | 5.344.846 / 6.879.484 = **77,69%** | Chỉ bổ sung với delay rate khi C = D + O; không điền missing label bằng 0 |
| **Cancellation Rate:** tỷ trọng bị hủy trong toàn bộ analytical flights? | X / NULLIF(N, 0) | 102.876 / 7.001.618 = **1,47%** | Lọc completed trước sẽ mất numerator; N = 0 → NULL, X = 0 và N > 0 → 0 |
| **Diversion Rate:** tỷ trọng chuyển hướng? | V / NULLIF(N, 0) | 19.258 / 7.001.618 = **0,28%** | Không gộp diverted vào cancelled; không đo độ trễ cuối hành trình |
| **Average Arrival Delay:** completed sớm/muộn trung bình bao nhiêu phút? | AVG(ARR_DELAY) trên completed; rollup S / NULLIF(A, 0) | 58.506.602 / 6.879.484 = **8,50 phút** | AVG bỏ NULL; không có quan sát → NULL. Không thay ARR_DELAY bằng ARR_DELAY_NEW hoặc lọc chỉ delay dương |

Count là số lượng, không có denominator. Các rate lưu fraction 0–1, chưa làm tròn trong SQL; chỉ format phần trăm ở lớp trình bày.

### Vì sao mẫu số khác nhau?

Arrival performance chỉ xác định cho completed với nhãn hợp lệ. Cancelled/diverted không thể mặc định “đúng giờ” khi không có nhãn arrival. Ngược lại, cancellation/diversion là tỷ trọng của một kết quả trong toàn bộ analytical population, nên dùng N.

Signed average giữ giá trị âm của chuyến đến sớm. **8,50 phút không phải độ trễ trung bình của riêng các chuyến delayed**. Cực trị vẫn được giữ nên average cần được đọc cùng tỷ lệ/count, không đại diện đầy đủ cho phân phối.

### Ratio of sums, không average of percentages

~~~sql
SUM(delayed_arrival_flights)
    / NULLIF(SUM(completed_flights), 0)

SUM(arrival_delay_minutes_sum)
    / NULLIF(SUM(arrival_delay_observed_flights), 0)
~~~

Mỗi tháng/nhóm có volume khác nhau. AVG(rate) cho mỗi nhóm trọng số bằng nhau; ratio of sums cho mỗi completed flight trọng số bằng nhau. Average cũng phải dùng tổng signed minutes chia tổng observed count, không trung bình các average tháng.

## 4. Grain và bốn analytical outputs

Output nằm dưới `data/analytics/2025_reviewed/`; tất cả dùng Parquet Snappy.

| Bảng | Grain | Cột | Dòng cả năm |
| --- | --- | ---: | ---: |
| monthly_performance | YEAR, MONTH | 15 | 12 |
| airline_monthly_performance | YEAR, MONTH, OP_UNIQUE_CARRIER | 16 | 168 |
| origin_airport_monthly_performance | YEAR, MONTH, ORIGIN | 16 | 4.073 |
| route_monthly_performance | YEAR, MONTH, ORIGIN, DEST | 17 | 69.944 |

Số dòng từ evidence read-back; 168 là airline-month rows, không phải số hãng. Mỗi bảng giữ bảy counts, signed sum, signed average và bốn rates.

Bốn bảng là bốn cách tổng hợp **cùng nguồn**:

- Không UNION ALL rồi cộng tổng flights.
- Không join airline và route chỉ theo tháng: many-to-many có thể nhân số dòng.
- Route grain không có carrier; không thể tái tạo carrier-by-route từ hai aggregate đã mất chiều chi tiết.
- A → B khác B → A. Route-month không phải annual route ranking khi chọn nhiều tháng.
- Origin-airport metrics đo kết quả đến nơi của các chuyến xuất phát tại sân bay đó, không quy trách nhiệm trễ cho sân bay.

## 5. SQL và Python

[000_kpi_summary.sql](../sql/analytics/000_kpi_summary.sql) dùng **CTE** để tách count khỏi rate và **GROUPING SETS** để dùng chung định nghĩa cho bốn grain. `GROUPING(column)` xác định cấp tổng hợp, tránh nhầm NULL do rollup với NULL nguồn.

`COUNT(CASE WHEN … THEN 1 END)` chỉ đếm dòng thỏa điều kiện vì CASE còn lại là NULL. `CAST(... AS DOUBLE) / NULLIF(..., 0)` giữ phép chia dạng tỷ lệ và tránh chia 0.

SQL [001](../sql/analytics/001_monthly_performance.sql)–[004](../sql/analytics/004_route_monthly_performance.sql) chọn level/cột để xuất, không áp volume threshold làm mất dữ liệu khi reconciliation. [build_analytics.py](../scripts/build_analytics.py) điều phối; [analytics_validation.py](../src/airline_analytics/analytics_validation.py) kiểm tra.

Shared summary được cache; bốn aggregate nhỏ ghi không partition, coalesce(1). Không cache toàn bộ flight-level input trong runner này. GROUPING SETS vẫn cần shuffle; không có benchmark chứng minh mức tăng tốc.

### Ba truy vấn business trên aggregate

| Query | Business question | Logic và giới hạn |
| --- | --- | --- |
| [005_airline_ranking.sql](../sql/analytics/005_airline_ranking.sql) | Hãng nào có rate thấp hơn trong tháng? | CTE eligible, DENSE_RANK theo rate tăng dần; hạng 1 thấp nhất. Ties cùng hạng; nhóm dưới ngưỡng rank NULL |
| [006_airline_mom.sql](../sql/analytics/006_airline_mom.sql) | Rate hãng thay đổi thế nào so với tháng liền trước? | LAG theo hãng; kiểm tra tháng trước bằng ADD_MONTHS(current, −1), không dùng dòng quan sát trước thay cho tháng lịch trước |
| [007_high_delay_routes.sql](../sql/analytics/007_high_delay_routes.sql) | Route-month nào có rate cao với đủ volume? | Lọc trên aggregate sẵn có, DENSE_RANK giảm dần; không cần GROUP BY/HAVING dư thừa |

MoM dùng `100 × (rate_current − rate_previous)` cho **điểm phần trăm**; relative change chia thêm rate_previous. Rate_previous = 0 hoặc thiếu tháng liền trước → relative change NULL. January 2025 không có December 2024 trong input nên không tạo MoM.

SQL default threshold là **30 completed/tháng**, có thể điều chỉnh. Dashboard dùng **1.000 completed** cho airline ranking và **300 completed cho từng route-month**, đồng thời hiển thị hãng có rate cao nhất. Những khác biệt về threshold/chiều ranking không thay công thức KPI.

Ba query đã preview thành công; preview giới hạn dòng không phải audit toàn bộ rankings/MoM cả năm, không tạo persisted tables mới. Không đưa số liệu AA MoM hoặc rank cụ thể nếu thiếu exported rows tương ứng.

## 6. Validation và Q1 regression

Evidence cả năm ghi overall PASS, errors rỗng; cả bốn bảng pre-write/read-back PASS.

- Input đúng schema 38 cột và cùng cleaned path đã được Phase 2 xác nhận; đủ 12 tháng.
- Status counts theo tháng khớp Phase 2; không invalid status, missing grouping keys, invalid label, completed thiếu arrival hoặc non-finite delay.
- Counts không âm; grain duy nhất; N = C + X + V và C = D + O.
- Các bảng airline/origin/route cộng về monthly; rate/average đúng công thức và NULL handling.
- Parquet read-back khớp schema/counts và **EXCEPT ALL hai chiều**, giữ multiplicity.

Q1 từ full-year được so sánh với bốn bảng MVP tại `data/analytics/`: keys/counts chính xác; DOUBLE dùng absolute tolerance `1E-9`, relative tolerance `1E-12`; NULL chỉ bằng NULL, không bằng 0. Cả bốn regression **PASS, zero mismatches**. Không collect toàn bộ route-level data về driver.

Mốc Q1: total 1.645.503; completed 1.611.046; delayed 317.266; rate suy ra 19,69%. Đây là dữ liệu January–March, không phải annual total. Checks dùng tolerance cho số thực để xử lý sai số tính toán, không để che chênh lệch counts.

## 7. Findings theo tháng từ JSON

Nguồn: `read_back.tables.monthly_performance.by_month`. Rates là phép chia từ counts cạnh nhau.

| Tháng 2025 | Delayed / completed | Arrival delay rate |
| --- | ---: | ---: |
| June | 169.405 / 599.472 | 28,26% |
| July | 177.012 / 612.811 | 28,89% |
| September | 92.859 / 558.328 | 16,63% |
| December | 153.113 / 571.924 | 26,77% |

July cao nhất, September thấp nhất trong 12 tháng. Chênh lệch tính trước làm tròn là **12,25 điểm phần trăm**. January có cancellation rate cao nhất: 16.312 / 539.747 = **3,02%**. Delay và cancellation xác định các mối quan tâm khác nhau; một năm không chứng minh mùa vụ lặp lại.

## 8. Nhất quán với Power BI

Mẫu measure tương đương về phương pháp:

~~~dax
Arrival Delay Rate =
DIVIDE(
    SUM(monthly_performance[delayed_arrival_flights]),
    SUM(monthly_performance[completed_flights])
)
~~~

Dashboard Power BI ba trang đã hoàn tất theo xác nhận của chủ dự án. Đoạn DAX trên minh họa định nghĩa KPI, không phải bản trích xuất measure từ PBIX. Dùng aggregate phù hợp chiều visual, không lấy AVG cột rate; không nhân phần trăm hai lần hoặc điền mẫu số 0 thành 0%.

Khi đọc dashboard, giữ đúng filter context: total hoặc từng tháng, Top 5/ties, threshold 1.000/300 và grain route-month khi chọn nhiều tháng. Text findings tĩnh không tự thay theo Month slicer. Phân tích time-band đo arrival delay theo giờ khởi hành dự kiến, không phải departure delay.

## Những điểm cần ghi nhớ

- Giữ count, numerator/denominator và grain để KPI có thể kiểm toán.
- Arrival rates dùng completed; cancellation/diversion dùng analytical total.
- Signed average giữ đến sớm; rate/average rollup bằng thành phần, không trung bình percentages.
- Các bảng khác grain không phải các tập chuyến độc lập.
- PASS chỉ bao phủ hợp đồng đã triển khai; không chứng minh nhân quả hoặc hiệu quả can thiệp.
