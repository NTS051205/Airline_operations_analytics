# Phân tích theo thứ, khung giờ và nhóm phút trễ

## 1. Phạm vi và nguồn bằng chứng

Ba phân tích bổ sung trả lời: **lịch bay nào có tỷ lệ trễ cao hơn, nhóm nào có nhiều chuyến bị ảnh hưởng hơn và phút trễ được báo cáo tập trung ở đâu?**

Phạm vi chính là **January–December 2025**. Input duy nhất: `data/processed/flights_cleaned_2025_reviewed/`; không đọc lại raw và không đưa rejected records vào KPI.

| Evidence / output local | Vai trò |
| --- | --- |
| `artifacts/phase3b_2025_reviewed/validation_summary.json` | Input counts, monthly source totals, validation và Q1 regression |
| `artifacts/phase3b_2025_reviewed/day_of_week/` | CSV theo YEAR, MONTH, DAY_OF_WEEK |
| `artifacts/phase3b_2025_reviewed/departure_time_band/` | CSV theo YEAR, MONTH, time_band |
| `artifacts/phase3b_2025_reviewed/delay_causes/` | CSV theo YEAR, MONTH, cause_category |
| `artifacts/phase3b/` | CSV/JSON Q1 lịch sử, giữ nguyên cho regression |

Counts và phút trong JSON là kết quả trực tiếp. Tổng năm/share tính từ các monthly source totals được ghi rõ là phép tính suy ra. Nhận xét từ screenshot không thay cho exported rows hoặc kiểm chứng DAX.

Population input đã đối chiếu: **7.001.618 total**, **6.879.484 completed**, **102.876 cancelled**, **19.258 diverted**, **1.534.638 delayed arrivals**. KPI chung xem [KPI Methodology](kpi_definitions.md).

## 2. Day-of-Week Performance

### Câu hỏi và phương pháp

**Ngày nào trong tuần có arrival delay rate cao hơn trong từng tháng?**

[008_day_of_week.sql](../sql/analytics/008_day_of_week.sql) nhóm theo YEAR/MONTH/DAY_OF_WEEK; quy ước BTS 1 = thứ Hai, …, 7 = Chủ nhật. Một nhóm là tất cả ngày cùng thứ trong tháng, không phải một ngày lịch duy nhất.

~~~text
Arrival Delay Rate = completed có ARR_DEL15 = 1 / completed
~~~

Total giữ cả cancelled/diverted; denominator arrival chỉ dùng completed. SQL dùng NULLIF để mẫu số 0 trả NULL. Các nhóm không xuất hiện không được tạo thêm dòng 0 giả.

Output giữ ba keys, total/completed/delayed counts và rate. Validation đối chiếu coverage, domain 1–7, unique grain và tổng counts từng tháng.

### Diễn giải count và rate

Ví dụ **Q1 lịch sử** đã được ghi nhận từ CSV `artifacts/phase3b/day_of_week/`:

| Nhóm | Delayed / completed | Rate |
| --- | ---: | ---: |
| Thứ Hai, January | 16.002 / 70.684 | 22,64% |
| Thứ Sáu, January | 17.196 / 88.355 | 19,46% |

Thứ Sáu có nhiều delayed flights hơn nhưng rate thấp hơn. Count hỗ trợ đánh giá quy mô vấn đề; rate hỗ trợ so sánh tỷ trọng. Không lấy trung bình các weekday rates để ra rate tháng.

Heatmap dashboard cả năm cho thấy mức độ trễ thay đổi theo cả tháng và thứ; các ô cao không chứng minh thứ trong tuần gây trễ. Không coi ví dụ Q1 trên là ranking cả năm. Muốn quyết định nguồn lực theo ngày cần counts của chính nhóm, cơ cấu hãng/tuyến/sân bay và các điều kiện vận hành liên quan.

## 3. Scheduled Departure Time Bands

### Vì sao dùng scheduled time?

[009_departure_time_band.sql](../sql/analytics/009_departure_time_band.sql) dùng `CRS_DEP_TIME`, không dùng `DEP_TIME`. Giờ actual có thể đã dịch chuyển vì trễ; dùng actual để grouping sẽ đổi câu hỏi từ “lịch dự kiến nào” sang “đã khởi hành lúc nào”.

HHmm là giờ địa phương tại sân bay xuất phát, không phải UTC chung.

| Nhóm | Quy tắc |
| --- | --- |
| Night | 0000–0559 và giá trị chính xác 2400 |
| Morning | 0600–1159 |
| Afternoon | 1200–1759 |
| Evening | 1800–2359 |
| Missing | CRS_DEP_TIME IS NULL |
| Invalid | Có giá trị nhưng sai HHmm bốn chữ số và không phải 2400 |

SQL kiểm tra format trước khi so sánh chuỗi. `2400` chỉ được gán Night để grouping, không đổi ngày/tháng. Blank còn sót là Invalid; cleaning đã chuẩn hóa blank thành NULL.

Output giữ total/completed/delayed, `scheduled_2400_flights` và arrival rate. Missing/Invalid nếu xuất hiện vẫn tham gia reconciliation, không bị loại khỏi total.

### Kết quả và ranh giới diễn giải

Trong ví dụ **March 2025 của Q1**, CSV đã ghi:

| Khung giờ | Delayed / completed | Arrival delay rate |
| --- | ---: | ---: |
| Night | 1.732 / 19.989 | 8,66% |
| Morning | 30.416 / 228.314 | 13,32% |
| Afternoon | 47.542 / 206.404 | 23,03% |
| Evening | 36.344 / 137.594 | 26,41% |

Evening có rate cao nhất, Afternoon có nhiều delayed flights nhất. Đây là ví dụ March, không phải rates cả năm. Dashboard cả năm cũng hiển thị hai nhóm này ở hai vị trí tương ứng, nhưng exact annual band rates không được suy từ độ dài thanh.

Dashboard Power BI đã hoàn tất theo xác nhận của chủ dự án. Định nghĩa của phân tích này là `SUM(delayed_arrival_flights) / SUM(completed_flights)`: đo **arrival** delay theo **scheduled departure band**, không phải departure-delay rate.

### Boundary cases cả năm

JSON `input.by_month` ghi:

- Missing: **0** trong mọi tháng.
- Invalid: **0** trong mọi tháng.
- `midnight_2400`: **1 trong July**, các tháng khác 0.

Vì vậy câu “không có 2400” chỉ đúng cho **Q1**, không đúng cho cả năm. Có 48 output rows, tương ứng bốn nhóm thông thường trong 12 tháng; 2400 thuộc Night, không tạo nhóm thứ năm.

Các nhánh Missing/Invalid vẫn chưa có quan sát khác 0 trong lần chạy này. Không dùng PASS để khẳng định đã kiểm nghiệm runtime mọi edge case.

## 4. Delay Cause Breakdown

### Câu hỏi và population

**Trong completed có ARR_DEL15 = 1, nhóm báo cáo nào chiếm nhiều attributed delay minutes nhất?**

| Cột | Nhóm báo cáo |
| --- | --- |
| CARRIER_DELAY | Hoạt động hãng |
| WEATHER_DELAY | Thời tiết |
| NAS_DELAY | Hệ thống hàng không quốc gia |
| SECURITY_DELAY | An ninh |
| LATE_AIRCRAFT_DELAY | Máy bay đến muộn từ chặng trước |

Các nhóm này là phân loại BTS, không phải nguyên nhân do project tự suy luận. WEATHER_DELAY không đo mọi ảnh hưởng trực tiếp/gián tiếp của thời tiết.

### Logic và tránh đếm trùng

[010_delay_causes.sql](../sql/analytics/010_delay_causes.sql) dùng:

1. `eligible`: lọc completed, label 1; xác định any/all cause fields được báo cáo.
2. `STACK(5, …)`: chuyển năm cột cause thành năm category rows cho mỗi chuyến.
3. GROUP BY tháng/category: COUNT giá trị được báo cáo và SUM phút.
4. Window SUM theo tháng: giữ grain category-month nhưng có tổng phút tháng làm denominator.

~~~text
cause_minute_share
    = attributed_minutes(category, month)
      / NULLIF(monthly_attributed_minutes, 0)
~~~

`eligible_delayed_flights`, any/all coverage và mẫu số tháng lặp trên năm categories. **Không cộng các flight counts lặp này qua categories**. Một chuyến có thể đóng góp phút cho nhiều nhóm.

`reported_cause_flights` đếm non-NULL, gồm cả reported zero; không phải số chuyến có cause minutes > 0. NULL là chưa báo cáo, 0 là giá trị đã báo cáo. SUM bỏ từng NULL; toàn bộ NULL khác tổng bằng 0. Mẫu số NULL/0 tạo share NULL.

### Kết quả cả năm

Tổng các `minutes_<CATEGORY>` trong 12 dòng `input.by_month` cho:

| Category | Attributed minutes | Share của tổng năm |
| --- | ---: | ---: |
| Late Aircraft | 43.862.971 | 39,19% |
| Carrier | 36.320.477 | 32,45% |
| NAS | 24.489.666 | 21,88% |
| Weather | 7.108.714 | 6,35% |
| Security | 152.802 | 0,14% |
| **Tổng** | **111.934.630** | **100% trước làm tròn** |

Đây là **phép cộng từ monthly evidence**, không average monthly shares. Mỗi share dùng attributed minutes của category chia 111.934.630.

Late Aircraft + Carrier: **80.183.448 / 111.934.630 = 71,63%**, tính từ số phút trước làm tròn. Không cộng 39,19% và 32,45% đã làm tròn để công bố 71,64%.

Carrier dẫn đầu January; Late Aircraft dẫn đầu February và March trong mốc Q1. Không suy ra tỷ trọng cả năm chỉ từ ba tháng đầu. Các kết quả annual hỗ trợ ưu tiên điều tra turnaround, chuỗi chuyến và hoạt động hãng; chưa chứng minh một thay đổi quy trình sẽ giảm bao nhiêu chuyến trễ.

### Reporting coverage

Trong từng tháng, `reported_<CATEGORY>`, `any_reported` và `all_reported` bằng delayed eligible count. Tính toàn năm theo từng category: **1.534.638 / 1.534.638 = 100% non-NULL coverage**, gồm reported zero.

Coverage 100% không có nghĩa mỗi chuyến chịu cả năm nguyên nhân, không bảo đảm phân loại đã giải thích mọi nguyên nhân thực tế. Share phút không phải tỷ lệ flights, và tổng cause minutes không phải signed ARR_DELAY sum của toàn bộ completed.

## 5. Validation, read-back và regression

Nguồn: `artifacts/phase3b_2025_reviewed/validation_summary.json`.

| Kết quả | Giá trị cả năm |
| --- | --- |
| overall_status | PASS |
| Input records | 7.001.618 |
| Checks | 97/97 passed |
| failed_checks; errors | Rỗng |
| Day-of-week: output/read-back | 84 / 84 |
| Departure bands: output/read-back | 48 / 48 |
| Delay causes: output/read-back | 60 / 60 |
| Q1 regression | Cả ba outputs PASS, zero mismatches |
| Runtime | Spark 3.5.8, local[1] |

[run_phase3b.py](../scripts/run_phase3b.py) kiểm tra evidence Phase 2/3A PASS, cùng input path, toàn bộ cleaned schema 38 cột và các trường cần phân tích. Counts total/status theo tháng khớp Phase 2; delayed counts khớp monthly Phase 3A.

Weekday/band aggregates reconcile với monthly nguồn; kiểm tra unique grain, `0 ≤ delayed ≤ completed ≤ total`, rates/NULL/zero denominator và boundary coverage. Cause validation đối chiếu eligible, coverage, phút, mẫu số và share; không cộng eligible qua categories.

CSV không mang schema: read-back dùng schema SQL tường minh, header/FAILFAST và chuỗi NULL thống nhất. Counts và row values/multiplicities được đối chiếu chính xác trên aggregate nhỏ. Q1 regression so keys/counts chính xác, DOUBLE theo absolute tolerance 1E-9 và relative tolerance 1E-12, phân biệt NULL với 0.

Runner chỉ collect các summary có giới hạn, không toàn bộ flights; cache các cột cần thiết và giải phóng khi xong. Output phải là đường dẫn mới, không chồng lên input/evidence/MVP; không tự xóa partial outputs khi lỗi.

Mốc Q1 lịch sử có 21/12/15 rows và 30/30 checks. Không trộn chúng với counts validation cả năm.

## 6. Ý nghĩa kinh doanh và giới hạn

- **Rate và volume cần đọc cùng nhau:** một nhóm rate cao chưa chắc ảnh hưởng nhiều chuyến nhất.
- **Timing là phân đoạn quan sát:** chênh lệch theo thứ/khung giờ chưa tách ảnh hưởng hãng, tuyến, sân bay hay điều kiện vận hành.
- **Cause minutes khoanh vùng điều tra:** hai nhóm lớn nhất chiếm 71,63% phút được phân loại, không chứng minh quyền kiểm soát hoàn toàn hoặc mức tiết kiệm.
- Một năm không đủ xác lập mùa vụ lặp lại; không có dữ liệu chi phí/hành khách để lượng hóa tác động tài chính.
- Arrival rate không bao phủ gián đoạn do hủy/chuyển hướng; đọc cùng cancellation/diversion.
- SQL evidence và xác nhận hoàn tất Power BI là hai nguồn kiểm chứng khác nhau. Text findings tĩnh không tự đổi theo Month slicer.

## Những điểm cần ghi nhớ

- Giữ đúng numerator/denominator khi đổi phạm vi lọc.
- Dùng scheduled time để phân nhóm lịch bay; không nhầm với departure-delay metric.
- Cả năm có một scheduled 2400, dù Q1 không có.
- Minute share, reporting coverage và tỷ lệ chuyến là ba khái niệm khác nhau.
- Quan sát giúp chọn nơi điều tra, chưa chứng minh nhân quả hay hiệu quả can thiệp.
