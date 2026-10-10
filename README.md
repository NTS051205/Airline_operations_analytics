# Airline Operations Analytics & Flight Delay Prediction

Portfolio project định hướng vị trí **Data Analyst Intern / Fresher**, tập trung vào Business Analytics, SQL, Power BI và các kết quả định lượng có thể kiểm chứng. Apache Spark và Machine Learning được sử dụng để thể hiện năng lực xử lý dữ liệu và dự đoán, nhưng không làm tăng độ phức tạp hệ thống nếu không tạo thêm giá trị phân tích.

> **Trạng thái hiện tại:** Phase 3A — Spark SQL KPI & Core Analytics (**implementation and runtime validation completed; pending Git commit**).

> Phase 0, 1 và 2 đã hoàn thành và push GitHub theo xác nhận của project owner. Phase 1: `3403d4d`; Phase 2 code: `6c50b00`, documentation: `4a9e907`. Phase 3A đã chạy thành công, exit code `0`, validation `PASS`; đang chờ Git commit. Phase 3B và Phase 4 chưa bắt đầu.

## Phase 1 — Verified profiling results

Nguồn bằng chứng là `artifacts/phase1/profile_summary.json`, được tạo bởi lần chạy profiler thành công của project owner. Trong phần này:

- **JSON direct** là số liệu được JSON cung cấp trực tiếp.
- **Derived from JSON** là phép tính số học từ các số liệu trực tiếp; đây không phải một Spark check bổ sung.

### Dataset coverage

| Period | Records | Evidence |
|---|---:|---|
| January 2025 | 539,747 | JSON direct |
| February 2025 | 504,884 | JSON direct |
| March 2025 | 600,872 | JSON direct |
| **January–March 2025** | **1,645,503** | **JSON direct** |

Input có **36 cột**. Tổng ba tháng `539,747 + 504,884 + 600,872 = 1,645,503` là phép đối chiếu derived from JSON.

### Flight status

| Status | Records | Evidence |
|---|---:|---|
| Completed | 1,611,046 | JSON direct |
| Canceled | 30,640 | JSON direct |
| Diverted | 3,817 | JSON direct |

Ba trạng thái cộng lại đúng 1,645,503 records. Đây là đối chiếu số lượng, không phải kết luận về hiệu quả vận hành.

### Data-quality checks

| Check | Result | Evidence |
|---|---:|---|
| Cast failures | 0 | JSON direct |
| Exact duplicate groups / rows in groups / excess rows | 0 / 0 / 0 | JSON direct |
| Candidate-key collision groups / rows in groups / excess rows | 0 / 0 / 0 | JSON direct |
| Confirmed-invalid-rule violations | 0 | JSON direct |
| Suspicious-business-rule violations | 0 | JSON direct |

Không phát hiện violation trong **các rule đã cấu hình** không đồng nghĩa dữ liệu hoàn hảo. Zero cast failures xác nhận khả năng parse theo schema đã cấu hình, không chứng minh mọi giá trị đều đúng về mặt nghiệp vụ. Candidate key là khóa ứng viên của project, không phải primary key chính thức do BTS công bố.

### Missingness and extreme value

- Missing `ARR_DEL15`: **34,457** — JSON direct.
- `30,640 canceled + 3,817 diverted = 34,457` — derived from JSON. Hai tổng số khớp nhau ở mức aggregate; điều này tự nó không chứng minh quan hệ row-by-row.
- `CANCELLATION_CODE` và năm cột delay-cause là các trường có điều kiện. `NULL` ở các trường này không mặc định là lỗi.
- Maximum `ARR_DELAY`: **3,407 phút** — JSON direct. Đây là giá trị cực đoan cần xem xét ở phase sau, chưa có bằng chứng để kết luận là dữ liệu sai.

### Descriptive rate

- Completed flights với `ARR_DEL15 = 1`: **317,266** — JSON direct.
- Arrival Delay Rate among completed flights: `317,266 / 1,611,046 ≈ 19.69%` — derived from JSON.

Mẫu số của tỷ lệ trên là **completed flights**, không phải toàn bộ raw records. Tại Phase 1, đây là thống kê mô tả từ profiling. Phase 3A sau đó đã xác minh cùng delayed/completed counts trong SQL aggregate baseline reconciliation.

Chi tiết và limitations được ghi tại [Phase 1 Data Quality Report](docs/data_quality_report.md). Phase 1 đã được project owner xác nhận hoàn thành và push tại commit `3403d4d`. **Không có cleaning, Parquet pipeline, Power BI hay Machine Learning nào được xác nhận trong Phase 1.**

> **Roadmap checkpoint:** Phase 0–2 đã hoàn thành. Phase 3A implementation và runtime validation đã hoàn thành; chờ Git commit. Các monthly findings bên dưới có bằng chứng JSON; ba business queries mới được bounded preview, chưa đánh giá toàn bộ kết quả ba tháng. Phase 3B và các phase sau chưa bắt đầu.
>
> Phase 2 đã giữ nguyên 1,645,503 records qua cleaning và Parquet read-back, xác minh cleaned schema 38 cột, 15/15 pre-write checks và 9/9 read-back checks PASS.

**GitHub repository:** [NTS051205/Airline_operations_analytics](https://github.com/NTS051205/Airline_operations_analytics) — Phase 1 và Phase 2 đã được commit và push; các commit ở trên do project owner cung cấp.

## 1. Business problem

Chậm chuyến làm giảm hiệu quả vận hành của hãng hàng không và ảnh hưởng trực tiếp đến trải nghiệm hành khách. Project sử dụng dữ liệu chuyến bay thực tế của Hoa Kỳ để đo lường hiệu suất đúng giờ, xác định các nhóm có rủi ro chậm chuyến cao và đánh giá khả năng dự đoán chuyến bay đến trễ trước thời điểm khởi hành theo lịch.

Project cần trả lời các câu hỏi chính:

1. Hiệu quả đúng giờ khác nhau như thế nào giữa các hãng hàng không?
2. Sân bay, tuyến bay và khung giờ nào có tỷ lệ chậm chuyến cao?
3. Tình trạng chậm chuyến thay đổi như thế nào theo tháng và ngày trong tuần?
4. Các nhóm nguyên nhân được BTS ghi nhận đóng góp như thế nào vào tổng số phút chậm?
5. Có thể dự đoán một chuyến bay đến trễ từ 15 phút trở lên bằng thông tin có sẵn trước giờ khởi hành hay không?
6. Kết quả phân tích hỗ trợ đề xuất vận hành nào có thể đo lường và truy vết về dữ liệu?

Phân tích mô tả mối liên hệ trong dữ liệu; không diễn giải tương quan thành quan hệ nhân quả khi chưa có phương pháp chứng minh phù hợp.

## 2. Dataset và phạm vi MVP

- **Nguồn:** U.S. Bureau of Transportation Statistics (BTS).
- **Dataset:** Reporting Carrier On-Time Performance.
- **Trang tải chính thức:** [BTS TranStats](https://transtats.bts.gov/DL_SelectFields.aspx?QO_fu146_anzr=&gnoyr_VQ=FGJ).
- **MVP hiện tại:** tháng 01, 02 và 03 năm 2025.
- **Mở rộng dự kiến:** cả năm 2025, chỉ sau khi pipeline MVP đã được người dùng chạy và kiểm chứng.
- **Định dạng nguồn:** CSV tải trực tiếp từ BTS.
- **Định dạng xử lý:** Snappy-compressed Parquet, đã write/read-back thành công trong Phase 2.

Ba file raw CSV January–March 2025 đã được profiling thành công trong Phase 1: 1,645,503 records, 36 cột và zero cast failures dưới contract đã kiểm tra. Phase 2 giữ nguyên grain một raw row thành một cleaned row và đã xác minh 1,645,503 records cùng schema 38 cột sau Parquet read-back.

Acquisition evidence, file sizes và SHA-256 được ghi tại [Phase 1 Data Quality Report](docs/data_quality_report.md). Schema contract được ghi tại [Phase 1 Data Dictionary](docs/data_dictionary.md).

## 3. Mục tiêu phân tích

### Business Analytics và SQL

- Xây dựng định nghĩa KPI với numerator, denominator, điều kiện lọc và cách xử lý `NULL` rõ ràng.
- So sánh hiệu suất hãng hàng không, sân bay và tuyến bay.
- Phân tích xu hướng theo tháng, ngày trong tuần và giờ khởi hành theo lịch.
- Tách nhất quán chuyến hoàn thành, chuyến hủy và chuyến chuyển hướng.
- Thực hiện các phép tổng hợp quan trọng bằng Spark SQL.
- Xuất bảng tổng hợp có grain rõ ràng cho Power BI, tránh double counting.

Các KPI của project gồm (phần theo ngày/giờ vẫn dành cho Phase 3B):

- Total Scheduled Flights.
- Completed Flights.
- On-Time Arrival Rate.
- Arrival Delay Rate (`>= 15` phút).
- Cancellation Rate.
- Diversion Rate.
- Average Arrival Delay Minutes.
- Delay rate theo airline, airport, route, month, day of week và scheduled departure hour.

Định nghĩa KPI Phase 3A nằm tại [KPI Definitions](docs/kpi_definitions.md), gồm numerator, denominator, grain và NULL handling. Arrival Delay Rate dùng completed flights; Cancellation/Diversion Rate dùng total flights. Bốn bảng aggregate đã được runtime validation và Parquet read-back xác minh. Day-of-Week, Scheduled Hour và Delay Cause Analysis thuộc Phase 3B, chưa triển khai.

### Power BI

Dashboard dự kiến có hai trang phân tích chính:

1. **Executive Overview:** tổng số chuyến, tỷ lệ đúng giờ, tỷ lệ chậm, tỷ lệ hủy, xu hướng theo tháng và so sánh hãng.
2. **Delay Analysis:** chậm chuyến theo sân bay, tuyến bay, ngày trong tuần, giờ khởi hành và nhóm nguyên nhân được ghi nhận.

Trang Prediction Insights chỉ được thêm sau khi mô hình ML hoạt động và các metric đã được người dùng tự chạy, kiểm chứng.

### Machine Learning

Bài toán dự kiến là binary classification: dự đoán chuyến bay có đến trễ từ 15 phút trở lên hay không bằng các trường có sẵn trước giờ khởi hành theo lịch.

- Baseline: majority-class hoặc dummy baseline.
- Model chính: Logistic Regression bằng Spark MLlib.
- Model so sánh tùy chọn: Random Forest, chỉ khi model chính đã hoạt động ổn định.
- Evaluation: Precision, Recall, F1, confusion matrix, ROC-AUC và PR-AUC khi phù hợp.
- Split: temporal split; không chia ngẫu nhiên toàn bộ dữ liệu.
- Data leakage: không sử dụng actual delay, actual time hoặc delay-cause fields làm features.

ML là phần bổ sung kỹ thuật. Kết quả Business Analytics và dashboard vẫn là trọng tâm của project.

## 4. Tech stack

| Thành phần | Lựa chọn | Trạng thái |
|---|---|---|
| Hệ điều hành | Windows 11 | Đã xác minh bởi người dùng |
| Ngôn ngữ | Python 3.11.9 | Đã xác minh trong `.venv` |
| Java | Eclipse Temurin JDK 17.0.20.1 | Đã xác minh; `JAVA_HOME` được cấu hình đúng |
| Xử lý dữ liệu | Apache Spark / PySpark 3.5.8 | Đã cài; Spark Smoke Test thành công |
| Phân tích | Spark SQL | Phase 3A pipeline PASS; ba business queries đã bounded preview |
| Lưu trữ | Apache Parquet | Phase 2 cleaned và Phase 3A aggregates write/read-back PASS; chỉ cleaned partition `YEAR`/`MONTH` |
| Machine Learning | Spark MLlib | Dự kiến Phase 5 |
| Dashboard | Power BI | Dự kiến Phase 4 |
| Tests | pytest 8.4.2 | Đã cài; chưa chạy unit tests |
| Version control | Git / GitHub | Repository đã có file ban đầu trên branch `main` |

### Phiên bản môi trường đã xác minh

- OS: Windows 11.
- Python: 3.11.9.
- Java: Eclipse Temurin JDK 17.0.20.1.
- Virtual environment: `.venv`.
- PySpark: 3.5.8.
- pytest: 8.4.2.

Đây là cấu hình đã được người dùng xác minh cho Spark chạy local trên Windows 11. Phase 0 xác nhận SparkSession/DataFrame/Spark SQL cơ bản; Phase 2 sau đó xác nhận riêng cleaning pipeline và Parquet write/read-back trên dữ liệu BTS.

Tham khảo: [PySpark 3.5.8 Documentation](https://spark.apache.org/docs/3.5.8/api/python/).

## 5. Kiến trúc repository

### File hiện có sau Phase 3A implementation và runtime validation

```text
airline_operations_analytics/
├── .gitignore          # Loại trừ môi trường local, dữ liệu và artifacts lớn
├── MASTER_PROMPT.md    # Quy tắc và phạm vi triển khai project
├── README.md           # Tài liệu project và trạng thái theo Phase
├── requirements.txt    # Dependency tối thiểu, đồng bộ với môi trường đã xác minh
├── data/raw/           # Ba CSV BTS local; không commit lên Git
├── docs/
│   ├── data_dictionary.md
│   ├── data_quality_report.md
│   ├── phase2_cleaning_report.md
│   └── kpi_definitions.md
├── scripts/
│   ├── profile_raw_data.py
│   ├── smoke_test_parquet.py
│   ├── build_cleaned_parquet.py
│   └── build_analytics.py
├── sql/analytics/
│   ├── 000_kpi_summary.sql
│   ├── 001_monthly_performance.sql
│   ├── 002_airline_monthly_performance.sql
│   ├── 003_origin_airport_monthly_performance.sql
│   ├── 004_route_monthly_performance.sql
│   ├── 005_airline_ranking.sql
│   ├── 006_airline_mom.sql
│   └── 007_high_delay_routes.sql
└── src/airline_analytics/
    ├── __init__.py
    ├── schema.py
    ├── ingestion.py
    ├── profiling.py
    ├── quality_checks.py
    ├── cleaning.py
    ├── validation.py
    └── analytics_validation.py
```

### Output local đã xác minh trong Phase 2 và Phase 3A

```text
data/processed/flights_cleaned/             # Parquet local; đã write/read-back thành công
artifacts/phase2/validation_summary.json     # Validation evidence; overall PASS
data/analytics/                             # Bốn analytical Parquet outputs đã xác minh
artifacts/phase3/validation_summary.json     # Phase 3A validation evidence; overall PASS
```

### Output chưa tạo và cấu trúc dành cho các Phase sau

```text
models/                 # Model artifacts và metrics; dự kiến Phase 5
powerbi/                # PBIX và dashboard screenshots; dự kiến Phase 4
tests/                  # Tests, chỉ tạo cùng business rule hoặc transformation cần test
```

Các thư mục dự kiến không được tạo trước. Mỗi thư mục hoặc module chỉ xuất hiện khi Phase tương ứng thực sự cần đến nó.

## 6. Luồng dữ liệu dự kiến

```text
BTS CSV
  -> schema profiling và data-quality checks
  -> Spark cleaning và validation
  -> cleaned Parquet
  -> Spark SQL KPI tables
  -> Power BI analytical model

cleaned Parquet
  -> leakage-safe feature engineering
  -> temporal train/validation/test split
  -> baseline và Spark MLlib model
  -> evaluation metrics
```

Phase 1 profiling, Phase 2 cleaning/Parquet validation và Phase 3A Spark SQL analytics đã chạy thành công. Cleaned output nằm tại `data/processed/flights_cleaned/`; bốn analytical tables nằm tại `data/analytics/` và đều đã vượt qua read-back schema/count/value checks.

## 7. Quản lý bằng chứng và kết quả định lượng

Mọi claim dùng trong README hoặc CV phải truy vết được về query, output, bảng kết quả hoặc báo cáo đánh giá. Không tạo số liệu minh họa như thể là kết quả thật.

Các nhóm metric cần lưu ở Phase phù hợp:

| Nhóm | Ví dụ | Trạng thái |
|---|---|---|
| Data | raw rows, cleaned rows, read-back rows, dataset size | Raw/cleaned/read-back đều 1,645,503; dataset-size benchmark chưa thực hiện |
| Quality | nulls, duplicate candidates, invalid values | Phase 1 profiling và Phase 2 validation đã xác minh theo các rules được triển khai |
| Business | delay rate, cancellation rate, airline/route differences | Phase 3A aggregates đã xác minh; monthly findings từ JSON; airline/route examples chờ bằng chứng preview chi tiết |
| ML | precision, recall, F1, ROC-AUC, baseline comparison | Chờ Phase 5 |
| Performance | processing time, Spark config, number of runs | Chỉ ghi nếu benchmark hợp lệ |

Các kết quả chưa được người dùng chạy và cung cấp output phải ghi là `pending` hoặc `not run`.

## 8. Roadmap và trạng thái

| Phase | Nội dung | Trạng thái |
|---|---|---|
| Phase 0 | Scope, cấu trúc tối thiểu, README, environment, requirements, Spark Smoke Test | **Hoàn thành — người dùng đã xác nhận** |
| Phase 1 | Data acquisition, schema profiling, data dictionary, quality findings | **Hoàn thành — committed and pushed (`3403d4d`)** |
| Phase 2 | Cleaning, validation, cleaned Parquet | **Completed, committed and pushed (`6c50b00`, `4a9e907`)** |
| Phase 3A | Core KPI tables, airline ranking, MoM, directed-route analysis | **Implementation and runtime validation completed; pending Git commit** |
| Phase 3B | Day-of-Week, Scheduled Hour, Delay Cause Analysis | Not started |
| Phase 4 | Power BI model, hai trang dashboard, metric verification | Chưa bắt đầu |
| Phase 5 | Feature engineering, baseline, Logistic Regression, evaluation | Chưa bắt đầu |
| Phase 6 | Final insights, documentation, CV bullets, interview preparation | Chưa bắt đầu |

Không chuyển sang Phase tiếp theo khi chưa có xác nhận của người dùng. README phải được cập nhật trong từng Phase trước khi Phase đó được đánh dấu hoàn thành.

## 9. Phase 0 acceptance criteria

- [x] Phạm vi MVP và business questions đã được mô tả.
- [x] Tech stack được giữ đúng phạm vi đã thống nhất.
- [x] Cấu trúc repository tối thiểu và kiến trúc dự kiến đã được phân biệt rõ.
- [x] README ghi rõ phần đã hoàn thành và phần dự kiến.
- [x] Dependency tối thiểu đã được cài và phiên bản thực tế đã được ghi nhận.
- [x] `.gitignore` đã được chuẩn bị để tránh đưa dữ liệu và artifacts local lên Git.
- [x] Người dùng đã xác nhận môi trường Windows 11 và virtual environment `.venv`.
- [x] Người dùng đã cung cấp kết quả phiên bản Python, Java, PySpark và pytest.
- [x] `JAVA_HOME` đã được người dùng xác nhận cấu hình đúng.
- [x] SparkSession, DataFrame API, `df.count()` và Spark SQL cơ bản đã được người dùng smoke test thành công.
- [x] GitHub repository đã có các file ban đầu trên branch `main`.
- [x] Người dùng đã xác nhận Phase 0 đạt yêu cầu.

Phase 0 đã hoàn thành. Không thay đổi lại phạm vi hoặc kết quả Phase 0 nếu không có lý do cụ thể.

## 10. Kết quả xác minh Phase 0

### Environment

| Hạng mục | Kết quả thực tế | Trạng thái |
|---|---|---|
| OS | Windows 11 | Verified |
| Python | 3.11.9 | Verified |
| Java | Eclipse Temurin JDK 17.0.20.1 | Verified |
| `JAVA_HOME` | Đã cấu hình đúng | Verified |
| Virtual environment | `.venv` | Verified |
| PySpark | 3.5.8 | Verified |
| pytest | 8.4.2 | Installed; unit tests not run |

### Spark Smoke Test

| Kiểm tra | Kết quả thực tế |
|---|---|
| Khởi tạo SparkSession | Thành công |
| Spark version | 3.5.8 |
| DataFrame test data | 3 records |
| `df.count()` | Trả về `3` |
| Spark SQL | Thành công; trả về `AA`, `DL`, `UA` |
| Dừng Spark | Chương trình kết thúc sau `spark.stop()` |

Smoke Test xác nhận Python có thể khởi tạo SparkSession và thực hiện thao tác DataFrame/Spark SQL cơ bản. Test này không đọc dữ liệu BTS, không đọc/ghi Parquet và không chạy pipeline của project.

### Cảnh báo Windows đã quan sát

- `winutils.exe not found`.
- `Native Hadoop library unavailable`.

Hai cảnh báo không ngăn Spark Smoke Test Phase 0 chạy thành công. Sau đó, project owner đã cấu hình Hadoop 3.3.4 Windows utilities và xác minh Parquet write/read trong Phase 2. Các binary này chỉ nằm trong môi trường local, không commit vào repository.

## 11. Những hạng mục chưa kiểm thử

- Đọc ba CSV BTS thực tế: **đã kiểm thử thành công trong Phase 1**.
- Chạy Spark profiler trên toàn bộ ba CSV BTS: **đã kiểm thử thành công, exit code `0`**.
- Xác minh logical types và các quality rules đã cấu hình: **đã hoàn thành trong Phase 1**.
- Đọc/ghi Parquet với Snappy và partition `YEAR`/`MONTH`: **đã kiểm thử thành công trong Phase 2**.
- Cleaning và validation pipeline: **PASS, exit code `0`; 15/15 pre-write và 9/9 read-back checks**.
- Spark SQL KPI queries trên dữ liệu thực tế: **Phase 3A PASS; ba business queries đã bounded preview, chưa đánh giá toàn bộ kết quả ba tháng**.
- Unit tests bằng pytest.
- Power BI analytical model và dashboard.
- Feature engineering, model training và Machine Learning evaluation.
- Processing-time benchmark.

Không có business metric, data-quality metric hoặc Machine Learning metric nào được ghi nhận ở Phase 0.

## 12. Phase 0 final checklist

- [x] Business problem và business questions được mô tả.
- [x] Scope MVP là tháng 01–03/2025.
- [x] Kiến trúc dự kiến được phân biệt với thành phần đã triển khai.
- [x] Environment versions được ghi nhận từ kết quả thực tế.
- [x] Dependency versions được ghi nhận là PySpark 3.5.8 và pytest 8.4.2.
- [x] Spark Smoke Test và phạm vi của test được ghi nhận.
- [x] Cảnh báo Windows được ghi nhận mà không kết luận quá mức.
- [x] Các hạng mục chưa kiểm thử được liệt kê rõ.
- [x] Chưa ghi business hoặc ML metrics khi chưa có dữ liệu.
- [x] GitHub repository và branch `main` được ghi nhận.
- [x] Người dùng xác nhận chốt Phase 0.

Phase 0, Phase 1 và Phase 2 đã được project owner xác nhận hoàn thành. Phase 3A implementation và runtime validation đã hoàn thành; đang chờ Git commit.

## 13. Phase 1 — Completed

### Verified acquisition facts

| Month | File size | SHA-256 source |
|---|---:|---|
| January 2025 | 111,474,672 bytes | User-provided |
| February 2025 | 104,767,501 bytes | User-provided |
| March 2025 | 124,595,215 bytes | User-provided |
| **Total** | **340,837,388 bytes** | — |

- Three monthly headers match exactly in name and order.
- The confirmed raw schema contains 36 columns.
- The January five-record preview confirms only observed formatting, not full-dataset quality.
- Raw columns will be read as strings; logical casts are measured without modifying source values.
- The profiler validates filename, size, header and SHA-256 before starting Spark.

### Implementation status

- [x] Raw schema contract prepared.
- [x] File-integrity and header validation prepared.
- [x] Raw-string Spark ingestion prepared.
- [x] Overall and per-file missing-value profiling prepared.
- [x] Cast, duplicate, flight-status, domain and categorized anomaly profiling prepared.
- [x] Data dictionary drafted from verified header and BTS meanings.
- [x] Data quality report structure prepared.
- [x] User ran `scripts/profile_raw_data.py` successfully with exit code `0`.
- [x] Record counts and profiling output were verified.
- [x] Data dictionary was finalized from cast results.
- [x] Data quality report and README contain verified Phase 1 results.
- [x] User confirmed Phase 1 complete; commit `3403d4d` was pushed to GitHub.

### User-run profiling command

Historical reproducibility command for Phase 1. It was run by the project owner, not by Codex.

```powershell
$projectPython = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$sparkSubmit = (Resolve-Path ".\.venv\Scripts\spark-submit.cmd").Path

$env:PYSPARK_PYTHON = $projectPython
$env:PYSPARK_DRIVER_PYTHON = $projectPython
$env:PYTHONPATH = (Resolve-Path ".\src").Path

& $sparkSubmit `
    --master "local[2]" `
    --driver-memory "4g" `
    ".\scripts\profile_raw_data.py" `
    --input-dir ".\data\raw" `
    --output ".\artifacts\phase1\profile_summary.json"

$sparkExitCode = $LASTEXITCODE
Write-Host "spark-submit exit code: $sparkExitCode"
```

The two `PYSPARK_*` variables ensure both the local driver and Python workers use Python 3.11 from `.venv`. They are set only for the current PowerShell session. The script reads raw CSVs only, writes one ignored JSON profiling artifact, and does not create cleaned data, Parquet, KPI tables, models, or dashboard files.

Phase 1 and Phase 2 are closed, committed and pushed. Phase 3A implementation and runtime validation are complete; Git commit is pending.

## 14. Phase 2 — Data Cleaning & Parquet

### Current status

- [x] Cleaning rules implemented with Spark DataFrame expressions.
- [x] Safe integer conversion prepared for binary flags represented as `"0.00"`/`"1.00"`.
- [x] `flight_status` derivation prepared without filtering cancelled or diverted flights.
- [x] Pre-write validation and Phase 1 baseline reconciliation prepared.
- [x] Snappy Parquet write prepared with `YEAR`/`MONTH` partitions and existing-output refusal.
- [x] Parquet read-back validation prepared.
- [x] Small Windows Parquet smoke-test script prepared.
- [x] Phase 2 documentation prepared.
- [x] Project owner ran the Parquet smoke test: PASS, exit code `0`.
- [x] Project owner ran the full cleaning pipeline: PASS, exit code `0`.
- [x] Project owner reviewed `artifacts/phase2/validation_summary.json` and supplied the verified runtime results.
- [x] Cleaned Parquet counts and 38-column schema were verified by read-back.
- [x] Phase 2 code and documentation have been committed and pushed (`6c50b00`, `4a9e907`).
- [x] Project owner has confirmed Phase 2 complete.

Phase 2 is complete, including owner sign-off, implementation, runtime validation, commit and push.

### Verified Phase 2 results

| Validation metric | Result |
|---|---:|
| Parquet smoke test | PASS; exit code `0` |
| Full pipeline | PASS; exit code `0` |
| Raw rows | 1,645,503 |
| Cleaned rows | 1,645,503 |
| Parquet read-back rows | 1,645,503 |
| January / February / March | 539,747 / 504,884 / 600,872 |
| Completed / Cancelled / Diverted / Invalid | 1,611,046 / 30,640 / 3,817 / 0 |
| Cleaned schema | 38 columns |
| Introduced cast `NULL`s | 0 |
| `ARR_DEL15` row-level violations | 0 |
| Pre-write checks | 15/15 PASS |
| Read-back checks | 9/9 PASS |
| Failed checks / errors | 0 / 0 |

Output: `data/processed/flights_cleaned/`, written/configured with Snappy compression and partitioned by `YEAR` and `MONTH`. Write/read succeeded on Windows 11 with PySpark 3.5.8 after Hadoop 3.3.4 Windows utilities were configured locally; those binaries are not committed to the repository.

Các kết quả này xác minh những rule Phase 2 đã triển khai và một lần chạy local thành công. Chúng không chứng minh dữ liệu hoàn hảo, không phải production benchmark, và không phải business KPI hoặc Machine Learning result. Kết quả Business Analytics Phase 3A được ghi riêng bên dưới; Machine Learning và production benchmark chưa thực hiện.

### Cleaning behavior

- Blank/whitespace-only source strings become `NULL`; conditional nulls are retained.
- Calendar fields and numeric IDs become integers; `FL_DATE` becomes `DateType`.
- Delay, distance, and delay-cause measures become doubles.
- Four binary flags become integer `0`/`1`/`NULL` without truncating other numeric values.
- Four HHmm columns remain strings, preserving leading zeros and literal `2400`.
- The output keeps all 36 BTS columns, `_source_file`, and derived `flight_status`.
- No deduplication, imputation, outlier capping, KPI aggregation, or ML feature engineering is performed.

Detailed rules and verified runtime evidence are documented in [Phase 2 Cleaning and Parquet Report](docs/phase2_cleaning_report.md).

### PowerShell environment check

Run from the repository root:

```powershell
Set-Location "E:\airline_operations_analytics"

& ".\.venv\Scripts\python.exe" --version
java -version
& ".\.venv\Scripts\python.exe" -c "import pyspark; print('PySpark', pyspark.__version__)"

Test-Path ".\artifacts\phase1\profile_summary.json"
Test-Path ".\data\processed\flights_cleaned"
Test-Path ".\artifacts\phase2\validation_summary.json"
```

For the verified run, the Phase 1 profile existed and both Phase 2 output paths were absent before execution. After success, the Phase 2 paths exist. The scripts continue to refuse overwriting existing evidence or Parquet output.

### Verified Parquet smoke-test command

This three-row synthetic test completed with PASS and exit code `0`. It checks the local Windows Parquet write/read path only and does not validate BTS data. The command below documents the verified run; a rerun requires a new output path or deliberate owner-managed cleanup.

```powershell
$projectPython = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$sparkSubmit = (Resolve-Path ".\.venv\Scripts\spark-submit.cmd").Path

$env:PYSPARK_PYTHON = $projectPython
$env:PYSPARK_DRIVER_PYTHON = $projectPython
$env:PYTHONPATH = (Resolve-Path ".\src").Path

& $sparkSubmit `
    --master "local[2]" `
    --driver-memory "1g" `
    ".\scripts\smoke_test_parquet.py" `
    --output-dir ".\artifacts\phase2\parquet_smoke_test"

$smokeExitCode = $LASTEXITCODE
Write-Host "Parquet smoke-test exit code: $smokeExitCode"
```

The script retains its output for inspection and does not delete it after failure. If Windows emits a blocking Hadoop/native-library error, keep the full error output; do not install unverified `winutils.exe` or Hadoop binaries.

### Verified full Phase 2 command

The command below completed with PASS and exit code `0`. It is retained for reproducibility. Because the current output and JSON now exist, the safety checks will refuse an identical rerun unless the project owner deliberately supplies new output paths or manages the existing artifacts.

```powershell
$projectPython = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$sparkSubmit = (Resolve-Path ".\.venv\Scripts\spark-submit.cmd").Path

$env:PYSPARK_PYTHON = $projectPython
$env:PYSPARK_DRIVER_PYTHON = $projectPython
$env:PYTHONPATH = (Resolve-Path ".\src").Path

& $sparkSubmit `
    --master "local[2]" `
    --driver-memory "4g" `
    ".\scripts\build_cleaned_parquet.py" `
    --input-dir ".\data\raw" `
    --phase1-profile ".\artifacts\phase1\profile_summary.json" `
    --output-dir ".\data\processed\flights_cleaned" `
    --validation-output ".\artifacts\phase2\validation_summary.json"

$phase2ExitCode = $LASTEXITCODE
Write-Host "Phase 2 exit code: $phase2ExitCode"
```

Review command used for the generated report:

```powershell
Get-Content ".\artifacts\phase2\validation_summary.json" -Raw
```

The verified evidence includes raw/cleaned/read-back counts, month counts, flight-status reconciliation, schema types, cast/null checks, binary and HHmm checks, row-level `ARR_DEL15` checks, write/read-back status, failed checks, and `overall_status`.

## 15. Phase 3A — Spark SQL KPI & Core Analytics

**Status: Implementation and runtime validation completed; pending Git commit.**

Evidence: `artifacts/phase3/validation_summary.json`, `generated_at_utc = 2026-10-10T05:35:56.597113+00:00`. The JSON records `overall_status = PASS`, `failure_stage = null`, and 1,645,503 input records. Pipeline exit code `0` was confirmed by the project owner; the exit code is not a JSON field.

The runner reads only `data/processed/flights_cleaned/` as flight data and the existing Phase 2 JSON as a read-only count baseline. It does not rerun acquisition, profiling or cleaning.

| Parquet output under `data/analytics/` | Grain | Verified pre-write / read-back rows |
|---|---|---:|
| `monthly_performance/` | `YEAR, MONTH` | 3 / 3 |
| `airline_monthly_performance/` | `YEAR, MONTH, OP_UNIQUE_CARRIER` | 42 / 42 |
| `origin_airport_monthly_performance/` | `YEAR, MONTH, ORIGIN` | 991 / 991 |
| `route_monthly_performance/` | `YEAR, MONTH, ORIGIN, DEST` | 16,997 / 16,997 |

Input, pre-write, write and read-back all recorded **PASS**. The recorded checks cover schema, nonnegative counts, grain uniqueness, status/label reconciliation, denominator formulas, NULL handling and monthly reconciliation. All four read-back tables have `all_values_unchanged = true`, the bidirectional `EXCEPT ALL` check. Every recorded `failed_checks` list is empty, all check booleans are true, and `errors` is empty: **0 failed checks; 0 errors**. PASS is limited to these implemented checks, not perfect data or a production benchmark.

All four outputs keep flight counts, unrounded 0–1 rates, signed average arrival delay, and its sum/non-null count for correct weighted rollups. They use Snappy Parquet with no directory partitioning and at most one data part file per table. Existing output directories and validation reports are refused; partial outputs are not deleted after failure.

`000_kpi_summary.sql` centralizes KPI definitions using four explicit `GROUPING SETS`; `001`–`004` select the corresponding grain. Only the aggregated summary is cached, with memory-and-disk fallback. The four sets are separate summaries of the same flights: never add their totals together.

Three non-persisted SQL analyses reuse validated aggregate read-back views:

- `005`: monthly airline `DENSE_RANK`, lowest unrounded delay rate first; low-volume groups stay visible with a NULL rank.
- `006`: airline `LAG` comparison with a consecutive-calendar-month check; separate percentage-point difference and relative percentage change.
- `007`: directed routes ranked by observed delay rate descending, with a default 30-completed-flights/month heuristic. There is no predictive risk score or causal conclusion.

The shared `--min-completed-flights` setting controls volume flags/eligibility for these analyses only; it never filters the four KPI outputs. The verified run used 30, an adjustable heuristic that does not guarantee statistical reliability. All three queries recorded `PREVIEW_EXECUTED`; only bounded previews were produced, and the complete three-month business-query results have not been reviewed. No fifth persisted table was created.

See [KPI Definitions and verified Phase 3A results](docs/kpi_definitions.md) for NULL handling, interpretation, evidence paths and limitations.

### Business Findings — verified monthly observations

Counts below come directly from `read_back.tables.monthly_performance.by_month` in the JSON. Percentages are **derived from those counts**, rounded to two decimal places for display; they are not literal rate fields in the report.

| Month (2025) | Delayed / completed flights | Arrival Delay Rate | Cancelled / total flights | Cancellation Rate |
|---|---:|---:|---:|---:|
| January | 98,130 / 522,269 | 18.79% | 16,312 / 539,747 | 3.02% |
| February | 103,102 / 496,476 | 20.77% | 7,405 / 504,884 | 1.47% |
| March | 116,034 / 592,301 | 19.59% | 6,923 / 600,872 | 1.15% |

February had the highest observed arrival delay rate of these three months. March had more delayed arrivals than February but a lower delay rate because its completed-flight denominator was larger. Cancellation rates decreased across the three observed months. These are descriptive comparisons, not explanations of causes, predictions, or evidence of a long-term trend.

**Preview evidence still needed for specific examples:** the JSON stores execution status only for `005`–`007`; its airline/route `by_month` sections are rollups across all carriers/routes. They do not contain carrier rankings, AA month-over-month rows, or January route rows. Those three example groups remain undocumented pending the owner's existing console previews; no carrier, route, rank or AA change is inferred from monthly totals. Full three-month business-query interpretation also remains outside the bounded preview evidence.

### User-run command — retained for reproducibility

The owner confirmed the Phase 3A pipeline completed with exit code `0`. JSON records PySpark `3.5.8`, `local[2]` and eight shuffle partitions. The command below is retained for reproducibility, not a request to rerun this completed phase. Use the working Phase 2 Java/Hadoop configuration; Codex has not executed these commands.

```powershell
Set-Location "E:\airline_operations_analytics"

$projectPython = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$sparkSubmit = (Resolve-Path ".\.venv\Scripts\spark-submit.cmd").Path
$env:PYSPARK_PYTHON = $projectPython
$env:PYSPARK_DRIVER_PYTHON = $projectPython
$env:PYTHONPATH = (Resolve-Path ".\src").Path

& $projectPython --version
java -version
& $projectPython -c "import pyspark; print(pyspark.__version__)"
Test-Path ".\data\processed\flights_cleaned"
Test-Path ".\artifacts\phase2\validation_summary.json"
Test-Path ".\data\analytics"
Test-Path ".\artifacts\phase3\validation_summary.json"

& $sparkSubmit `
    --master "local[2]" `
    --driver-memory "4g" `
    ".\scripts\build_analytics.py" `
    --input-dir ".\data\processed\flights_cleaned" `
    --phase2-report ".\artifacts\phase2\validation_summary.json" `
    --output-dir ".\data\analytics" `
    --validation-output ".\artifacts\phase3\validation_summary.json" `
    --min-completed-flights 30 `
    --preview-rows 20

$phase3ExitCode = $LASTEXITCODE
Write-Host "Phase 3A exit code: $phase3ExitCode"
Get-Content ".\artifacts\phase3\validation_summary.json" -Raw
```

The analytics output directory and Phase 3 report now exist after the successful run. An identical rerun is refused by the existing-output safeguards. Any deliberate rerun requires fresh output/report paths or owner-managed handling of prior artifacts; there is no automatic deletion. An argument/path preflight refusal exits before creating a report. In a failed future run, the reached stage is recorded and partial outputs are retained; stages not reached remain `NOT_RUN`.

### Acceptance checklist — verified execution; Git commit pending

- [x] Four core aggregate SQL outputs and three business SQL queries prepared.
- [x] KPI definitions, denominator rules and validation code prepared.
- [x] Owner ran and verified the Phase 3A pipeline: exit code `0`, JSON `PASS`.
- [x] Monthly totals reconcile to 1,645,503 and the Phase 2 status/month counts.
- [x] Airline/origin/route counts reconcile to monthly; schema, grain, formula and NULL checks pass.
- [x] All four Parquet outputs pass read-back schema/count/value checks.
- [x] All three business queries executed bounded previews.
- [x] Monthly findings are documented from JSON counts with derived rates labeled.
- [ ] Carrier ranking, AA MoM and January route examples await the existing preview rows as evidence.
- [ ] Full three-month business-query results have been reviewed (not claimed by preview execution).
- [ ] Phase 3A changes have been committed to Git.

**Current stop point:** Phase 3A implementation and runtime validation completed; pending Git commit. Specific carrier/route examples await preview evidence. Phase 3B, Power BI and Machine Learning have not started.
