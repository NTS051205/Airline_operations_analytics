# Airline Operations Analytics & Flight Delay Prediction

Portfolio project định hướng vị trí **Data Analyst Intern / Fresher**, tập trung vào Business Analytics, SQL, Power BI và các kết quả định lượng có thể kiểm chứng. Apache Spark và Machine Learning được sử dụng để thể hiện năng lực xử lý dữ liệu và dự đoán, nhưng không làm tăng độ phức tạp hệ thống nếu không tạo thêm giá trị phân tích.

> **Trạng thái hiện tại:** Phase 0 — Project Planning & Environment (**đã hoàn tất phần chuẩn bị và xác minh kỹ thuật cơ bản, đang chờ người dùng xác nhận chốt Phase**).
>
> Môi trường và Spark Smoke Test đã được người dùng tự chạy thành công. Chưa kiểm thử pipeline dữ liệu BTS, đọc/ghi Parquet, unit tests, Power BI hoặc Machine Learning.

**GitHub repository:** [NTS051205/Airline_operations_analytics](https://github.com/NTS051205/Airline_operations_analytics) — các file ban đầu đã xuất hiện trên branch `main` theo xác nhận của người dùng.

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
- **MVP dự kiến:** tháng 01, 02 và 03 năm 2025.
- **Mở rộng dự kiến:** cả năm 2025, chỉ sau khi pipeline MVP đã được người dùng chạy và kiểm chứng.
- **Định dạng nguồn dự kiến:** CSV tải trực tiếp từ BTS.
- **Định dạng xử lý dự kiến:** Parquet.

Chưa có dữ liệu nào được tải hoặc kiểm tra trong Phase 0. Tên cột, schema, kiểu dữ liệu, số dòng và chất lượng dữ liệu chỉ được xác nhận sau khi người dùng cung cấp file thực tế ở Phase 1.

## 3. Mục tiêu phân tích

### Business Analytics và SQL

- Xây dựng định nghĩa KPI với numerator, denominator, điều kiện lọc và cách xử lý `NULL` rõ ràng.
- So sánh hiệu suất hãng hàng không, sân bay và tuyến bay.
- Phân tích xu hướng theo tháng, ngày trong tuần và giờ khởi hành theo lịch.
- Tách nhất quán chuyến hoàn thành, chuyến hủy và chuyến chuyển hướng.
- Thực hiện các phép tổng hợp quan trọng bằng Spark SQL.
- Xuất bảng tổng hợp có grain rõ ràng cho Power BI, tránh double counting.

Các KPI dự kiến gồm:

- Total Scheduled Flights.
- Completed Flights.
- On-Time Arrival Rate.
- Arrival Delay Rate (`>= 15` phút).
- Cancellation Rate.
- Diversion Rate.
- Average Arrival Delay Minutes.
- Delay rate theo airline, airport, route, month, day of week và scheduled departure hour.

Định nghĩa chính thức của từng KPI sẽ được hoàn thiện sau khi kiểm tra schema và ý nghĩa trường dữ liệu ở Phase 1. Không dùng chung một denominator cho mọi KPI.

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
| Phân tích | Spark SQL | Đã xác minh truy vấn cơ bản trên DataFrame in-memory; chưa chạy dữ liệu BTS |
| Lưu trữ | Apache Parquet | Dự kiến |
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

Đây là cấu hình đã được người dùng xác minh cho Spark chạy local trên Windows 11. SparkSession, DataFrame API và Spark SQL cơ bản đã hoạt động với tổ hợp phiên bản trên. Kết quả này chỉ xác nhận môi trường Spark tối thiểu, không chứng minh pipeline dữ liệu của project đã hoạt động.

Tham khảo: [PySpark 3.5.8 Documentation](https://spark.apache.org/docs/3.5.8/api/python/).

## 5. Kiến trúc repository

### File hiện có sau phần chuẩn bị Phase 0

```text
airline_operations_analytics/
├── .gitignore          # Loại trừ môi trường local, dữ liệu và artifacts lớn
├── MASTER_PROMPT.md    # Quy tắc và phạm vi triển khai project
├── README.md           # Tài liệu project và trạng thái theo Phase
└── requirements.txt    # Dependency tối thiểu, đồng bộ với môi trường đã xác minh
```

### Cấu trúc dự kiến, chưa được tạo

```text
data/
├── raw/                # CSV gốc từ BTS; dự kiến Phase 1
├── processed/          # Parquet đã làm sạch; dự kiến Phase 2
└── analytics/          # Bảng KPI cho Power BI; dự kiến Phase 3
docs/                   # Data dictionary, business rules và findings khi phát sinh
models/                 # Model artifacts và metrics; dự kiến Phase 5
powerbi/                # PBIX và dashboard screenshots; dự kiến Phase 4
scripts/                # Entry points, chỉ tạo ở Phase sử dụng
src/airline_analytics/  # Module Python, chỉ tạo theo nhu cầu từng Phase
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

Đây là kiến trúc dự kiến, chưa phải pipeline đã triển khai hoặc chạy thành công.

## 7. Quản lý bằng chứng và kết quả định lượng

Mọi claim dùng trong README hoặc CV phải truy vết được về query, output, bảng kết quả hoặc báo cáo đánh giá. Không tạo số liệu minh họa như thể là kết quả thật.

Các nhóm metric cần lưu ở Phase phù hợp:

| Nhóm | Ví dụ | Trạng thái |
|---|---|---|
| Data | raw rows, valid rows, rejected rows, dataset size | Chưa có dữ liệu |
| Quality | nulls, duplicate candidates, invalid values | Chờ Phase 1–2 |
| Business | delay rate, cancellation rate, airline/route differences | Chờ Phase 3 |
| ML | precision, recall, F1, ROC-AUC, baseline comparison | Chờ Phase 5 |
| Performance | processing time, Spark config, number of runs | Chỉ ghi nếu benchmark hợp lệ |

Các kết quả chưa được người dùng chạy và cung cấp output phải ghi là `pending` hoặc `not run`.

## 8. Roadmap và trạng thái

| Phase | Nội dung | Trạng thái |
|---|---|---|
| Phase 0 | Scope, cấu trúc tối thiểu, README, environment, requirements, Spark Smoke Test | **Sẵn sàng chốt — chờ người dùng xác nhận** |
| Phase 1 | Data acquisition, schema profiling, data dictionary, quality findings | Chưa bắt đầu |
| Phase 2 | Cleaning, validation, cleaned Parquet | Chưa bắt đầu |
| Phase 3 | Spark SQL KPI, analytical tables, business findings | Chưa bắt đầu |
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
- [ ] Người dùng đã xác nhận Phase 0 đạt yêu cầu.

Phase 0 chưa được đánh dấu hoàn thành cho đến khi người dùng kiểm tra README và xác nhận chốt Phase.

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

Hai cảnh báo không ngăn Smoke Test chạy thành công. Hiện chưa áp dụng workaround hoặc thêm dependency vì chưa có bằng chứng chúng gây lỗi cho phạm vi local của project. Cần đánh giá lại nếu đọc/ghi Parquet hoặc pipeline sau này thất bại.

## 11. Những hạng mục chưa kiểm thử

- Đọc CSV thực tế từ BTS.
- Kiểm tra schema và chất lượng dữ liệu BTS.
- Đọc hoặc ghi Parquet.
- Cleaning và validation pipeline.
- Spark SQL KPI queries trên dữ liệu thực tế.
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
- [ ] Người dùng xác nhận chốt Phase 0.

Sau khi người dùng xác nhận, README có thể đánh dấu Phase 0 là hoàn thành. Phase 1 chỉ bắt đầu khi có yêu cầu tiếp tục rõ ràng.

**Dừng tại đây:** không bắt đầu Phase 1, không tải dữ liệu và không chạy thêm test khi chưa có xác nhận của người dùng.
