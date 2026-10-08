# Airline Operations Analytics & Flight Delay Prediction

Portfolio project định hướng vị trí **Data Analyst Intern / Fresher**, tập trung vào Business Analytics, SQL, Power BI và các kết quả định lượng có thể kiểm chứng. Apache Spark và Machine Learning được sử dụng để thể hiện năng lực xử lý dữ liệu và dự đoán, nhưng không làm tăng độ phức tạp hệ thống nếu không tạo thêm giá trị phân tích.

> **Trạng thái hiện tại:** Phase 0 — Project Planning & Environment (**đang chuẩn bị, chưa hoàn thành**).
>
> Chưa có pipeline, test, truy vấn phân tích hoặc mô hình nào được chạy. Việc kiểm tra môi trường đang chờ người dùng tự thực hiện và cung cấp output.

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
| Ngôn ngữ | Python | Chờ xác minh phiên bản cài trên máy |
| Xử lý dữ liệu | Apache Spark / PySpark | Chưa cài hoặc kiểm tra |
| Phân tích | Spark SQL | Dự kiến |
| Lưu trữ | Apache Parquet | Dự kiến |
| Machine Learning | Spark MLlib | Dự kiến Phase 5 |
| Dashboard | Power BI | Dự kiến Phase 4 |
| Tests | pytest | Dự kiến từ Phase cần validation |
| Version control | Git / GitHub | Chờ xác minh Git local |

### Phiên bản đề xuất, chưa được xác nhận

- Python 3.11.x.
- Java 17 LTS.
- PySpark 3.5.7.
- pytest `>=8,<9`.

Đây là cấu hình thận trọng cho Spark chạy local trên Windows 11. Tài liệu chính thức của PySpark 3.5.7 cho biết phiên bản này hỗ trợ Python 3.8 trở lên và Java 8, 11 hoặc 17. Project chọn Java 17 và đề xuất Python 3.11 để có một tổ hợp hiện đại nhưng không cần chuyển sang kiến trúc hoặc tính năng Spark 4.x.

Các phiên bản trên chỉ được chốt sau khi người dùng chạy checklist môi trường và gửi output. Không cài dependency trước bước xác minh này.

Tham khảo: [PySpark 3.5.7 Installation](https://spark.apache.org/docs/3.5.7/api/python/getting_started/install.html).

## 5. Kiến trúc repository

### File hiện có sau phần chuẩn bị Phase 0

```text
airline_operations_analytics/
├── .gitignore          # Loại trừ môi trường local, dữ liệu và artifacts lớn
├── MASTER_PROMPT.md    # Quy tắc và phạm vi triển khai project
├── README.md           # Tài liệu project và trạng thái theo Phase
└── requirements.txt    # Dependency tối thiểu được đề xuất, chưa cài đặt
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
| Phase 0 | Scope, cấu trúc tối thiểu, README, environment plan, requirements | **Đang thực hiện — chờ environment output và xác nhận** |
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
- [x] Dependency tối thiểu đã được đề xuất nhưng chưa cài đặt.
- [x] `.gitignore` đã được chuẩn bị để tránh đưa dữ liệu và artifacts local lên Git.
- [ ] Người dùng đã cung cấp output kiểm tra thư mục làm việc.
- [ ] Người dùng đã cung cấp output phiên bản Python, Java, Git và trạng thái PySpark.
- [ ] Tổ hợp phiên bản cuối cùng đã được xác nhận từ output thực tế.
- [ ] Người dùng đã xác nhận Phase 0 đạt yêu cầu.

Phase 0 chưa hoàn thành cho đến khi các mục còn lại được người dùng kiểm tra và xác nhận.

## 10. Environment checklist — người dùng tự chạy

Chạy từng nhóm lệnh trong **PowerShell** tại thư mục project và gửi lại toàn bộ output. Các lệnh này chỉ đọc thông tin; chưa cài dependency và chưa chạy pipeline.

### 10.1. Xác nhận thư mục và file hiện có

```powershell
Get-Location
Get-ChildItem -Force
```

### 10.2. Kiểm tra Python và pip

```powershell
python --version
py -0p
python -m pip --version
where.exe python
```

### 10.3. Kiểm tra Java

```powershell
java -version
$env:JAVA_HOME
where.exe java
```

### 10.4. Kiểm tra Git

```powershell
git --version
git status --short
```

### 10.5. Kiểm tra PySpark mà không cài đặt

```powershell
python -m pip show pyspark
```

Nếu lệnh báo không tìm thấy Python, Java, Git hoặc PySpark, giữ nguyên thông báo lỗi và gửi lại; chưa tự cài hoặc thay đổi cấu hình ở bước này.

## 11. Chưa được chạy ở thời điểm hiện tại

Không chạy các lệnh sau cho đến khi output môi trường được xem xét và tổ hợp phiên bản được xác nhận:

- Tạo virtual environment.
- Cài `requirements.txt`.
- Khởi tạo SparkSession.
- Tải dataset BTS.
- Chạy script, pipeline hoặc test.
- Git commit hoặc Git push.

## 12. Phase 0 handoff

Sau khi người dùng gửi output trong mục 10, bước tiếp theo vẫn thuộc **Phase 0**:

1. Đối chiếu phiên bản Python, Java, Git và PySpark.
2. Điều chỉnh `requirements.txt` nếu cần.
3. Ghi kết quả xác minh thực tế vào README.
4. Cung cấp hướng dẫn tạo môi trường và cài dependency để người dùng tự chạy.
5. Chờ output cài đặt/kiểm tra và chỉ kết luận Phase 0 khi có bằng chứng.

Không bắt đầu Phase 1 nếu chưa có yêu cầu tiếp tục rõ ràng.
