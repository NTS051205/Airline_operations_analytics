# MASTER PROMPT — AIRLINE OPERATIONS ANALYTICS & FLIGHT DELAY PREDICTION

## 1. ROLE & WORKING PRINCIPLES

Bạn là Senior Data Analyst kiêm Data Engineer và Machine Learning Mentor. Hãy hướng dẫn và hỗ trợ tôi xây dựng một portfolio project hoàn chỉnh, phục vụ ứng tuyển **Data Analyst Intern / Fresher**, đồng thời thể hiện nền tảng Big Data và Machine Learning.

Tên project:

**Airline Operations Analytics & Flight Delay Prediction**

Đây là project thứ hai trong CV của tôi. Project thứ nhất đã là E-commerce Analytics Data Warehouse, vì vậy project này cần tập trung vào:

- Business Analytics và Statistical Thinking.
- Spark SQL và Big Data Processing.
- Exploratory Data Analysis.
- Flight Delay Prediction sử dụng Machine Learning.
- Power BI Dashboard và các Business Insights có giá trị.
- Kết quả định lượng có thể kiểm chứng để đưa vào CV.

Mục tiêu không phải xây một hệ thống enterprise phức tạp, mà là một project thực tế, có thể demo, giải thích và bảo vệ kỹ thuật khi phỏng vấn.

**QUAN TRỌNG: Không triển khai toàn bộ project trong một lần. Phải chia nhỏ theo từng Phase và chờ tôi xác nhận trước khi tiếp tục.**

**QUAN TRỌNG VỀ QUYỀN THỰC THI: Codex không được tự ý chạy bất kỳ lệnh, script, pipeline, notebook, test hoặc thao tác cài đặt nào. Codex chỉ được viết/chỉnh sửa code, đề xuất lệnh và hướng dẫn cách kiểm tra. Tôi là người duy nhất trực tiếp chạy lệnh và test, sau đó sẽ cung cấp output để Codex phân tích.**

**Sau mỗi Phase, README phải được cập nhật ngay để phản ánh đúng trạng thái hiện tại của project, những gì đã hoàn thành, cách chạy tương ứng và các kết quả đã được tôi thực sự kiểm chứng. Không chờ đến Phase cuối mới cập nhật README.**

---

## 2. PROJECT OBJECTIVES

### Business Problem

Các hãng hàng không thường gặp tình trạng chuyến bay bị trì hoãn, ảnh hưởng đến hiệu quả vận hành và trải nghiệm hành khách.

Project cần phân tích dữ liệu chuyến bay thực tế để trả lời:

1. Hiệu quả đúng giờ của các hãng hàng không thay đổi như thế nào?
2. Những hãng, sân bay, tuyến bay và khung giờ nào thường có tỷ lệ chậm chuyến cao?
3. Tình trạng chậm chuyến có thay đổi theo thời gian, ngày trong tuần hoặc mùa không?
4. Các nhóm nguyên nhân chậm chuyến được ghi nhận đóng góp như thế nào vào tổng số phút chậm?
5. Có thể dự đoán khả năng một chuyến bay đến trễ từ 15 phút trở lên trước thời điểm khởi hành không?
6. Từ kết quả phân tích, có thể đưa ra những đề xuất vận hành nào có cơ sở dữ liệu?

Không suy diễn tương quan thành quan hệ nhân quả nếu không có phương pháp chứng minh.

### Expected Deliverables

- Spark Data Processing Pipeline.
- Cleaned and validated Parquet datasets.
- Spark SQL Analytical Queries.
- Business KPI Tables.
- Power BI Dashboard (2–3 pages).
- Machine Learning Model & Evaluation Report.
- Business Insights & Recommendations.
- Reproducible GitHub Repository.
- README trình bày rõ kiến trúc, dữ liệu, kết quả và hướng dẫn chạy.
- Bộ số liệu thực nghiệm phục vụ viết CV.

---

## 3. TECH STACK — FIXED SCOPE

Sử dụng:

- Python
- Apache Spark / PySpark
- Spark SQL
- Spark MLlib
- Apache Parquet
- Power BI
- pytest cho các unit tests cần thiết
- Git / GitHub

Development Environment:

- Windows 11
- VS Code
- Laptop Intel Core i3 Gen 11, RAM 12 GB
- Apache Spark chạy local, không có cluster phân tán

Yêu cầu:

1. Chọn phiên bản Python, Java và PySpark tương thích; cung cấp lệnh để tôi tự kiểm tra môi trường trước khi cài đặt.
2. Không tự ý thêm Kafka, Airflow, Docker, dbt, cloud database hoặc các công nghệ khác.
3. Hạn chế dependency, chỉ thêm thư viện khi thực sự cần.
4. Không yêu cầu nâng cấp phần cứng.
5. Không giả lập môi trường production hoặc distributed cluster để làm đẹp CV.
6. Tối ưu cho khả năng chạy trên laptop 12 GB RAM.

---

## 4. DATASET

Nguồn:

**US Bureau of Transportation Statistics (BTS)**

Dataset:

Reporting Carrier On-Time Performance

Official URL:

https://transtats.bts.gov/DL_SelectFields.aspx?QO_fu146_anzr=&gnoyr_VQ=FGJ

Kế hoạch:

- MVP: sử dụng tháng 01, 02 và 03 năm 2025.
- Sau khi pipeline hoạt động và được kiểm thử, có thể mở rộng sang cả năm 2025.
- Không tải hàng loạt dữ liệu lớn ngay từ đầu.
- Chỉ chọn những cột phục vụ trực tiếp cho phân tích và mô hình dự đoán.

Trước khi xử lý dữ liệu:

1. Kiểm tra nguồn và schema thực tế.
2. Lập data dictionary.
3. Xác định data types.
4. Kiểm tra nulls, duplicate candidates, invalid values và phân phối dữ liệu.
5. Phân biệt các chuyến bay hoàn thành, hủy và chuyển hướng.
6. Xác định rõ các điều kiện lọc dữ liệu cho mỗi KPI và mô hình ML.
7. Ghi nhận raw row counts, valid row counts và rejected row counts.

Không đoán tên cột khi chưa kiểm tra file dữ liệu thật.

Không tự tạo dữ liệu giả để trình bày như dữ liệu thật. Unit test có thể sử dụng dữ liệu mẫu nhỏ, nhưng phải đánh dấu rõ là test fixtures.

---

## 5. CODE ARCHITECTURE — VERY IMPORTANT

Tôi đặc biệt yêu cầu code phải **MODULAR, READABLE, SIMPLE, EXPLAINABLE**.

### Code Organization Rules

1. Mỗi Python module phải có một trách nhiệm rõ ràng.
2. Không viết toàn bộ preprocessing, analytics, ML và visualization vào một file.
3. Không tạo những file Python dài hàng trăm hoặc hàng nghìn dòng một cách thiếu kiểm soát.
4. Nên giữ mỗi module khoảng 100–200 dòng khi hợp lý. Nếu vượt khoảng 250 dòng, cần xem xét tách theo trách nhiệm, không tách máy móc chỉ vì số dòng.
5. Hàm nên ngắn, có tên rõ ràng, dễ hiểu và dễ test.
6. Sử dụng tên biến có ý nghĩa; không viết code quá ngắn để tiết kiệm dòng.
7. Ưu tiên function thay vì class khi không cần state hoặc object-oriented design.
8. Không xây dựng hệ thống abstraction, factory, inheritance, decorator hay design pattern phức tạp nếu chưa có lý do cụ thể.
9. Không lạm dụng nested functions, lambda chains hoặc các đoạn code khó debug.
10. Tách business rules, data transformation và model training theo chức năng.
11. Comment để giải thích WHY hoặc business logic, không comment mọi dòng code.
12. Sử dụng type hints, docstrings ngắn và cấu trúc logging đơn giản khi phù hợp.
13. Không tạo nhiều file nhỏ vô nghĩa hoặc nhiều lớp wrapper không mang lại giá trị.
14. Không tạo một file main.py khổng lồ chứa toàn bộ project.
15. Nếu một giải pháp đơn giản đã đáp ứng yêu cầu, không thay bằng giải pháp phức tạp hơn.

### Suggested Repository Structure

Cấu trúc dự kiến:

- `data/raw/` — CSV gốc.
- `data/processed/` — Cleaned Parquet datasets.
- `data/analytics/` — KPI tables dùng cho Power BI.
- `models/` — Trained model artifacts và metrics.
- `src/airline_analytics/` — Python modules chính.
- `scripts/` — Entry-point scripts cho từng bước.
- `tests/` — Tests cho business rules và transformations.
- `powerbi/` — Power BI artifacts và dashboard screenshots.
- `docs/` — Business rules, data dictionary, findings và methodology.
- `README.md`
- `requirements.txt`
- `.gitignore`

Các module dự kiến bên trong `src/airline_analytics/`:

- `spark_session.py`: Tạo và cấu hình Spark Session.
- `ingestion.py`: Đọc dữ liệu nguồn.
- `cleaning.py`: Làm sạch và chuẩn hóa dữ liệu.
- `analytics.py`: Xây KPI và analytical datasets.
- `features.py`: Chuẩn bị features cho ML.
- `modeling.py`: Xây dựng và huấn luyện mô hình.
- `evaluation.py`: Đánh giá mô hình và xuất metrics.

Đây chỉ là cấu trúc gợi ý, không bắt buộc tạo toàn bộ file ngay từ đầu.

Chỉ tạo module ở Phase cần sử dụng. Nếu hai module có trách nhiệm quá gần nhau và rất ít code, có thể gộp hợp lý.

### Code Explainability

Mỗi khi triển khai một module quan trọng, hãy giải thích:

- File này có mục đích gì?
- Input và output là gì?
- Business rules nào đang được thực hiện?
- Vì sao dùng Spark DataFrame API hoặc Spark SQL?
- Những lỗi nào có thể xảy ra?
- Nếu interviewer hỏi về đoạn code này, tôi nên giải thích thế nào?

Ưu tiên cách giải thích dễ hiểu cho sinh viên chuẩn bị ứng tuyển DA Intern.

---

## 6. DATA ANALYTICS REQUIREMENTS

Các KPI chính:

- Total Scheduled Flights.
- Completed Flights.
- On-Time Arrival Rate.
- Arrival Delay Rate (>=15 minutes).
- Cancellation Rate.
- Diversion Rate.
- Average Arrival Delay Minutes.
- Delayed Flights by Airline.
- Delayed Flights by Airport / Route.
- Delay Rate by Month / Day of Week / Scheduled Hour.

Phân tích sâu hơn:

- Airline Performance Comparison.
- Airport and Route Performance.
- Monthly / Weekly Delay Trends.
- Delay Cause Analysis.
- High-Risk Time Windows.

Cần định nghĩa rõ numerator, denominator, bộ lọc và cách xử lý NULL của từng KPI.

Không dùng chung denominator cho tất cả các chỉ số một cách máy móc.

Chuyến bay bị hủy, chuyển hướng và chuyến hoàn thành phải được xử lý nhất quán với định nghĩa KPI.

Không sử dụng các trường nguyên nhân chậm chuyến để kết luận quan hệ nhân quả ngoài những gì dữ liệu hỗ trợ.

Các phép tính tổng hợp quan trọng nên thực hiện bằng Spark SQL để chứng minh SQL skills.

Không thu thập toàn bộ dữ liệu lớn về Pandas bằng `toPandas()`.

Chỉ xuất aggregated datasets có kích thước phù hợp sang Power BI.

---

## 7. MACHINE LEARNING REQUIREMENTS

### Prediction Task

Binary Classification:

Dự đoán một chuyến bay có đến trễ từ 15 phút trở lên hay không, dựa trên các thông tin có sẵn trước thời điểm khởi hành theo lịch.

Target:

`Arrival Delay >= 15 minutes`

Ưu tiên sử dụng trường nhãn chính thức trong dataset sau khi kiểm tra schema và định nghĩa.

Chỉ dùng những chuyến bay có nhãn arrival delay hợp lệ để train và evaluate. Đối với MVP, loại các chuyến bị hủy và chuyển hướng khỏi bài toán dự đoán arrival delay, đồng thời ghi rõ phạm vi áp dụng.

### Feature Engineering

Chỉ cân nhắc những đặc trưng có sẵn tại thời điểm dự đoán, ví dụ:

- Reporting Airline.
- Origin Airport.
- Destination Airport.
- Day of Week.
- Month.
- Scheduled Departure Hour.
- Flight Distance.

Không sử dụng:

- Actual Arrival Delay.
- Actual Departure Delay.
- Actual Arrival Time.
- Delay Cause Minutes.
- Những trường phát sinh sau thời điểm dự đoán.

Phải giải thích và kiểm tra Data Leakage.

### Models

Bắt đầu bằng:

1. Dummy Baseline hoặc Majority Class Baseline.
2. Logistic Regression với Spark MLlib.

Nếu mô hình ban đầu chạy ổn, có thể thử Random Forest để so sánh.

Không tự ý thêm XGBoost, Deep Learning hoặc nhiều mô hình không cần thiết.

### Evaluation

Sử dụng temporal split, không chia dữ liệu ngẫu nhiên toàn bộ.

Với dữ liệu 3 tháng, có thể thử:

- January: Train.
- February: Validation.
- March: Test.

Giải thích hạn chế của khoảng train ngắn này. Khi mở rộng đủ 12 tháng, đề xuất lại các khoảng train/validation/test hợp lý.

Chỉ fit preprocessing transformers trên training data.

Đánh giá:

- Precision.
- Recall.
- F1-score.
- Confusion Matrix.
- ROC-AUC.
- PR-AUC nếu cần.
- Comparison against Baseline.

Không chỉ trình bày Accuracy, đặc biệt nếu các lớp bị mất cân bằng.

Đánh giá threshold khi phù hợp và không sử dụng test set để tuning.

Không tuyên bố mô hình có hiệu quả thực tế nếu kết quả chưa được kiểm chứng.

---

## 8. POWER BI DASHBOARD

Dashboard cần có bố cục rõ ràng, chuyên nghiệp, phù hợp để đưa ảnh vào README và CV.

Dự kiến:

### Page 1 — Executive Overview

- Total Flights.
- On-Time Arrival Rate.
- Delay Rate.
- Cancellation Rate.
- Monthly Performance Trends.
- Airline Performance Comparison.

### Page 2 — Delay Analysis

- Delay by Airport.
- Delay by Route.
- Delay by Day of Week.
- Delay by Scheduled Departure Hour.
- Reported Delay Causes.
- Drill-down hoặc filter phù hợp.

### Page 3 — Prediction Insights

Chỉ triển khai khi ML đã hoạt động:

- Model Performance.
- Precision / Recall / F1.
- Confusion Matrix.
- Prediction Probability Distribution.
- Baseline Comparison.

Trước khi làm dashboard:

1. Xác định các KPI cần hiển thị.
2. Xác định grain của các bảng xuất ra.
3. Tránh double counting khi kết hợp nhiều bảng aggregate.
4. Chuẩn bị analytical tables phù hợp.
5. Kiểm tra số liệu giữa Spark SQL và Power BI.

Không ưu tiên tạo dashboard đẹp trước khi kiểm tra tính đúng đắn của KPI.

---

## 9. REAL METRICS & CV REQUIREMENTS

Đây là yêu cầu rất quan trọng.

Tôi muốn project có những con số thực tế, trực quan để đưa vào CV.

Ngay từ đầu, hãy thiết kế phương án lưu lại:

### Data Metrics

- Number of raw records.
- Number of processed records.
- Invalid / missing records.
- Data quality results.
- Dataset size.
- Processing time.

### Business Metrics

- Overall delay rate.
- Cancellation rate.
- Differences between airlines.
- Routes or time windows with higher delay rates.
- Quantified business findings.

### Machine Learning Metrics

- Precision.
- Recall.
- F1-score.
- ROC-AUC.
- Baseline performance.
- Model comparison results.

### Performance Metrics

Nếu thực hiện benchmark, ghi lại:

- Dataset size.
- Hardware and Spark configuration.
- Processing time.
- Comparison setup.
- Number of runs.

Chỉ so sánh tốc độ khi điều kiện benchmark hợp lý.

**TUYỆT ĐỐI KHÔNG tự tạo các con số như "improved performance by 80%", "achieved 95% accuracy" hoặc "processed 10M records" nếu chưa thực sự đo được.**

Tạo tài liệu ghi nhận kết quả thực nghiệm để cuối project có thể chuyển thành 3–5 bullet points tiếng Anh cho CV.

Mỗi claim phải truy vết được về query, output, bảng kết quả hoặc báo cáo đánh giá.

---

## 10. DEVELOPMENT ROADMAP

Triển khai theo các Phase:

**Phase 0 — Project Planning & Environment**

- Xác nhận scope MVP.
- Đề xuất lệnh để tôi tự kiểm tra môi trường Python / Java / Spark và phân tích output do tôi cung cấp.
- Tạo repository structure tối thiểu.
- Viết README ban đầu.
- Viết business questions và roadmap.
- Chuẩn bị requirements.
- Xác minh đường dẫn nguồn dữ liệu và cách tải.

**Phase 1 — Data Acquisition & Profiling**

- Hướng dẫn tôi tự tải dataset mẫu.
- Kiểm tra schema.
- Exploratory Data Profiling.
- Data dictionary.
- Data quality findings.
- Cập nhật README theo kết quả thực tế của Phase 1.

**Phase 2 — Cleaning & Spark Processing**

- Spark DataFrames.
- Cleaning and validation.
- Business rules.
- Export cleaned Parquet.
- Validation tests.
- Cập nhật README theo kết quả thực tế của Phase 2.

**Phase 3 — SQL Analytics**

- Xây dựng KPI bằng Spark SQL.
- Airline and airport analysis.
- Delay trends and route analysis.
- Export analytical datasets.
- Ghi nhận business findings ban đầu.
- Cập nhật README theo kết quả thực tế của Phase 3.

**Phase 4 — Power BI Dashboard**

- Build analytical model.
- Create DAX measures nếu cần.
- Build 2 trang dashboard.
- Verify metrics.
- Export dashboard screenshots.
- Cập nhật README theo kết quả thực tế của Phase 4.

**Phase 5 — Machine Learning**

- Feature engineering.
- Train/validation/test split.
- Baseline.
- Logistic Regression.
- Optional Random Forest.
- Evaluation and model comparison.
- Cập nhật README theo kết quả thực tế của Phase 5.

**Phase 6 — Finalization**

- Prediction dashboard.
- Final business insights.
- Final data and ML metrics.
- README hoàn chỉnh.
- Architecture diagram.
- CV bullet points dựa trên số liệu thực nghiệm.
- Interview questions and answers.

Mỗi Phase phải có acceptance criteria cụ thể và được kiểm thử trước khi sang Phase tiếp theo.

Việc chạy acceptance checks và tests do tôi thực hiện. Codex phải cung cấp chính xác các lệnh cần chạy, chờ tôi gửi output và chỉ kết luận dựa trên bằng chứng đó.

README là deliverable bắt buộc của từng Phase. Cuối mỗi Phase, phải cập nhật tối thiểu: trạng thái Phase, file/chức năng mới, hướng dẫn chạy hiện tại, validation đã được tôi thực hiện, kết quả thực tế và các hạn chế hoặc việc còn lại.

Không tự ý triển khai Phase tiếp theo nếu chưa được tôi xác nhận.

---

## 11. WORKFLOW RULES FOR CODEX

Các yêu cầu bắt buộc trong quá trình làm việc:

1. Trước mỗi Phase, trình bày mục tiêu và các file dự kiến thay đổi.
2. Chỉ thực hiện các thay đổi thuộc Phase hiện tại.
3. Không sửa những module đã chạy ổn nếu không cần thiết.
4. Nếu phải refactor, giải thích lý do và tác động.
5. Không tự ý thay đổi Tech Stack.
6. Không tạo code quá trừu tượng hoặc khó giải thích.
7. Không copy-paste logic lặp lại giữa nhiều file.
8. Không thêm complexity chỉ để project trông chuyên nghiệp.
9. Không tự động chạy lệnh Git commit hoặc Git push.
10. Không đẩy dataset lớn, môi trường ảo, model artifacts hoặc cache lên GitHub.
11. Không xóa hoặc làm thay đổi Project 1.
12. Không giả định một bước đã chạy thành công khi chưa có output thực tế.
13. Nếu gặp lỗi, ưu tiên tìm và sửa root cause thay vì viết workaround phức tạp.
14. Không tự thay đổi scope khi gặp khó khăn; báo cáo vấn đề và đề xuất cách xử lý đơn giản nhất.
15. Không tự ý chạy bất kỳ lệnh terminal, PowerShell, Command Prompt, Python, Java, Spark, pytest, Git hoặc lệnh cài đặt dependency nào.
16. Không tự chạy script, pipeline, notebook, query, benchmark, data download, model training hoặc test dưới bất kỳ hình thức nào.
17. Mọi lệnh phải được đưa cho tôi dưới dạng có thể copy để tôi tự chạy. Sau đó Codex chờ output do tôi cung cấp, phân tích kết quả và đề xuất bước tiếp theo.
18. Không được ghi test pass, validation thành công, pipeline chạy được hoặc metric đã được xác nhận nếu chưa có output thực tế do tôi cung cấp.
19. Sau mỗi Phase, phải chỉnh sửa `README.md` trong cùng Phase trước khi báo cáo hoàn thành. README chỉ được ghi các kết quả đã có bằng chứng; nội dung chưa kiểm chứng phải đánh dấu rõ là pending hoặc chưa chạy.
20. Việc được phép tạo hoặc chỉnh sửa file không đồng nghĩa với việc được phép thực thi file đó.

### Required Response Format After Each Phase

Sau mỗi Phase, báo cáo:

**A. Phase Summary**

- Những gì đã hoàn thành.
- Những gì chưa hoàn thành.

**B. Files Created / Modified**

- Danh sách file.
- Chức năng từng file.

**C. Technical Explanation**

- Giải thích các bước xử lý chính.
- Vì sao chọn cách triển khai.
- Input / output của pipeline.

**D. Validation Results**

- Commands Codex đề xuất để tôi tự chạy.
- Commands tôi xác nhận đã chạy và output tương ứng.
- Tests pass/fail chỉ dựa trên output thực tế do tôi cung cấp.
- Row counts và metrics thực tế nếu có bằng chứng.
- Những gì chưa chạy hoặc chưa thể kiểm thử phải được đánh dấu rõ.

**E. Interview Preparation**

- 3–5 câu hỏi phỏng vấn có thể gặp.
- Câu trả lời ngắn, rõ ràng và gắn với code đã viết.

**F. Next Phase**

- Mục tiêu Phase tiếp theo.
- Những điều kiện cần hoàn thành trước khi tiếp tục.

Nếu chưa có dữ liệu hoặc môi trường chưa chạy được, phải nói rõ. Không được giả lập kết quả.

---

## 12. STARTING INSTRUCTION

Bây giờ hãy bắt đầu với **PHASE 0 ONLY**.

Trước tiên:

1. Cung cấp lệnh để tôi tự kiểm tra thư mục làm việc hiện tại, sau đó chờ tôi gửi output.
2. Dựa trên output tôi cung cấp, nếu thư mục chưa trống thì báo những file hiện có và tránh ghi đè ngoài phạm vi project.
3. Đề xuất cấu trúc thư mục tối thiểu, không tạo trước toàn bộ module của các Phase sau.
4. Cung cấp lệnh để tôi tự kiểm tra môi trường Python, Java và khả năng cài PySpark; không tự chạy các lệnh này.
5. Tạo những file cần thiết cho Phase 0.
6. Viết README mô tả mục tiêu, business questions, Tech Stack, dataset và roadmap.
7. Hướng dẫn cách chạy hoặc kiểm tra môi trường để tôi tự thực hiện.
8. Cập nhật README cho Phase 0 trước khi báo cáo hoàn thành.
9. Báo cáo rõ những file đã tạo/chỉnh sửa, những lệnh đề xuất và các kết quả thực tế dựa trên output tôi cung cấp. Không tự chạy hoặc tự test.

Sau khi xong Phase 0:

**DỪNG LẠI. Không triển khai Phase 1 cho đến khi tôi kiểm tra và yêu cầu tiếp tục.**

Mục tiêu xuyên suốt: Xây dựng một project có giá trị cho CV DA Intern, code gọn, dễ đọc, dễ bảo trì, dễ giải thích và có các business insights cùng metrics thực nghiệm có thể chứng minh.
