# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Van An |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602810 |
| GitHub | vanan05092004 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-07774 |

## Kết quả cuối

- `dataset_version` của bạn: `e51654bfeb45`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: train=660, val=192, test=167
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

**4 khác biệt giữa A và B:**
1. **Tên cột khác nhau**: A dùng `speed_mps`, `heading_deg`, `lat`, `lon`; B dùng `spd_kmh`, `hdg`, `latitude`, `longitude`
2. **Đơn vị tốc độ khác**: A dùng m/s, B dùng km/h
3. **Định dạng timestamp khác**: A dùng ISO UTC (`2025-10-07T08:00:00.014Z`), B dùng epoch milliseconds (`1759933801509`)
4. **Tên cột gia tốc khác**: A dùng `acc_x`, `acc_y`; B dùng `ax`, `ay`
5. **Bàn đạp khác**: A dùng `gas`, `brake`; B dùng `throttle_pct`, `brake_pct`
6. **Đồng hồ lệch**: B lệch +1500ms so với video (theo `sources.json`)

**Vì sao giữ riêng và không ép kiểu ngay:**
- Bronze giữ nguyên bản để có nguồn tin đáng tin cậy (single source of truth)
- Để mọi cột ở dạng chữ vì dữ liệu thô có thể có null, outlier, hoặc format không đúng; ép kiểu sớm có thể mất thông tin hoặc lỗi
- Khi có sự cố, có thể backfill lại từ bronze mà không cần nguồn gốc

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo t�ng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**Cách đưa về một schema:**
- Đơn vị: km/h → m/s (chia 3.6)
- Timestamp: epoch ms → UTC, trừ độ lệch 1500ms của drive B
- Cột: đổi tên, thống nhất

**Bằng chứng đúng:**
- Drive B bắt đầu `2025-10-08 14:30:00.009000+00:00` = đúng thời điểm video bắt đầu (đã trừ 1.5 giây lệch)
- Tốc độ tối đa drive B: 100.0 km/h = 27.78 m/s, drive A: 71.0 km/h = 19.7 m/s (hợp lý)

**Bảng quarantine:**
| drive_id | reject_reason | so_dong |
|----------|--------------|---------|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

**Vì sao vào rejects thay vì xoá:**
- Giữ lại để audit và debug: biết tại sao dữ liệu bị loại
- 106 dòng lỗi / 6255 tổng = 1.7%, không ảnh hưởng phân tích
- Phục vụ kiểm tra chất lượng: xem pattern lỗi theo thời gian, drive

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

**Vì sao max_gap_ms là WARN:**
- `max_gap_ms` phát hiện cảm biến mất tín hiệu tạm thời (ví dụ đi qua đường hầm)
- Đây là vấn đề có thể chấp nhận được, không nghiêm trọng → chỉ cảnh báo, không dừng pipeline
- Cảm biến tạm mất = dữ liệu vẫn usable

**Vì sao in_range là ERROR:**
- `in_range` kiểm tra dữ liệu nằm trong ngưỡng hợp lý (tốc độ 0-70 m/s)
- Dữ liệu vô lý = lỗi nghiêm trọng, không nên đi tiếp vào dataset huấn luyện
- Nếu cho qua → model học từ dữ liệu sai → ảnh hưởng chất lượng model

**Kết quả break-gate:**
- Khi nới luật lên 1000 m/s: `ERROR=1`, test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**
- Khôi phục luật gốc: `ERROR=0`

**Ý nghĩa cổng chất lượng:**
- Cổng chất lượng là "gatekeeper" bảo vệ dataset huấn luyện
- ERROR = dừng pipeline, không cho dữ liệu xấu đi tiếp
- WARN = thông báo cho data engineer, pipeline vẫn chạy
- Break-gate chứng minh: nới luật → dữ liệu vô lý lọt qua → test bắt được

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

**Kết quả idempotency:**
- Trước: 1246 dòng, fingerprint `6c7480dfde7e`
- Sau 2 lần chạy lại: 1246 dòng, fingerprint `6c7480dfde7e` → **KHÔNG ĐỔI**
- Ghi append 2 lần (sai): 120 dòng → **NHÂN ĐÔI**

**Delta overwrite theo partition an toàn hơn:**
- Overwrite chỉ ghi đè partition bị ảnh hưởng (một trip), các partition khác không bị chạm
- Nếu giữa chừng lỗi: partition đang ghi không hoàn tất, nhưng dữ liệu cũ vẫn tồn tại (Delta Lake transaction)
- Xoá rồi ghi lại toàn bộ: nếu lỗi giữa chừng → mất toàn bộ dữ liệu
- Delta ghi đè atomically: hoặc tất cả thành công, hoặc không gì thay đổi

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Khung đen vs khung tối:**
- **Khung đen**: pixel = 0 hoặc gần 0, camera bị che hoàn toàn hoặc lỗi → **xoá** (không có thông tin)
- **Khung tối**: pixel thấp nhưng > 0, xe đi qua đường hầm, bóng râm → **gắn cờ nhưng giữ lại** (vẫn có thông tin)

**Số khung trùng theo ngưỡng dHash:**
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**Tôi đồng ý** với cách xử lý của pipeline: khung tối vẫn có texture và object, chỉ khó nhận diện hơn; khung đen mất hoàn toàn thông tin.

**Chọn ngưỡng:**
- Pipeline dùng ngưỡng **2 bit** (loại 149 khung A, 53 khung B)
- Tôi đồng ý với ngưỡng 2: cho phép khung hơi khác một chút (camera shake, ánh sáng thay đổi nhẹ) nhưng vẫn loại bỏ khung trùng thật sự

**Ngưỡng phụ thuộc tốc độ xe:**
- Xe chạy nhanh: khung liên tiếp khác nhau nhiều → ngưỡng thấp để tránh loại nhầm
- Xe chạy chậm/đứng: khung liên tiếp gần như giống nhau → ngưỡng cao hơn để loại bớt
- Drive A có nhiều khung trùng hơn (camera đứng hình 6 giây) → phụ thuộc vào điều kiện lái

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

**Kết quả leakage:**
- Chia theo **khung hình**: **26/26 trip** bị chia cắt (cùng nằm trong train và test)
- Chia theo **trip**: **0 trip** bị chia cắt

**Vì sao điểm kiểm thử đẹp giả tạo:**
- Các khung hình liên tiếp trong cùng trip gần như giống hệt nhau (cùng cảnh, cùng điều kiện)
- Nếu một nửa trip ở train, nửa kia ở test → model "đã thấy" đề thi rồi
- Model đạt accuracy cao trên test set nhưng thực tế không generalizes được
- Đây là **data leakage**: thông tin từ test set rò rỉ vào training

**Minh hoạ:**
- Trip đi qua ngã tư: frame 100 ở train, frame 101 ở test
- Frame 100 và 101 gần như giống nhau
- Model nhận diện ngã tư từ frame 100 → áp dụng cho frame 101 → điểm cao giả tạo

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

**Vì sao là băm nội dung:**
- Băm (hash) của nội dung → nếu dữ liệu đầu vào thay đổi → hash khác → phát hiện thay đổi ngay
- Không phụ thuộc thời gian: cùng dữ liệu, cùng hash dù chạy lúc nào
- Rõ ràng, verifiable: ai cũng tính được hash từ dữ liệu

**Hai lần chạy liên tiếp:**
- Cùng dataset_version (`e51654bfeb45`)
- Vì dữ liệu đầu vào không đổi → hash không đổi

**Tại sao khác bạn bè:**
- `LAB_STUDENT_ID` (MSSV) được dùng làm salt khi chia train/val/test
- MSSV khác → thứ tự trip trong mỗi split khác
- Dataset có cùng frames nhưng phân bố train/val/test khác → nội dung khác → hash khác
- Đảm bảo mỗi sinh viên có dataset riêng, không thể chép bài

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

**Lineage từ manifest.json:**
```json
"lineage": {
  "bronze.raw_telemetry_a": 0,
  "bronze.raw_telemetry_b": 0,
  "bronze.trips_raw": 0,
  "bronze.video_files": 1,
  "silver.frames": 25,
  "silver.telemetry": 0,
  "silver.frames_flagged": 0,
  "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc"
  }
}
```

**Dataset được tạo từ:**
- Video gốc drive_a (sha256: `636ba1a...`) và drive_b (sha256: `bfa6270...`)
- Telemetry từ hai nguồn: A và B
- Trips metadata (26 trips)
- Tất cả đi qua silver để clean và flag

**Để tái tạo lại sau 3 tháng:**
- `dataset_version`: `e51654bfeb45`
- `manifest_sha256`: `711dfd17de072bb7cd0dc58ef111200c1421e20af17027293dca5f2c2b8a923d`
- Input video sha256: dùng để xác minh video gốc chưa thay đổi
- Pipeline code version: dbt models, scripts
- MSSV: `2A202602810` (dùng làm salt)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

**Trip được gán: `trip-07774`**

**Điều xảy ra khi sập:**
- `LAB_FAIL_ONCE=trip-07774` được đặt trong `.env`
- Task `silver_frames__trip-07774` chạy → crash với log `simulated crash while extracting trip-07774`
- Task này chuyển **đỏ** (failed)
- Các task phụ thuộc vào nó (dbt_run_silver, dbt_test_silver, publish_lake, export_dataset, publish_warehouse) chuyển **upstream_failed** (cam nâu)

**Sau khi clear:**
- Clear task đỏ `trip-07774`
- Task chạy lại → **xanh** (success)
- Các task phụ thuộc tự động chạy lại → toàn DAG **xanh**

**Dữ liệu có nhân đôi không:**
- **Không** nhân đôi
- Delta Lake overwrite theo partition đảm bảo: dữ liệu trip-07774 được ghi đè, không nhân đôi
- Idempotent: chạy lại nhiều lần cho cùng kết quả

**dataset_version có đổi không:**
- **Không đổi**: `e51654bfeb45`
- Vì dữ liệu đầu vào không thay đổi, nội dung gold không thay đổi

**Bằng chứng:**
- Verify 27/27 vẫn pass
- DAG graph xanh (ảnh `airflow_graph.png`)
- dataset_version không đổi

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

**Ba thành phần sẽ vỡ đầu tiên:**

1. **Xử lý video (FFmpeg)** - Nút cổ chai lớn nhất
   - 500 xe × 5 phút video × resource CPU/GPU = quá tải
   - Thay bằng: **Spark/Dask distributed** hoặc **GPU cluster** (như Ray)
   - Hoặc dùng **serverless functions** (AWS Lambda, Cloud Run) để scale tự động

2. **DuckDB (local)** - Không đủ cho scale
   - DuckDB tốt cho single-node, không scale được ngang
   - Thay bằng: **Apache Spark + Delta Lake** hoặc **Databricks**
   - Hoặc **Snowflake/BigQuery** cho cloud warehouse

3. **Airflow (LocalExecutor)** - parallelism=2 quá nhỏ
   - 500 xe = 500 partitions = 500 tasks × thời gian xử lý
   - Thay bằng: **CeleryExecutor** hoặc **Kubernetes executor** để scale ngang
   - Hoặc **dbt Cloud + Orchestration** chuyên dụng

**Giải pháp tổng thể (theo slide):**
- Slide 12: Data pipeline cần thiết kế horizontal scaling
- Slide 22: ELT với Spark, dbt cho transformation
- Slide 23: Data quality framework (Great Expectations, dbt tests) cần mở rộng

## Câu hỏi riêng của bạn trên Metabase

**Câu SQL đã thêm:**
```sql
SELECT 
  drive_id,
  split,
  COUNT(*) as so_anh,
  AVG(speed_mps) as toc_do_tb,
  MAX(speed_mps) as toc_do_max
FROM marts.mart_frame_gallery
GROUP BY drive_id, split
ORDER BY split, drive_id
```

**Điều câu này cho biết:**
- Phân bố khung hình theo drive và split
- Tốc độ trung bình và tối đa theo từng nhóm
- Drive B có tốc độ cao hơn drive A (thể hiện điều kiện lái khác nhau)

## Nhìn lại

- **Bất ngờ nhất**: Delta Lake với overwrite theo partition thực sự idempotent - chạy lại nhiều lần không ảnh hưởng kết quả, rất an toàn
- **Chưa rõ**: Tại sao drive B có nhiều khung tối hơn drive A (61 vs 4)? Có phải do route khác hay cảm biến khác?
- **Lỗi chưa bắt được**: Pipeline không kiểm tra GPS jump (xe nhảy vị trí đột ngột từ Hà Nội sang TP.HCM trong 1 giây) - có thể mask bằng outlier detection trên velocity/acceleration
