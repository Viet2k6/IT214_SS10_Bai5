# BÀI TẬP 5: ĐẢM BẢO THỨ TỰ SỰ KIỆN VỚI KAFKA PARTITION KEY

## Phần 1 - Phân tích: Vì sao việc chia Partition làm mất thứ tự tổng thể

Kafka chỉ đảm bảo thứ tự (ordering) của các message **trong cùng một Partition**. 
Nếu một topic có nhiều partition (ví dụ: 5 partitions) nhằm tăng thông lượng (throughput), và Producer gửi các sự kiện của cùng một đơn hàng vào topic mà không chỉ định `Partition Key`, Kafka mặc định sẽ sử dụng thuật toán Round-robin để phân phối các message này đều đặn sang các partition khác nhau.

**Minh họa cụ thể:**
Đơn hàng `ORDER_001` có 3 sự kiện phát ra theo thứ tự thời gian: `CREATED` (t=1), `PAID` (t=2), `SHIPPED` (t=3).
- `CREATED` được gửi vào Partition 0.
- `PAID` được gửi vào Partition 1.
- `SHIPPED` được gửi vào Partition 2.

Vì Consumer A đọc từ Partition 2 có thể xử lý nhanh hơn Consumer B đang đọc từ Partition 1 (do khác biệt về network, tải CPU...), nên sự kiện `SHIPPED` có thể bị xử lý xong trước `PAID`. Điều này vi phạm nghiêm trọng tính logic nghiệp vụ của quy trình xử lý đơn hàng (hàng chưa thanh toán đã được giao).

## Phần 2 - Thiết kế giải pháp

**Cách cấu hình Producer:**
Để giải quyết bài toán trên, chúng ta cần ép tất cả các sự kiện của cùng một `orderId` luôn đi vào cùng 1 Partition cố định. Khi đó, do Kafka bảo đảm thứ tự trong một partition, các sự kiện của đơn hàng đó sẽ được Consumer đọc ra tuần tự đúng như lúc gửi.
Giải pháp là sử dụng `orderId` làm **Partition Key** khi gửi thông điệp bằng `KafkaTemplate`.

**Cơ chế Partition Key của Kafka:**
Khi Producer gửi một message kèm theo một Key, Kafka sẽ tính toán mã băm (Hash) của Key đó, rồi lấy phần dư khi chia cho tổng số partition của topic:
`Partition = hash(Key) % Số lượng Partitions`

Với cơ chế này, cùng một `orderId` sẽ luôn tạo ra cùng một giá trị hash, dẫn đến luôn được điều hướng vào một Partition duy nhất. Do đó, `CREATED`, `PAID`, `SHIPPED` của cùng một `orderId` sẽ nằm chung partition và được đảm bảo thứ tự xử lý tuyệt đối.

## Phần 3 - Giải thích số lượng Consumer tối đa

Số lượng Consumer tối đa trong cùng một Consumer Group **không nên vượt quá số lượng Partition** của Topic.
Trong bài toán này, Topic "order-tracking" có **5 partitions**. Do đó, số lượng Consumer tối đa nên bật trong cùng một group là **5**.
- Nếu cấu hình đúng 5 consumers (concurrency = 5), mỗi consumer sẽ phụ trách chính xác 1 partition. Đây là trạng thái tối ưu nhất, đạt được sự song song (parallelism) tối đa.
- Nếu cấu hình lớn hơn 5 (ví dụ 6 consumers), thì sẽ có 1 consumer dư thừa, bị rảnh rỗi (idle) và không nhận được bất kỳ message nào. Điều này gây lãng phí tài nguyên hệ thống, bởi vì Kafka quy định: **một partition chỉ được gán cho tối đa 1 consumer trong cùng 1 group**.
