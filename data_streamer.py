import socket
import time
import random

def send_data():
    # Tạo socket server
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("0.0.0.0", 9999))
    server_socket.listen(1)
    print("Socket server started on port 9999. Waiting for connection...")

    conn, addr = server_socket.accept()
    print(f"Connected to {addr}")

    try:
        while True:
            # Giả lập dữ liệu giao dịch
            transaction_id = f"txn_{random.randint(1000, 9999)}"
            user_id = random.randint(1, 3)
            amount = round(random.uniform(10.0, 500.0), 2)
            data = f"{transaction_id},{user_id},{amount}\n"
            conn.send(data.encode())
            print(f"Sent: {data.strip()}")
            time.sleep(1)  # Gửi mỗi giây
    except KeyboardInterrupt:
        print("Stopping data streamer...")
        conn.close()
        server_socket.close()

if __name__ == "__main__":
    send_data()