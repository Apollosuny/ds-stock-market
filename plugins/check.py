from dotenv import load_dotenv
import os

load_dotenv()  # Tải các biến môi trường

# Kiểm tra: Kiểm tra xem biến có được tải đúng không
if "DATABASE_URL" in os.environ:
    print("DATABASE_URL đã được tải thành công.")
else:
    print("DATABASE_URL bị thiếu.")
