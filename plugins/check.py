from dotenv import load_dotenv
import os

load_dotenv()  # Tải các biến môi trường

# Kiểm tra: Kiểm tra xem biến có được tải đúng không
if "DATABASE_URL" in os.environ:
    print("DATABASE_URL đã được tải thành công.")
    print("=====================================")
    # try:
    #     conn = psycopg2.connect(
    #         host="postgres",  # Hoặc tên container nếu trong Docker
    #         port=5432,
    #         database="dw_amazon_sales",
    #         user="airflow",
    #         password="airflow",
    #     )
    #     print("Connection successful!")
    #     conn.close()
    # except Exception as e:
    #     print("Failed to connect:", e)
    print("Database URL:", os.getenv("DATABASE_URL"))
    print("=====================================")
else:
    print("DATABASE_URL bị thiếu.")
