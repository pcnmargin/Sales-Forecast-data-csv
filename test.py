import streamlit as st
import PY0019_REVENUE_LOCAL as rl
import mysql.connector
import PY0002_ACCOUNT as ac

import streamlit as st
import mysql.connector
from mysql.connector import Error

# Kết nối đến MySQL database
def create_connection():
    try:
        connection = mysql.connector.connect(
            host=ac.host_da,
            user=ac.user_da,
            password=ac.pass_da,
            database=ac.database_da,
            connect_timeout=60
        )
        if connection.is_connected():
            st.success("Kết nối thành công đến cơ sở dữ liệu!")
            return connection
    except Error as e:
        st.error(f"Không thể kết nối đến cơ sở dữ liệu: {e}")
        return None

# Tạo bảng trong cơ sở dữ liệu nếu chưa tồn tại
def create_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS feedback (
            id INT AUTO_INCREMENT PRIMARY KEY,
            phone VARCHAR(15),
            customer_name VARCHAR(255),
            order_code VARCHAR(50),
            service_type VARCHAR(50),
            service_rating VARCHAR(10),
            note TEXT
        )
        """)
        connection.commit()
        st.success("Đã kiểm tra và tạo bảng nếu chưa tồn tại!")
    except Error as e:
        st.error(f"Không thể tạo bảng: {e}")

# Hàm để chèn dữ liệu vào bảng
def insert_feedback(connection, feedback):
    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO feedback (phone, customer_name, order_code, service_type, service_rating, note)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, feedback)
        connection.commit()
        st.success("Đã lưu ý kiến thành công!")
    except Error as e:
        st.error(f"Không thể lưu ý kiến: {e}")

# Tạo giao diện người dùng với Streamlit
def main():
    st.title("Thu thập ý kiến đánh giá của khách hàng")
    
    phone = st.text_input("Số điện thoại")
    customer_name = st.text_input("Tên khách hàng")
    order_code = st.text_input("Mã đơn")
    
    service_type = st.selectbox(
        "Dịch vụ",
        ("Tư vấn sản phẩm", "Chương trình", "Không gian mua sắm")
    )
    
    service_rating = st.selectbox(
        "Đánh giá dịch vụ",
        ("Tốt", "Không tốt")
    )
    
    note = ""
    if service_rating == "Không tốt":
        note = st.text_area("Ghi chú chi tiết")

    if st.button("Gửi đánh giá"):
        connection = create_connection()
        if connection:
            create_table(connection)
            feedback = (phone, customer_name, order_code, service_type, service_rating, note)
            insert_feedback(connection, feedback)
            connection.close()

if __name__ == "__main__":
    main()