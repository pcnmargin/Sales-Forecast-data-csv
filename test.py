import pandas as pd
import numpy as np
import mysql.connector
import psycopg2
from datetime import datetime, timedelta
import PY0012_CODE as cc
import PY0002_ACCOUNT as ac
import streamlit as st
import matplotlib.pyplot as plt


@st.cache_data(ttl=3600)
def fetch_and_prepare_data():
    sht = ac.gs.open_by_key("1gGpQWYdhV_dxrZjv_Fxkytm7vx3IwgYqyIYYBWdCNPE")
    SHEET2 = 'REF CATEGORY FIX'
    SHEET5 = 'REF KPI STORE'
    worksheet_ref_cat = sht.worksheet(SHEET2)
    data_cat = worksheet_ref_cat.get_all_values()
    df_ref_cat = pd.DataFrame(data_cat[1:], columns=data_cat[0])

    # Connect database - Xboss
    conn = psycopg2.connect(
        host=ac.host_xboss,
        database=ac.database_xboss,
        user=ac.user_xboss,
        password=ac.pass_xboss,
        port = ac.port_xboss
    )

    # REF GOODLIST
    # tạo một đối tượng cursor
    cur = conn.cursor()

    # thực thi một truy vấn SQL

    query_cate = f'''SELECT 
        DISTINCT 
        pt.default_code,
        TRIM(SPLIT_PART(pcat.complete_name,'/',1)) AS cat1,
        TRIM(SPLIT_PART(pcat.complete_name,'/',2)) AS cat2,
        TRIM(SPLIT_PART(pcat.complete_name,'/',3)) AS cat3,
        pc.name AS collection,
        pt.list_price AS tag_price
        FROM product_product pp
        LEFT JOIN product_template pt ON pp.product_tmpl_id = pt.id
        LEFT JOIN product_category pcat ON pt.categ_id = pcat.id
        LEFT JOIN product_collection pc ON pt.collection_id = pc.id
        WHERE pp.active = TRUE'''
    cur.execute(query_cate)

    # lấy kết quả
    result_gl = cur.fetchall()

    # in kết quả
    df_goodlist = pd.DataFrame(result_gl, columns=['DEFAULT_CODE', 'REF_CAT1', 'REF_CAT2', 'REF_CAT3','BST', 'TAG_PRICE'])

    current_date = datetime.now()
    current_date_str = current_date.strftime('%Y-%m-%d %H:%M')
    Year = current_date.year
    Month = current_date.month

    # TIÊU ĐỀ
    #//////////////////////////////
    st.image('LOGO.png', width=150)
    st.title(f"""THEO DÕI DOANH THU""")
    st.sidebar.header(f"""Update: {current_date_str}""")
    st.sidebar.header(f"""TUỲ CHỈNH BỘ LỌC""")
    #//////////////////////////////
    cur = conn.cursor()

    query_sale_off = f"""SELECT
            distinct(po.id) AS order_id,
            DATE(po.date_order + '7 hour') AS date_order,
            TRIM(SPLIT_PART(sw.name, '-', 2)) AS store,
            SPLIT_PART(pp.default_code,'-',1) AS default_code,
            SPLIT_PART(pp.default_code,'-',2) AS color,
            SPLIT_PART(pp.default_code,'-',3) AS size,
            CASE WHEN pp.default_code = 'Chiết khấu' THEN 0 ELSE pol.qty END AS qty,
            pol.price_subtotal_incl AS total_amount,
            pc1.name AS collection,
            TRIM(SPLIT_PART(pc.complete_name,'/',1)) AS cat1,
            TRIM(SPLIT_PART(pc.complete_name,'/',2)) AS cat2,
            rp.id AS idkh,
            (CURRENT_DATE - rp.birth_date)/365 AS "age",
            CASE 
                WHEN rp.birth_date IS NULL THEN '7.Unknown'
                WHEN (CURRENT_DATE - rp.birth_date)/365 BETWEEN 18 AND 24 THEN '1.18-24' 
            WHEN (CURRENT_DATE - rp.birth_date)/365 BETWEEN 25 AND 30 THEN '2.25-30' 
            WHEN (CURRENT_DATE - rp.birth_date)/365 BETWEEN 31 AND 35 THEN '3.31-35' 
            WHEN (CURRENT_DATE - rp.birth_date)/365 BETWEEN 36 AND 40 THEN '4.36-40'  
            WHEN (CURRENT_DATE - rp.birth_date)/365 >= 41 THEN '5.40+' 
            ELSE '0.17-' END AS group_age,
            CASE WHEN DATE(rp.create_date + '7 hour') = DATE(po.date_order + '7 hour') THEN '1.KHM' 
                WHEN DATE(rp.create_date + '7 hour') <= DATE(po.date_order + '7 hour') THEN '2.KHC' ELSE '3.KHVL' END AS type_customer
        FROM pos_order po
            LEFT JOIN pos_order_line pol ON po.id=pol.order_id
            LEFT JOIN stock_location sl ON po.location_id=sl.id
            LEFT JOIN stock_warehouse sw ON sl.warehouse_id=sw.id
            LEFT JOIN product_product pp ON pol.product_id=pp.id
            LEFT JOIN product_template pt ON pp.product_tmpl_id=pt.id
            LEFT JOIN product_category pc ON pt.categ_id=pc.id
            LEFT JOIN product_collection pc1 ON pc1.id=pt.collection_id
            LEFT JOIN res_partner rp ON rp.id = po.partner_id
        WHERE EXTRACT(YEAR FROM po.date_order + '7 hour') = {Year}
        AND sw.name != 'KHO TEST'
        ORDER BY po.id"""
    cur.execute(query_sale_off)
    result_sale_off = cur.fetchall()
    # in kết quả
    df_sale_off = pd.DataFrame(result_sale_off, columns=['ORD_ID', 'DATE', 'STORE', 'DEFAULT_CODE', 'COLOR', 'SIZE', 'QTY', 'TOTAL_AMOUNT', 'BST', 'REF_CAT1', 'REF_CAT2', 'IDKH', 'AGE', 'GROUP_AGE', 'TYPE_CUSTOMER'])
    df_sale_off['TOTAL_AMOUNT'].fillna(0, inplace=True)
    df_sale_off['TOTAL_AMOUNT'] = df_sale_off['TOTAL_AMOUNT'].astype(int)
    df_sale_off['DATE'] = pd.to_datetime(df_sale_off['DATE'])
    df_sale_off['QTY'] = pd.to_numeric(df_sale_off['QTY'])
    df_sale_off_null = df_sale_off[df_sale_off['IDKH'].isnull()]
    df_sale_off_null['IDKH'] = df_sale_off_null.groupby('ORD_ID')['ORD_ID'].transform(lambda x: np.random.randint(100000, 999999))
    df_sale_off.update(df_sale_off_null)

    # SALE ĐẠI LÍ
    # XB_PG0007_004SALEWHO
    # tạo một đối tượng cursor    
    cur2 = conn.cursor()
    query_sale_wholesale = f"""SELECT 
            so.id,
            DATE(so.date_order + '7 hour') AS date_order,
            bu.name AS store_name,
            SPLIT_PART(sol.name,'-',1) AS default_code,
            SPLIT_PART(sol.name,'-',2) AS color,
            SPLIT_PART(sol.name,'-',3) AS "size",
            sol.product_uom_qty AS qty,
            sol.price_total AS total_amount,
            pc.name AS collection,
            TRIM(SPLIT_PART(pcat.complete_name,'/',1)) AS cat1,
            TRIM(SPLIT_PART(pcat.complete_name,'/',2)) AS cat2,
            rp.parent_id AS idkh,
            CASE WHEN rp.birth_date IS NULL THEN 0 ELSE (CURRENT_DATE - rp.birth_date)/365 END AS "age",
            CASE 
                WHEN rp.birth_date IS NULL THEN '7.Unknown'
                WHEN (CURRENT_DATE - rp.birth_date)/365 BETWEEN 18 AND 24 THEN '1.18-24' 
                WHEN (CURRENT_DATE - rp.birth_date)/365 BETWEEN 25 AND 30 THEN '2.25-30' 
                WHEN (CURRENT_DATE - rp.birth_date)/365 BETWEEN 35 AND 44 THEN '3.31-35' 
                WHEN (CURRENT_DATE - rp.birth_date)/365 BETWEEN 45 AND 54 THEN '4.36-40' 
                WHEN (CURRENT_DATE - rp.birth_date)/365 >= 41 THEN '5.40+' 
            ELSE '0.17-'
        END AS group_age,
        CASE WHEN rp.register_date = DATE(so.date_order + '7 hour') THEN '1.KHM'
                WHEN rp.register_date <= DATE(so.date_order + '7 hour') THEN '2.KHC' ELSE '3.KHVL' END AS type_customer
        FROM sale_order so
            LEFT JOIN business_unit bu ON so.business_unit_id=bu.id
            LEFT JOIN sale_order_line sol ON so.id = sol.order_id
            LEFT JOIN product_product pp ON sol.product_id=pp.id
            LEFT JOIN product_template pt ON pp.product_tmpl_id=pt.id
            LEFT JOIN product_collection pc ON pc.id=pt.collection_id
            LEFT JOIN product_category pcat ON pcat.id=pt.categ_id
            LEFT JOIN res_partner rp ON so.partner_id=rp.id
            LEFT JOIN membership_level ml ON rp.membership_level_id=ml.id
        WHERE bu.name IN('B2B', 'Wholesale')
        AND so.state = 'sale'
        AND EXTRACT(YEAR FROM so.date_order + '7 hour') = {Year}"""
    # thực thi một truy vấn SQL
    cur2.execute(query_sale_wholesale)

    # lấy kết quả
    result2 = cur2.fetchall()

    # in kết quả
    df_wholesale = pd.DataFrame(result2, columns=['ORD_ID','DATE', 'STORE', 'DEFAULT_CODE', 'COLOR', 'SIZE', 'QTY', 'TOTAL_AMOUNT', 'BST', 'REF_CAT1', 'REF_CAT2', 'IDKH', 'AGE', 'GROUP_AGE', 'TYPE_CUSTOMER'])
    df_wholesale['STORE'] = df_wholesale['STORE'].replace('', 'B2B')
    df_wholesale['TOTAL_AMOUNT'].fillna(0, inplace=True)
    df_wholesale['TOTAL_AMOUNT'] = df_wholesale['TOTAL_AMOUNT'].astype(int)
    df_wholesale['DATE'] = pd.to_datetime(df_wholesale['DATE'])
    df_wholesale['QTY'] = pd.to_numeric(df_wholesale['QTY'])
    df_sale_who_null = df_wholesale[df_wholesale['IDKH'].isnull()]
    df_sale_who_null['IDKH'] = df_sale_who_null.groupby('ORD_ID')['ORD_ID'].transform(lambda x: np.random.randint(100000, 999999))
    df_wholesale.update(df_sale_who_null)


    # SALE ONLINE
    # WL0007_002SALEON
    mydb = mysql.connector.connect(
    host=ac.host_weblime,
    user=ac.user_weblime,
    password=ac.pass_weblime,
    database=ac.database_weblime
    )

    mycursor = mydb.cursor()
    # WL0007_002SALEON
    query_sale_online = f"""SELECT 
        DISTINCT(OM.ORD_CODE) ORD_ID,
        DATE(OM.C_TIME) DATE,
        PT.DEFAULT_CODE DEFAULT_CODE,
        PD.PD_COLOR COLOR,
        PD.PD_SIZE SIZE,
        OD.ORD_QTY QTY,
        ROUND((OD.ORD_QTY*OD.SALE_PRICE) - ((OD.ORD_QTY*OD.SALE_PRICE)/SALE.TOTAL_AMOUNT)*(OM.ORD_SPECIAL_DISCOUNT + OM.ORD_COUPON + OM.ORD_POINT)) AS TOTAL_AMOUNT,
        MM.MEM_CODE IDKH,
        CASE WHEN OM.mem_code IN ('41489','38958') OR MM.MEM_BIRTH IS NULL THEN NULL ELSE YEAR(CURRENT_DATE)-YEAR(MM.MEM_BIRTH) END AS age,
        CASE 
            WHEN OM.mem_code IN ('41489','38958') OR MM.MEM_BIRTH IS NULL THEN '7.Unknown'
            WHEN YEAR(CURRENT_DATE)-YEAR(MM.MEM_BIRTH) <= 18 THEN '0.17-'
            WHEN YEAR(CURRENT_DATE)-YEAR(MM.MEM_BIRTH) BETWEEN 18 AND 24 THEN '1.18-24' 
            WHEN YEAR(CURRENT_DATE)-YEAR(MM.MEM_BIRTH) BETWEEN 25 AND 30 THEN '2.25-30'  
            WHEN YEAR(CURRENT_DATE)-YEAR(MM.MEM_BIRTH) BETWEEN 31 AND 35 THEN '3.31-35' 
            WHEN YEAR(CURRENT_DATE)-YEAR(MM.MEM_BIRTH) BETWEEN 36 AND 40 THEN '4.36-40'
            WHEN YEAR(CURRENT_DATE)-YEAR(MM.MEM_BIRTH) >= 41 THEN '5.40+' 
            ELSE NULL
        END AS group_age,
        CASE 
        WHEN DATE(MM.C_TIME) = DATE(OM.C_TIME) THEN '1.KHM'
        WHEN OM.mem_code IN ('41489','38958') THEN '3.KHVL'
        ELSE '2.KHC' END AS type_customer
        FROM TBL_ORD_MAIN OM
            LEFT JOIN TBL_ORD_DETAIL OD ON OD.ORD_CODE=OM.ORD_CODE
            LEFT JOIN TBL_PRD_MAIN_TMP PMT ON PMT.SEQ = OD.PRD_MAIN_SEQ
            LEFT JOIN TBL_PRD_TMPL PT ON PT.SEQ = PMT.PRD_TMPL_SEQ
            LEFT JOIN (SELECT DISTINCT PMT.SEQ PD_ID, PMT.SKU,        C.Color PD_COLOR,        S.Size PD_SIZE
                            FROM TBL_PRD_MAIN_TMP as PMT
                            LEFT JOIN (SELECT T1.PRD_MAIN_SEQ SEQ, V1.VALUE_CODE Color
                                                FROM TBL_PRD_ATT_VAL_REF AS T1
                                                JOIN TBL_PRD_ATT_VAL V1 ON T1.PRD_ATT_VAL_SEQ = V1.SEQ AND V1.PRD_ATT_SEQ = 1) AS C ON  C.SEQ = PMT.SEQ
                            LEFT JOIN (SELECT T2.PRD_MAIN_SEQ SEQ, V2.VALUE_CODE Size
                                                FROM TBL_PRD_ATT_VAL_REF AS T2
                                                JOIN TBL_PRD_ATT_VAL V2 ON T2.PRD_ATT_VAL_SEQ = V2.SEQ AND V2.PRD_ATT_SEQ = 9) AS S ON  S.SEQ = PMT.SEQ) PD ON OD.PRD_MAIN_SEQ = PD.PD_ID
        LEFT JOIN TBL_MEM_MAIN MM ON OM.MEM_CODE=MM.MEM_CODE
        LEFT JOIN 
            (SELECT 
                OD.ORD_CODE,
                SUM(OD.SALE_PRICE*OD.ORD_QTY) TOTAL_AMOUNT
            FROM TBL_ORD_MAIN OM
                LEFT JOIN TBL_ORD_DETAIL OD ON OM.ORD_CODE=OD.ORD_CODE
            GROUP BY 
                OD.ORD_CODE) AS SALE ON OD.ORD_CODE=SALE.ORD_CODE
        WHERE 
        YEAR(OM.C_TIME) = {Year}
        AND OM.ORD_STATUS BETWEEN 60 AND 82 
        AND OM.ORD_STATUS NOT IN ('61')"""
    mycursor.execute(query_sale_online)


    myresult = mycursor.fetchall()

    df_sale_on = pd.DataFrame(myresult,columns=['ORD_ID', 'DATE','DEFAULT_CODE', 'COLOR', 'SIZE', 'QTY', 'TOTAL_AMOUNT', 'IDKH', 'AGE', 'GROUP_AGE', 'TYPE_CUSTOMER'])
    df_sale_on['STORE'] = 'WEBLIME'
    df_sale_on = pd.merge(df_sale_on, df_goodlist[['DEFAULT_CODE', 'BST', 'REF_CAT1', 'REF_CAT2']], on='DEFAULT_CODE', how='left' )
    df_sale_on['TOTAL_AMOUNT'].fillna(0, inplace=True)
    df_sale_on['TOTAL_AMOUNT'] = df_sale_on['TOTAL_AMOUNT'].astype(int)
    df_sale_on['DATE'] = pd.to_datetime(df_sale_on['DATE'])
    df_sale_on['QTY'] = pd.to_numeric(df_sale_on['QTY'])
    df_sale_on_null = df_sale_on[df_sale_on['IDKH'].isnull()]
    df_sale_on_null['IDKH'] = df_sale_on_null.groupby('ORD_ID')['ORD_ID'].transform(lambda x: np.random.randint(100000, 999999))
    df_sale_on.update(df_sale_on_null)


    # SALE ECOM
    # GG_GG0004_006SALE_ECOM
    sht_com = ac.gs.open_by_key(f'''{cc.GG_GG0004_006SALE_ECOM}''')
    SHEET1_ECON = 'Rawdata'

    worksheet_ecom = sht_com.worksheet(SHEET1_ECON)
    data_ecom = worksheet_ecom.get('A1:O')
    name_columns = ['DATE_TIME', 'PF_CODE', 'ORD_ID', 'QTY', 'TOTAL_AMOUNT', 'STORE', 'DATE', 'DEFAULT_CODE', 'COLOR', 'SIZE', 'BST', 'REF_CAT1', 'REF_CAT2', 'REF_CAT3', 'REF_CAT4']
    df_sale_ecom = pd.DataFrame(data_ecom[1:], columns=name_columns)
    df_sale_ecom['ORD_ID'] = np.random.randint(1, 100001, size=len(df_sale_ecom))
    df_sale_ecom['DATE'] = pd.to_datetime(df_sale_ecom['DATE'], format='%Y/%m/%d')
    df_sale_ecom['QTY'] = pd.to_numeric(df_sale_ecom['QTY'])
    df_sale_ecom['TOTAL_AMOUNT'] = df_sale_ecom['TOTAL_AMOUNT'].str.replace(',','')
    df_sale_ecom['TOTAL_AMOUNT'] = pd.to_numeric(df_sale_ecom['TOTAL_AMOUNT'])

    df_sale_ecom_fix = df_sale_ecom[['ORD_ID', 'DATE', 'STORE', 'DEFAULT_CODE', 'COLOR', 'SIZE', 'QTY', 'TOTAL_AMOUNT','BST', 'REF_CAT1','REF_CAT2']]
    df_sale_ecom_fix_month = df_sale_ecom_fix[df_sale_ecom_fix['DATE'].dt.year == Year]

    total_sale = pd.concat([df_sale_off, df_sale_on, df_wholesale, df_sale_ecom_fix_month], ignore_index=True)

    def convert_date_format(date_str):
        if isinstance(date_str, str):  # Kiểm tra nếu giá trị là chuỗi
            for fmt in ("%d/%m/%Y", "%m/%d/%Y"):
                try:
                    return datetime.strptime(date_str, fmt).strftime("%d/%m/%Y")
                except ValueError:
                    continue
        return date_str  # Trả về giá trị gốc nếu không thể chuyển đổi

    total_sale['DATE'] = total_sale['DATE'].apply(convert_date_format)
    #total_sale['DATE'] = total_sale['DATE'].dt.strftime('%d/%m/%Y')
    total_sale['DATE'] = pd.to_datetime(total_sale['DATE'])
    worksheet_store = sht.worksheet(SHEET5)
    data_store = worksheet_store.get_all_values()
    df_store = pd.DataFrame(data_store[1:], columns=data_store[0])

    total_sale = pd.merge(total_sale, df_store[['STORE', 'TYPE']], on='STORE', how='left')
    total_sale['GROUP_AGE'] = total_sale['GROUP_AGE'].fillna('7.Unknown')
    total_sale['TYPE_CUSTOMER'] = total_sale['TYPE_CUSTOMER'].fillna('3.KHVL')
    def qty_dis(row):
        if row['REF_CAT1'] in ['Chiết khấu', 'PHIẾU GIẢM GIÁ']:
            return 0
        else:
            return row['QTY']

    # Áp dụng hàm vào cột QTY
    total_sale['QTY'] = total_sale.apply(qty_dis, axis=1)

    return total_sale

update_button_clicked = st.button('Cập nhật dữ liệu')

if update_button_clicked:
    fetch_and_prepare_data.clear()
    total_sale = fetch_and_prepare_data()
    st.success('Cập nhật dữ liệu mới thành công')
else:
    total_sale = fetch_and_prepare_data()

# FILTER
#////////////////////////////////////////////////////////////////////////
    # Tạo page 
page = st.sidebar.selectbox("Select Page", ["Page 1", "Page 2"]) # Có thể đặt tên lại

bst_options_fn = total_sale[~total_sale['BST'].isin(['Bao Nilong', 'Qua Tang', 'Discount', 'None', ''])]
bst_options_fn.replace(to_replace=[None], value=np.nan, inplace=True)
bst_options_fn = bst_options_fn.dropna(subset=['BST'])
bst_options = bst_options_fn['BST'].unique()
selected_bst = st.sidebar.multiselect("Chọn BST", bst_options)

channel_options = total_sale['TYPE'].unique()
selected_channel = st.sidebar.multiselect("Chọn CHANNEL", channel_options)

type_customer_options = sorted(total_sale['TYPE_CUSTOMER'].unique())
selected_type_customer = st.sidebar.multiselect("Chọn KHÁCH HÀNG", type_customer_options)

# Date range selection
start_date = st.sidebar.date_input("Từ ngày")
end_date = st.sidebar.date_input("Đến ngày")

# Convert start_date and end_date to datetime64[ns]
if start_date:
    start_date = pd.to_datetime(start_date)
if end_date:
    end_date = pd.to_datetime(end_date)

# Function to filter data
def get_filtered_data(total_sale, selected_bst, selected_channel, selected_type_customer, start_date, end_date):
    filters = (
        (total_sale['BST'].isin(selected_bst) if selected_bst else True) &
        (total_sale['TYPE'].isin(selected_channel) if selected_channel else True) &
        (total_sale['TYPE_CUSTOMER'].isin(selected_type_customer) if selected_type_customer else True)
    )
    
    if start_date and end_date:
        filters &= (total_sale['DATE'] >= start_date) & (total_sale['DATE'] <= end_date)
    
    return total_sale[filters]

# Assuming DATE column exists in your total_sale DataFrame
total_sale['DATE'] = pd.to_datetime(total_sale['DATE'])

# Filter the data
filtered_data = get_filtered_data(total_sale, selected_bst, selected_channel, selected_type_customer, start_date, end_date)
#////////////////////////////////////////////////////////////////////////

# SCORECARD
#/////////////////////////////////////////////////////////////////////////

#/////////////////////////////////////////////////////////////////////////
# REPORT
#////////////////////////////////////////////////////////////////////////
if page == "Page 1":
    if not filtered_data.empty:
        # Scorecard
        total_revenue = round(filtered_data['TOTAL_AMOUNT'].sum()/1000000000,2)
        total_qty = filtered_data['QTY'].fillna(0).astype(int).sum()
        total_orders = filtered_data['ORD_ID'].nunique()  # Assuming each date represents a unique order
            # Scorecard title
        #////////////////////////
        colsc1, colsc2, colsc3 = st.columns(3)
        with colsc1:
            st.metric(label="TOTAL GMV", value=f"{total_revenue:,}B")
        with colsc2:
            st.metric(label="TOTAL QTY", value=f"{total_qty:,}")
        with colsc3:
            st.metric(label="TOTAL ORD", value=f"{total_orders:,}")
    #//////////////////////////
        # TYPE
        channel_gr = filtered_data.groupby('TYPE').agg({
            'QTY': 'sum',
            'TOTAL_AMOUNT': 'sum'
        }).reset_index()
        channel_gr_sort = channel_gr.sort_values(by='TOTAL_AMOUNT', ascending=False)

        # Plot pie chart
        fig, ax = plt.subplots()
        ax.pie(channel_gr_sort['TOTAL_AMOUNT'], labels=channel_gr_sort['TYPE'], autopct='%1.1f%%', startangle=90)
        ax.axis('equal')  # Ensure the pie chart is circular
        #ax.set_title('DOANH THU THEO KÊNH')

        # Group by BST and get top 10
        bst_gr = filtered_data.groupby('BST')[['QTY', 'TOTAL_AMOUNT']].sum()
        top_10_bst_gr = bst_gr.nlargest(10, 'TOTAL_AMOUNT')
        
        # Color group
        color_gr = filtered_data.groupby('COLOR')[['QTY', 'TOTAL_AMOUNT']].sum()
        top_10_color = color_gr.nlargest(10, 'TOTAL_AMOUNT')

        # Store group
        store = filtered_data.groupby('STORE')[['QTY', 'TOTAL_AMOUNT']].sum()
        store_group = store.nlargest(13, 'TOTAL_AMOUNT')

        #//////////////////////////////////////
            # Tạo biểu đồ cột
        plt.figure(figsize=(13, 6))
        store_group['TOTAL_AMOUNT'].plot(kind='bar', color='#32cd00')
            
        # Thêm tiêu đề và nhãn cho trục
        plt.title('DOANH THU STORE')
        plt.xlabel('STORE')
        plt.ylabel('TOTAL GMV')
        
        #//////////////////////////    
        col1, col2 = st.columns(2)
        with col1:
            st.write('CHANNEL', channel_gr_sort)
        with col2:
            st.pyplot(fig)
        #///////////////////////////////////////
        st.pyplot(plt)
            #////////////////////////////////////////////////////
        try:            
            # Check if the date range is more than 31 days
            if (end_date - start_date).days > 31:
                filtered_data['MONTH'] = filtered_data['DATE'].dt.month
                month_group = filtered_data.groupby('MONTH')[['QTY', 'TOTAL_AMOUNT']].sum().reset_index()
                
                # Create figure and axes for the monthly report
                fig_month, ax1_month = plt.subplots(figsize=(10, 6))
                
                # Plot total amount as columns
                ax1_month.bar(month_group['MONTH'], month_group['TOTAL_AMOUNT'], color='#32cd00', width=0.5, alpha=0.7, label='Doanh thu')
                
                # Set labels for the x-axis and y-axis
                ax1_month.set_xlabel('Tháng')
                ax1_month.set_ylabel('Doanh thu', color='#32cd00')
                
                # Create a second axis for quantity
                ax2_month = ax1_month.twinx()
                ax2_month.plot(month_group['MONTH'], month_group['QTY'], color='orange', marker='o', linestyle='-', linewidth=2, markersize=8, label='Số lượng')
                
                # Add data labels for quantity
                for i, txt in enumerate(month_group['QTY']):
                    ax2_month.annotate(txt, (month_group['MONTH'][i], month_group['QTY'][i]), textcoords="offset points", xytext=(0,10), ha='center')
                
                # Set labels for the second y-axis
                ax2_month.set_ylabel('Số lượng', color='orange')
                
                # Add legend
                fig_month.legend(loc="upper left")
                
                # Set unique month ticks on x-axis
                ax1_month.set_xticks(month_group['MONTH'])
                ax1_month.xaxis.set_major_locator(plt.MaxNLocator(len(month_group['MONTH'])))
                
                # Set the title for the monthly chart
                plt.title('DOANH THU THEO THÁNG')
                plt.tight_layout()
                st.pyplot(fig_month)
            else:
                date_group = filtered_data.groupby('DATE')[['QTY', 'TOTAL_AMOUNT']].sum()
                
                # Create figure and axes for the daily report
                fig_date, ax1_date = plt.subplots(figsize=(10, 6))
                
                # Plot total amount as columns
                ax1_date.bar(date_group.index, date_group['TOTAL_AMOUNT'], color='#32cd00', width=0.5, alpha=0.7, label='Doanh thu')
                
                # Set labels for the x-axis and y-axis
                ax1_date.set_xlabel('Ngày')
                ax1_date.set_ylabel('Doanh thu', color='#32cd00')
                
                # Create a second axis for quantity
                ax2_date = ax1_date.twinx()
                ax2_date.plot(date_group.index, date_group['QTY'], color='orange', marker='o', linestyle='-', linewidth=2, markersize=8, label='Số lượng')
                
                # Add data labels for quantity
                for i, txt in enumerate(date_group['QTY']):
                    ax2_date.annotate(txt, (date_group.index[i], date_group['QTY'][i]), textcoords="offset points", xytext=(0,10), ha='center')
                
                # Set labels for the second y-axis
                ax2_date.set_ylabel('Số lượng', color='orange')
                
                # Add legend
                fig_date.legend(loc="upper left")
                
                # Set unique date ticks on x-axis
                ax1_date.set_xticks(date_group.index)
                ax1_date.xaxis.set_major_locator(plt.MaxNLocator(len(date_group.index) // 3))
                
                # Set the title for the daily chart
                plt.title('DOANH THU THEO NGÀY')
                plt.tight_layout()
                st.pyplot(fig_date)
        except ZeroDivisionError:
            st.write("Vui lòng chọn ngày kết thúc lớn hơn ngày bắt đầu 2 đơn vị để xem biểu đồ. Nếu khoảng thời gian mà bạn chọn lớn hơn 31 ngày biểu đồ sẽ chuyển sang dạng tháng")
        #///////////////////////////////////////
        col3, col4 = st.columns(2)
        with col3:
            st.write('TOP 10 BST:', top_10_bst_gr)
        with col4:
            st.write('TOP 10 COLOR', top_10_color)

        # Filter and group customer data
        df_cus = filtered_data.groupby(['GROUP_AGE']).agg({
            'IDKH': 'nunique',
            'ORD_ID': 'nunique',
            'QTY': 'sum',
            'TOTAL_AMOUNT': 'sum'
        }).reset_index()
        df_cus_fix_cat = df_cus[['GROUP_AGE', 'IDKH', 'ORD_ID', 'QTY', 'TOTAL_AMOUNT']]
        df_cus_fix_cat = df_cus_fix_cat.sort_values(by='GROUP_AGE', ascending=True)

        # Biểu đồ:
        fig_group_age, ax = plt.subplots()
        ax.pie(df_cus_fix_cat['TOTAL_AMOUNT'], labels=df_cus_fix_cat['GROUP_AGE'], autopct='%1.1f%%', startangle=90)
        ax.axis('equal')  # Ensure the pie chart is circular
        

        st.write('NHÓM TUỔI KHÁCH HÀNG', df_cus_fix_cat)
    
        st.pyplot(fig_group_age)
    else:
        st.write("Không có dữ liệu để vẽ biểu đồ.")
elif page == "Page 2":
    st.title("Page 2: COMING SOON")

