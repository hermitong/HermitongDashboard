import os
import pandas as pd
import gspread
import re
from datetime import datetime
import numpy as np # 导入numpy库来处理nan

# --- 1. 配置区域 (请根据您的实际情况修改) ---

# [必填] 要监控的文件夹的绝对路径
# macOS/Linux示例: '/Users/edwin/Desktop/Study/HermitongDashboard(Google)/TradeRecord'
SOURCE_FOLDER_PATH = '/Users/edwin/Desktop/Study/HermitongDashboard(Google)/TradeRecord' # <--- 修改这里

# [必填] Google Sheet的名称 (必须与您在Google Drive中的文件名完全一致)
GOOGLE_SHEET_NAME = 'HermitongDashboard' # <--- 修改这里

# 用于记录已处理文件的文件名 (保持默认即可)
PROCESSED_FILES_LOG = 'processed_files.txt'

# Google API凭据文件名 (保持默认即可)
CREDENTIALS_FILE = 'credentials.json'


# --- 2. 核心数据清洗函数 (保持上一版的升级逻辑) ---

def clean_and_transform_orders(file_path):
    """
    (升级版) 读取交易数据Excel文件，并根据特定规则进行清洗和转换。
    """
    try:
        df = pd.read_excel(file_path)
        df = df[df['Order Status'] == '已成交'].copy()
        if df.empty:
            return None

        def parse_symbol(symbol):
            option_match = re.match(r'^([A-Z.]{2,6})(\d{6})([CP])(\d+)$', str(symbol))
            if option_match:
                stock, expiry, direction_code, strike_raw = option_match.groups()
                direction = 'Call' if direction_code == 'C' else 'Put'
                strike_price = float(strike_raw) / 1000.0
                return stock, expiry, direction, strike_price
            else:
                return symbol, None, None, None
        
        df[['Stock', '行权日', '期权方向', '行权价']] = df['Symbol'].apply(lambda x: pd.Series(parse_symbol(x)))

        def convert_order_qty(qty):
            s_qty = str(qty).strip()
            if '张' in s_qty:
                num_match = re.search(r'(\d+)', s_qty)
                return int(num_match.group(1)) * 100 if num_match else None
            else:
                return pd.to_numeric(s_qty, errors='coerce')

        df['数量'] = df['Order Qty'].apply(convert_order_qty)
        
        df.rename(columns={'Direction': '方向', 'Avg Price': '价格'}, inplace=True)
        time_series = pd.to_datetime(df['Order Time'].astype(str).str.replace(' ET', '', regex=False), errors='coerce')
        df['交易日期'] = time_series.dt.strftime('%Y-%m-%d')
        df['交易时间'] = time_series.dt.strftime('%H:%M:%S')

        final_columns = [
            '交易日期', '交易时间', 'Stock', '方向', '数量', '价格', 
            '行权日', '期权方向', '行权价'
        ]
        existing_final_columns = [col for col in final_columns if col in df.columns]
        df_final = df[existing_final_columns]
        
        return df_final

    except Exception as e:
        print(f"在 clean_and_transform_orders 中处理文件 {file_path} 时出错: {e}")
        return None

# --- 3. 脚本主流程函数 (已添加nan修复逻辑) ---

def get_new_files(folder_path, log_file):
    # (此函数保持不变)
    if not os.path.exists(log_file):
        with open(log_file, 'w') as f: pass
    with open(log_file, 'r') as f:
        processed_files = set(line.strip() for line in f)
    all_files_in_folder = set(os.listdir(folder_path))
    new_files = all_files_in_folder - processed_files
    new_xlsx_files = [f for f in new_files if not f.startswith('~') and f.lower().endswith('.xlsx')]
    print(f"发现 {len(new_xlsx_files)} 个新的Excel文件: {new_xlsx_files}")
    return new_xlsx_files

def process_single_file(file_path):
    print(f"正在处理文件: {file_path}...")
    cleaned_df = clean_and_transform_orders(file_path)
    if cleaned_df is None or cleaned_df.empty:
        print(f"文件 {os.path.basename(file_path)} 未产生有效数据。")
        return None
    
    # --- [关键修复] ---
    # 在上传前，将所有 nan 值替换为空字符串 ''
    cleaned_df = cleaned_df.replace({np.nan: ''})
    
    processed_data = cleaned_df.values.tolist()
    print(f"文件处理成功，提取到 {len(cleaned_df)} 条有效记录。")
    return processed_data, cleaned_df.columns.tolist()

def update_google_sheet(data_to_append, headers):
    # (此函数保持不变)
    if not data_to_append:
        print("没有数据需要写入Google Sheet。")
        return
    try:
        print("正在连接Google Sheet...")
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        worksheet = gc.open(GOOGLE_SHEET_NAME).sheet1
        
        if not worksheet.get_all_values():
            print("工作表为空，正在写入表头...")
            worksheet.append_row(headers, value_input_option='USER_ENTERED')
            
        print(f"正在向 '{GOOGLE_SHEET_NAME}' 中追加 {len(data_to_append)} 行数据...")
        worksheet.append_rows(data_to_append, value_input_option='USER_ENTERED')
        print("数据写入成功！")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"错误：找不到名为 '{GOOGLE_SHEET_NAME}' 的Google Sheet。请检查名称是否完全匹配，并且您已经将表格分享给了服务账号的邮箱。")
    except Exception as e:
        print(f"更新Google Sheet时出错: {e}")

def mark_as_processed(files, log_file):
    # (此函数保持不变)
    with open(log_file, 'a') as f:
        for file in files:
            f.write(file + '\n')
    print(f"已将 {len(files)} 个文件标记为已处理。")

def main():
    # (此函数保持不变)
    print(f"--- 脚本开始运行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    if not os.path.isdir(SOURCE_FOLDER_PATH):
        print(f"错误：配置的源文件夹路径不存在: {SOURCE_FOLDER_PATH}")
        print("--- 脚本运行结束 ---")
        return

    new_files = get_new_files(SOURCE_FOLDER_PATH, PROCESSED_FILES_LOG)
    if not new_files:
        print("--- 没有新文件需要处理，脚本运行结束。 ---")
        return
        
    all_data_for_sheet = []
    successfully_processed_files = []
    final_headers = []

    for file_name in new_files:
        full_file_path = os.path.join(SOURCE_FOLDER_PATH, file_name)
        result = process_single_file(full_file_path)
        
        if result:
            data, headers = result
            all_data_for_sheet.extend(data)
            successfully_processed_files.append(file_name)
            if not final_headers: 
                final_headers = headers

    if all_data_for_sheet:
        update_google_sheet(all_data_for_sheet, final_headers)
    
    if successfully_processed_files:
        mark_as_processed(successfully_processed_files, PROCESSED_FILES_LOG)
    
    print("--- 脚本运行结束 ---")

if __name__ == "__main__":
    main()