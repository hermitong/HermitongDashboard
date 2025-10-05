import os
import pandas as pd
import gspread
import re
from datetime import datetime
import numpy as np

# --- 1. 配置区域 ---
SOURCE_FOLDER_PATH = '/Users/edwin/Desktop/Study/HermitongDashboard(Google)/TradeRecord' # <--- 修改这里
GOOGLE_SHEET_NAME = 'HermitongDashboard'  # <--- 修改这里
CREDENTIALS_FILE = 'credentials.json'
PROCESSED_FILES_LOG = 'processed_files.txt'

# 工作表名称配置
SHEET_ALL_TRADES = '所有交易数据'
SHEET_OPEN_POSITIONS = '持仓中'
SHEET_CLOSED_POSITIONS = '已平仓'

# 手动输入列配置
MANUAL_COLUMNS_OPEN = ['消息来源']
MANUAL_COLUMNS_CLOSED = ['消息来源', '平仓理由']


# --- 2. 核心数据清洗函数 (保持不变) ---
def clean_and_transform_orders(file_path):
    try:
        df = pd.read_excel(file_path)
        df = df[df['Order Status'] == '已成交'].copy()
        if df.empty: return None

        def parse_symbol(symbol):
            option_match = re.match(r'^([A-Z.]{2,6})(\d{6})([CP])(\d+)$', str(symbol))
            if option_match:
                stock, expiry, direction_code, strike_raw = option_match.groups()
                direction = 'Call' if direction_code == 'C' else 'Put'
                strike_price = float(strike_raw) / 1000.0
                asset_type = '期权'
                return stock, expiry, direction, strike_price, asset_type
            else:
                return symbol, None, None, None, '股票'
        
        df[['Stock', '行权日', '期权方向', '行权价', '资产类型']] = df['Symbol'].apply(lambda x: pd.Series(parse_symbol(x)))

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

        final_columns = ['交易日期', '交易时间', '资产类型', 'Stock', '方向', '数量', '价格', '行权日', '期权方向', '行权价']
        df_final = df[[col for col in final_columns if col in df.columns]]
        
        return df_final
    except Exception as e:
        print(f"清洗文件 {file_path} 时出错: {e}")
        return None

# --- 3. FIFO 核心计算逻辑 (保持不变) ---
def calculate_positions_fifo(df_all_trades):
    df_all_trades['datetime'] = pd.to_datetime(df_all_trades['交易日期'] + ' ' + df_all_trades['交易时间'])
    df_all_trades = df_all_trades.sort_values(by='datetime').reset_index(drop=True)

    open_positions = {}
    closed_trades = []

    def create_asset_key(row):
        if row['资产类型'] == '期权':
            return f"OPT_{row['Stock']}_{row['行权日']}_{row['期权方向']}_{row['行权价']}"
        else:
            return f"STK_{row['Stock']}"

    for index, row in df_all_trades.iterrows():
        asset_key = create_asset_key(row)
        if row['方向'] == 'Buy':
            if asset_key not in open_positions: open_positions[asset_key] = []
            open_positions[asset_key].append({'qty': row['数量'], 'price': row['价格'], 'date': row['交易日期'], 'row_data': row.to_dict()})
        elif row['方向'] == 'Sell':
            if asset_key not in open_positions or not open_positions[asset_key]: continue
            sell_qty_remaining = row['数量']
            while sell_qty_remaining > 0 and open_positions[asset_key]:
                oldest_buy_lot = open_positions[asset_key][0]
                qty_to_close = min(sell_qty_remaining, oldest_buy_lot['qty'])
                
                buy_cost = oldest_buy_lot['price'] * qty_to_close
                pnl = (row['价格'] - oldest_buy_lot['price']) * qty_to_close
                return_rate = pnl / buy_cost if buy_cost != 0 else 0
                
                days_to_expiry = None
                if oldest_buy_lot['row_data']['资产类型'] == '期权':
                    try:
                        buy_date = pd.to_datetime(oldest_buy_lot['date'])
                        expiry_date_str = str(int(oldest_buy_lot['row_data']['行权日']))
                        expiry_date = pd.to_datetime('20' + expiry_date_str, format='%Y%m%d')
                        days_to_expiry = (expiry_date - buy_date).days
                    except (ValueError, TypeError):
                        days_to_expiry = None

                closed_trades.append({
                    '资产类型': oldest_buy_lot['row_data']['资产类型'], '资产代码': oldest_buy_lot['row_data']['Stock'],
                    '买入日期': oldest_buy_lot['date'], '平仓日期': row['交易日期'],
                    '平仓数量': qty_to_close, '买入价格': oldest_buy_lot['price'], '卖出价格': row['价格'],
                    '已实现盈亏': pnl, '收益率': return_rate, '距离到期日的天数': days_to_expiry,
                    '期权信息': f"{oldest_buy_lot['row_data']['行权日']} {oldest_buy_lot['row_data']['期权方向']} @{oldest_buy_lot['row_data']['行权价']}" if oldest_buy_lot['row_data']['资产类型'] == '期权' else ''
                })
                sell_qty_remaining -= qty_to_close
                oldest_buy_lot['qty'] -= qty_to_close
                if oldest_buy_lot['qty'] < 1e-9: open_positions[asset_key].pop(0)

    open_positions_list = []
    for key, lots in open_positions.items():
        if not lots: continue
        total_qty, avg_cost, first_lot_data = sum(l['qty'] for l in lots), sum(l['qty'] * l['price'] for l in lots) / sum(l['qty'] for l in lots), lots[0]['row_data']
        days_to_expiry = None
        if first_lot_data['资产类型'] == '期权':
            try:
                buy_date = pd.to_datetime(lots[0]['date'])
                expiry_date_str = str(int(first_lot_data['行权日']))
                expiry_date = pd.to_datetime('20' + expiry_date_str, format='%Y%m%d')
                days_to_expiry = (expiry_date - buy_date).days
            except (ValueError, TypeError):
                days_to_expiry = None
        
        open_positions_list.append({
            '资产类型': first_lot_data['资产类型'], '资产代码': first_lot_data['Stock'],
            '持仓数量': total_qty, '平均成本': avg_cost, '开仓日期': lots[0]['date'],
            '距离到期日的天数': days_to_expiry,
            '期权信息': f"{first_lot_data['行权日']} {first_lot_data['期权方向']} @{first_lot_data['行权价']}" if first_lot_data['资产类型'] == '期权' else ''
        })
    return (pd.DataFrame(open_positions_list), pd.DataFrame(closed_trades))

# --- 4. 智能合并手动输入数据 (保持不变) ---
def merge_manual_data(new_df, old_df, key_cols, manual_cols):
    if old_df.empty:
        for col in manual_cols: new_df[col] = ''
        return new_df
    preserved_data = old_df[key_cols + [col for col in manual_cols if col in old_df.columns]].copy()
    for key in key_cols:
      preserved_data[key] = preserved_data[key].astype(str)
      new_df[key] = new_df[key].astype(str)
    merged_df = pd.merge(new_df, preserved_data, on=key_cols, how='left')
    for col in manual_cols:
        if col not in merged_df.columns: merged_df[col] = ''
    return merged_df

# --- 5. 主流程与Google Sheet交互 (已升级) ---
def get_new_files(folder_path, log_file):
    if not os.path.exists(log_file):
        with open(log_file, 'w') as f: pass
    with open(log_file, 'r') as f:
        processed_files = set(line.strip() for line in f)
    all_files_in_folder = set(os.listdir(folder_path))
    new_files = all_files_in_folder - processed_files
    new_xlsx_files = [f for f in new_files if not f.startswith('~') and f.lower().endswith('.xlsx')]
    print(f"发现 {len(new_xlsx_files)} 个新的Excel文件: {new_xlsx_files}")
    return new_xlsx_files

def update_sheets(gc, sheet_name_to_df_map):
    """(V4版) 上传前将所有数据强制转为字符串，防止JSON序列化错误"""
    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    for sheet_name, df in sheet_name_to_df_map.items():
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            print(f"正在清空并更新工作表: '{sheet_name}'...")
        except gspread.WorksheetNotFound:
            print(f"工作表 '{sheet_name}' 不存在，正在创建...")
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="30")
        
        # --- [关键修复] ---
        # 1. 将整个DataFrame的所有内容都转换为字符串
        # 2. 将所有表示空值的字符串（'nan', 'NaT'等）替换为真正的空白''
        df_upload = df.astype(str).replace({'nan': '', 'NaT': '', 'None': ''})
        
        worksheet.clear()
        worksheet.update([df_upload.columns.values.tolist()] + df_upload.values.tolist(), value_input_option='USER_ENTERED')
        print(f"'{sheet_name}' 更新成功！")

def main():
    print(f"--- 脚本开始运行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    try: gc = gspread.service_account(filename=CREDENTIALS_FILE)
    except Exception as e: print(f"连接Google Sheets失败: {e}"); return

    sheet_data = {}
    for sheet_name in [SHEET_ALL_TRADES, SHEET_OPEN_POSITIONS, SHEET_CLOSED_POSITIONS]:
        try:
            worksheet = gc.open(GOOGLE_SHEET_NAME).worksheet(sheet_name)
            records = worksheet.get_all_records()
            sheet_data[sheet_name] = pd.DataFrame(records)
            print(f"成功读取 '{sheet_name}' 中的 {len(records)} 条历史记录。")
        except gspread.WorksheetNotFound:
            print(f"工作表 '{sheet_name}' 不存在。")
            sheet_data[sheet_name] = pd.DataFrame()
        except Exception as e:
            print(f"读取 '{sheet_name}' 失败: {e}"); sheet_data[sheet_name] = pd.DataFrame()

    new_files = get_new_files(SOURCE_FOLDER_PATH, PROCESSED_FILES_LOG)
    new_trades_list = [clean_and_transform_orders(os.path.join(SOURCE_FOLDER_PATH, f)) for f in new_files]
    new_trades_df = pd.concat([df for df in new_trades_list if df is not None], ignore_index=True)
    
    if new_trades_df.empty and sheet_data[SHEET_ALL_TRADES].empty:
        print("--- 没有历史数据，也没有新文件需要处理。脚本运行结束。 ---"); return

    all_trades_df = pd.concat([sheet_data[SHEET_ALL_TRADES].astype(str), new_trades_df.astype(str)], ignore_index=True)
    all_trades_df = all_trades_df.replace({'nan': None, 'None': None, '': None, 'NaT': None})
    for col in ['数量', '价格', '行权价']:
        if col in all_trades_df.columns:
            all_trades_df[col] = pd.to_numeric(all_trades_df[col], errors='coerce')
    
    print("开始执行FIFO算法...")
    df_open, df_closed = calculate_positions_fifo(all_trades_df)
    print("FIFO计算完成！")
    
    # 智能合并
    df_open = merge_manual_data(df_open, sheet_data[SHEET_OPEN_POSITIONS], key_cols=['资产代码', '开仓日期', '期权信息'], manual_cols=MANUAL_COLUMNS_OPEN)
    df_closed = merge_manual_data(df_closed, sheet_data[SHEET_CLOSED_POSITIONS], key_cols=['资产代码', '买入日期', '平仓日期', '期权信息'], manual_cols=MANUAL_COLUMNS_CLOSED)
    
    if '收益率' in df_closed.columns:
        df_closed['收益率'] = pd.to_numeric(df_closed['收益率'], errors='coerce').apply(lambda x: f"{x:.2%}" if pd.notna(x) else '')

    sheets_to_update = {
        SHEET_ALL_TRADES: all_trades_df,
        SHEET_OPEN_POSITIONS: df_open,
        SHEET_CLOSED_POSITIONS: df_closed
    }
    update_sheets(gc, sheets_to_update)

    if new_files:
        with open(PROCESSED_FILES_LOG, 'a') as f:
            for file in new_files: f.write(file + '\n')
        print(f"已将 {len(new_files)} 个新文件标记为已处理。")
    
    print("--- 脚本运行结束 ---")

if __name__ == "__main__":
    main()