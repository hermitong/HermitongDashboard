import os
import pandas as pd
import gspread
import re
from datetime import datetime
import numpy as np

# --- 1. 配置区域 ---
SOURCE_FOLDER_PATH = '/Users/edwin/Desktop/Study/HermitongDashboard(Google)/TradeRecord' 
GOOGLE_SHEET_NAME = 'HermitongDashboard' 
CREDENTIALS_FILE = 'credentials.json'
PROCESSED_FILES_LOG = 'processed_files.txt'

SHEET_ALL_TRADES = '所有交易数据'
SHEET_OPEN_POSITIONS = '持仓中'
SHEET_CLOSED_POSITIONS = '已平仓'

MANUAL_COLUMNS_OPEN = ['消息来源']
MANUAL_COLUMNS_CLOSED = ['消息来源', '平仓理由']


# --- 2. 核心数据清洗函数 ---
def clean_and_transform_orders(file_path):
    try:
        df = pd.read_excel(file_path)
        df = df[df['Order Status'] == '已成交'].copy()
        if df.empty: 
            # print(f"文件 {os.path.basename(file_path)} 中没有找到'已成交'的订单。")
            return None

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
        
        # 强制生成标准时间戳 (带T)
        time_series = pd.to_datetime(df['Order Time'].astype(str).str.replace(' ET', '', regex=False), errors='coerce')
        df['交易时间戳'] = time_series.dt.strftime('%Y-%m-%dT%H:%M:%S')

        final_columns = ['交易时间戳', '资产类型', 'Stock', '方向', '数量', '价格', '行权日', '期权方向', '行权价']
        df_final = df[[col for col in final_columns if col in df.columns]]
        
        return df_final
    except Exception as e:
        print(f"清洗文件 {file_path} 时出错: {e}")
        return None

# --- 3. FIFO 核心计算逻辑 ---
def calculate_positions_fifo(df_all_trades):
    # 确保有 datetime 对象列
    if 'datetime' not in df_all_trades.columns:
        df_all_trades['datetime'] = pd.to_datetime(df_all_trades['交易时间戳'], errors='coerce')
    
    # 再次排序确保顺序正确
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
            open_positions[asset_key].append({'qty': row['数量'], 'price': row['价格'], 'datetime': row['datetime'], 'row_data': row.to_dict()})
        elif row['方向'] == 'Sell':
            if asset_key not in open_positions or not open_positions[asset_key]: continue
            
            sell_qty_remaining = row['数量']
            while sell_qty_remaining > 0 and open_positions[asset_key]:
                oldest_buy_lot = open_positions[asset_key][0]
                
                # 容错处理：允许1秒内的误差
                if row['datetime'] < oldest_buy_lot['datetime']:
                    time_diff = (oldest_buy_lot['datetime'] - row['datetime']).total_seconds()
                    if time_diff > 1: 
                        # print(f"警告：卖出时间早于买入时间。卖出: {row['datetime']}, 买入: {oldest_buy_lot['datetime']}")
                        break 

                qty_to_close = min(sell_qty_remaining, oldest_buy_lot['qty'])
                buy_cost = oldest_buy_lot['price'] * qty_to_close
                pnl = (row['价格'] - oldest_buy_lot['price']) * qty_to_close
                return_rate = pnl / buy_cost if buy_cost != 0 else 0
                
                buy_data = oldest_buy_lot['row_data']
                asset_direction = f"{buy_data['方向']} {buy_data['期权方向']}" if buy_data['资产类型'] == '期权' else buy_data['方向']
                win_loss = 1 if pnl > 0 else 0
                
                holding_days = (row['datetime'] - oldest_buy_lot['datetime']).days
                
                days_to_expiry = None
                if buy_data['资产类型'] == '期权':
                    try:
                        # 兼容处理，防止timestamp丢失
                        buy_date = oldest_buy_lot['datetime']
                        expiry_date_str = str(int(buy_data['行权日']))
                        expiry_date = pd.to_datetime('20' + expiry_date_str, format='%Y%m%d')
                        days_to_expiry = (expiry_date - buy_date).days
                    except (ValueError, TypeError): days_to_expiry = None

                closed_trades.append({
                    '资产类型': buy_data['资产类型'], '资产代码': buy_data['Stock'], '标的方向': asset_direction,
                    '买入时间': oldest_buy_lot['row_data']['交易时间戳'], '平仓时间': row['交易时间戳'], '持仓天数': holding_days,
                    '平仓数量': qty_to_close, '买入价格': oldest_buy_lot['price'], '卖出价格': row['价格'],
                    '已实现盈亏': pnl, '胜负': win_loss, '收益率': return_rate, '距离到期日的天数': days_to_expiry,
                    '期权信息': f"{buy_data['行权日']} {buy_data['期权方向']} @{buy_data['行权价']}" if buy_data['资产类型'] == '期权' else ''
                })
                
                sell_qty_remaining -= qty_to_close
                oldest_buy_lot['qty'] -= qty_to_close
                if oldest_buy_lot['qty'] < 1e-9: open_positions[asset_key].pop(0)
    
    df_closed = pd.DataFrame(closed_trades)
    if not df_closed.empty:
        # 1. 先按平仓时间排序 (升序，为了计算累计)
        df_closed = df_closed.sort_values(by='平仓时间', ascending=True).reset_index(drop=True)
        # 2. 计算累计盈亏
        df_closed['累计盈亏'] = df_closed['已实现盈亏'].cumsum()
        # 3. 最后按降序排列 (为了展示)
        df_closed = df_closed.sort_values(by='平仓时间', ascending=False).reset_index(drop=True)

    open_positions_list = []
    for key, lots in open_positions.items():
        if not lots: continue
        total_qty = sum(l['qty'] for l in lots)
        avg_cost = sum(l['qty'] * l['price'] for l in lots) / total_qty if total_qty > 0 else 0
        first_lot_data = lots[0]['row_data']
        days_to_expiry = None
        if first_lot_data['资产类型'] == '期权':
            try:
                buy_date = lots[0]['datetime']
                expiry_date_str = str(int(first_lot_data['行权日']))
                expiry_date = pd.to_datetime('20' + expiry_date_str, format='%Y%m%d')
                days_to_expiry = (expiry_date - buy_date).days
            except (ValueError, TypeError): days_to_expiry = None
        
        open_positions_list.append({
            '资产类型': first_lot_data['资产类型'], '资产代码': first_lot_data['Stock'],
            '持仓数量': total_qty, '平均成本': avg_cost, '开仓时间': first_lot_data['交易时间戳'],
            '距离到期日的天数': days_to_expiry,
            '期权信息': f"{first_lot_data['行权日']} {first_lot_data['期权方向']} @{first_lot_data['行权价']}" if first_lot_data['资产类型'] == '期权' else ''
        })
    
    df_open = pd.DataFrame(open_positions_list)
    if not df_open.empty:
        df_open = df_open.sort_values(by='开仓时间', ascending=False).reset_index(drop=True)
        
    return (df_open, df_closed)

# --- 4. 智能合并手动输入数据 ---
def merge_manual_data(new_df, old_df, key_cols, manual_cols):
    if old_df.empty:
        for col in manual_cols: new_df[col] = ''
        return new_df
    if not all(k in new_df.columns for k in key_cols) or not all(k in old_df.columns for k in key_cols):
        # print("警告: 缺少用于合并的key列，将跳过手动数据合并。")
        for col in manual_cols: new_df[col] = ''
        return new_df
    preserved_data = old_df[key_cols + [col for col in manual_cols if col in old_df.columns]].copy()
    preserved_data = preserved_data.drop_duplicates(subset=key_cols, keep='last')
    for key in key_cols:
      preserved_data[key] = preserved_data[key].astype(str).str.strip()
      new_df[key] = new_df[key].astype(str).str.strip()
    merged_df = pd.merge(new_df, preserved_data, on=key_cols, how='left')
    for col in manual_cols:
        if col not in merged_df.columns: merged_df[col] = ''
    return merged_df

# --- 5. 辅助工具：规范化 Google Sheet 数据 ---
def normalize_sheet_data(df):
    """
    专门用于处理从 Google Sheet 读取的数据。
    强制将时间列标准化为 'T' 分隔格式，防止因为 Google 自动格式化导致的历史数据丢失。
    """
    if df.empty or '交易时间戳' not in df.columns:
        return df
    
    # 强制转为字符串，并将可能存在的空格替换为 T
    # 这一步是修复历史数据丢失的关键！
    df['交易时间戳'] = df['交易时间戳'].astype(str).str.replace(' ', 'T')
    
    return df

# --- 6. 主流程 ---
def get_new_files(folder_path, log_file):
    if not os.path.exists(log_file):
        with open(log_file, 'w') as f: pass
    with open(log_file, 'r') as f:
        processed_files = set(line.strip() for line in f)
    all_files_in_folder = set(os.listdir(folder_path))
    new_files = all_files_in_folder - processed_files
    new_xlsx_files = [f for f in new_files if not f.startswith('~') and f.lower().endswith('.xlsx')]
    print(f"发现 {len(new_xlsx_files)} 个新的Excel文件")
    return new_xlsx_files

def update_sheets(gc, sheet_name_to_df_map):
    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    for sheet_name, df in sheet_name_to_df_map.items():
        if df is None: continue
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="30")
        
        # 将 NaT/NaN 替换为空字符串
        df_upload = df.copy()
        df_upload = df_upload.astype(object) # 转为object以支持混合类型替换
        df_upload.fillna('', inplace=True)
        
        # 确保所有内容都是字符串，防止 JSON 序列化错误
        df_upload = df_upload.astype(str)
        
        worksheet.clear()
        worksheet.update([df_upload.columns.values.tolist()] + df_upload.values.tolist(), value_input_option='USER_ENTERED')
        print(f"'{sheet_name}' 更新成功！")

def main():
    print(f"--- 脚本开始运行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    try: gc = gspread.service_account(filename=CREDENTIALS_FILE)
    except Exception as e: print(f"连接Google Sheets失败: {e}"); return

    # 读取历史数据
    sheet_data = {}
    for sheet_name in [SHEET_ALL_TRADES, SHEET_OPEN_POSITIONS, SHEET_CLOSED_POSITIONS]:
        try:
            worksheet = gc.open(GOOGLE_SHEET_NAME).worksheet(sheet_name)
            records = worksheet.get_all_records()
            df = pd.DataFrame(records)
            # 【关键修复】读取后立即规范化历史数据
            if sheet_name == SHEET_ALL_TRADES:
                df = normalize_sheet_data(df)
            sheet_data[sheet_name] = df
            print(f"从Sheet读取到 {len(df)} 条 '{sheet_name}' 记录。")
        except gspread.WorksheetNotFound:
            sheet_data[sheet_name] = pd.DataFrame()
        except Exception as e:
            print(f"读取 '{sheet_name}' 失败: {e}"); sheet_data[sheet_name] = pd.DataFrame()

    # 处理新文件
    new_files = get_new_files(SOURCE_FOLDER_PATH, PROCESSED_FILES_LOG)
    new_trades_list = [clean_and_transform_orders(os.path.join(SOURCE_FOLDER_PATH, f)) for f in new_files]
    valid_new_trades = [df for df in new_trades_list if df is not None]
    
    new_trades_df = pd.concat(valid_new_trades, ignore_index=True) if valid_new_trades else pd.DataFrame()
    print(f"从新文件中解析到 {len(new_trades_df)} 条新记录。")

    if new_trades_df.empty and sheet_data[SHEET_ALL_TRADES].empty:
        print("没有数据需要处理。结束。"); return

    # --- 数据合并与去重 ---
    print("开始合并与数据校验...")
    
    # 合并
    all_trades_df = pd.concat([sheet_data[SHEET_ALL_TRADES], new_trades_df], ignore_index=True)
    
    # 统一时间解析 (兼容 T 和 空格)
    # 这一步非常关键：它将所有字符串（无论格式如何）转为 datetime 对象
    all_trades_df['datetime'] = pd.to_datetime(all_trades_df['交易时间戳'], errors='coerce')
    
    # 剔除无效时间
    original_count = len(all_trades_df)
    all_trades_df.dropna(subset=['datetime'], inplace=True)
    dropped_count = original_count - len(all_trades_df)
    if dropped_count > 0:
        print(f"警告：丢弃了 {dropped_count} 条时间格式无效的记录 (可能是空行或格式极度混乱)。")

    # 重建标准时间戳字符串 (用于去重和上传)
    all_trades_df['交易时间戳'] = all_trades_df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # 去重
    trade_key_cols = ['交易时间戳', '资产类型', 'Stock', '方向', '数量', '价格', '行权日', '期权方向', '行权价']
    # 字符标准化
    for col in trade_key_cols:
        if col in all_trades_df.columns:
            all_trades_df[col] = all_trades_df[col].astype(str).str.strip()
    # 数字标准化
    for col in ['数量', '价格']:
         if col in all_trades_df.columns:
            all_trades_df[col] = pd.to_numeric(all_trades_df[col], errors='coerce').round(4)
            
    all_trades_df = all_trades_df.drop_duplicates(subset=trade_key_cols).reset_index(drop=True)
    print(f"去重后，总计有效记录: {len(all_trades_df)} 条。")

    # --- 计算 ---
    print("开始执行FIFO计算...")
    df_open, df_closed = calculate_positions_fifo(all_trades_df)
    
    # 合并手动数据
    df_open = merge_manual_data(df_open, sheet_data[SHEET_OPEN_POSITIONS], key_cols=['资产代码', '开仓时间', '期权信息'], manual_cols=MANUAL_COLUMNS_OPEN)
    df_closed = merge_manual_data(df_closed, sheet_data[SHEET_CLOSED_POSITIONS], key_cols=['资产代码', '买入时间', '平仓时间', '期权信息'], manual_cols=MANUAL_COLUMNS_CLOSED)
    
    # 格式化
    if '收益率' in df_closed.columns:
        df_closed['收益率'] = pd.to_numeric(df_closed['收益率'], errors='coerce').apply(lambda x: f"{x:.2%}" if pd.notna(x) else '')

    # 排序并上传
    all_trades_df_sorted = all_trades_df.sort_values(by='datetime', ascending=False)
    
    sheets_to_update = {
        SHEET_ALL_TRADES: all_trades_df_sorted.drop(columns=['datetime'], errors='ignore'),
        SHEET_OPEN_POSITIONS: df_open,
        SHEET_CLOSED_POSITIONS: df_closed
    }
    update_sheets(gc, sheets_to_update)

    if new_files:
        with open(PROCESSED_FILES_LOG, 'a') as f:
            for file in new_files: f.write(file + '\n')
        print(f"标记 {len(new_files)} 个新文件为已处理。")
    
    print("--- 脚本运行结束 ---")

if __name__ == "__main__":
    main()