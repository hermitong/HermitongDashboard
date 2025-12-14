# HermitongDashboard

> 自动化交易数据分析平台 - 从 Excel 到 Looker 仪表盘的完整解决方案

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📊 项目简介

HermitongDashboard 是一个自动化交易数据处理和可视化分析系统,帮助交易者轻松管理和分析交易记录。

### 核心功能

- 🔄 **自动数据清洗**: 从券商导出的 Excel 文件中自动提取和清洗交易数据
- 📈 **FIFO 计算**: 采用先进先出算法精确计算持仓成本和已实现盈亏
- ☁️ **云端存储**: 自动上传至 Google Sheets,随时随地访问
- 📊 **可视化分析**: 通过 Looker Studio 创建专业的交易分析仪表盘
- 🔁 **增量更新**: 智能识别新文件,避免重复处理

### 系统架构

```
Excel 交易记录 → Python 数据清洗 → Google Sheets → Looker 仪表盘
```

## 🚀 快速开始

### 前置要求

- Python 3.7+
- Google 账号
- 券商交易记录 Excel 文件

### 安装步骤

1. **克隆项目**
   ```bash
   git clone https://github.com/你的用户名/HermitongDashboard.git
   cd HermitongDashboard
   ```

2. **安装依赖**
   ```bash
   pip3 install pandas gspread numpy openpyxl
   ```

3. **配置 Google Cloud**
   - 创建 Google Cloud 项目
   - 启用 Google Sheets API
   - 创建服务账号并下载 `credentials.json`
   - 将 `credentials.json` 放到项目根目录

4. **创建 Google Sheet**
   - 创建名为 `HermitongDashboard` 的 Google Sheet
   - 将 Sheet 共享给服务账号邮箱(在 `credentials.json` 中查找)

5. **准备交易数据**
   - 创建 `TradeRecord` 文件夹
   - 将交易记录 Excel 文件放入该文件夹

6. **运行脚本**
   ```bash
   python3 process_files.py
   ```

## 📖 详细文档

完整的部署和使用指南请查看:

- [**部署安装手册**](部署安装手册.md) - 详细的小白用户部署指南
  - 环境配置
  - Google Cloud 设置
  - Python 脚本配置
  - Looker 仪表盘配置
  - 常见问题解答

## 📁 项目结构

```
HermitongDashboard/
├── TradeRecord/              # 交易记录文件夹 (gitignore)
├── process_files.py          # 主处理脚本
├── credentials.json          # Google 服务账号密钥 (gitignore)
├── processed_files.txt       # 已处理文件日志 (gitignore)
├── 部署安装手册.md            # 详细部署指南
├── README.md                 # 项目说明
├── LICENSE                   # 许可证
└── .gitignore                # Git 忽略配置
```

## 🔧 配置说明

在 `process_files.py` 中修改以下配置:

```python
SOURCE_FOLDER_PATH = '/path/to/your/TradeRecord'  # 交易记录文件夹路径
GOOGLE_SHEET_NAME = 'HermitongDashboard'          # Google Sheet 名称
CREDENTIALS_FILE = 'credentials.json'             # 服务账号密钥文件
```

## 📊 数据处理流程

1. **数据提取**: 从 Excel 文件中提取已成交订单
2. **数据解析**: 自动识别股票和期权交易,解析期权代码
3. **FIFO 计算**: 按时间顺序匹配买卖订单,计算盈亏
4. **数据去重**: 基于多字段去重,确保数据唯一性
5. **数据上传**: 更新三个工作表:
   - **所有交易数据**: 完整的交易记录
   - **持仓中**: 当前持有的仓位
   - **已平仓**: 已平仓交易及盈亏统计

## 🎨 Looker 仪表盘

系统支持创建专业的交易分析仪表盘,包括:

- 📈 总盈亏和累计盈亏曲线
- 🎯 胜率和平均盈亏统计
- 📊 按资产类型、股票代码的盈亏分析
- 📅 时间序列分析
- 📋 详细交易记录表

详细配置步骤请参考 [部署安装手册](部署安装手册.md#looker-仪表盘配置)。

## 🔒 安全说明

本项目已通过 `.gitignore` 保护敏感信息:

- ✅ `credentials.json` - Google 服务账号密钥
- ✅ `TradeRecord/` - 交易记录文件夹
- ✅ `processed_files.txt` - 处理日志
- ✅ `*.xlsx`, `*.xls`, `*.csv` - 所有数据文件

**请勿将这些文件上传到公开仓库!**

## 🤝 贡献

欢迎提交 Issue 和 Pull Request!

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 💡 常见问题

### Q: 脚本提示连接 Google Sheets 失败?

确保:
1. `credentials.json` 文件存在且路径正确
2. Google Sheets API 已启用
3. Google Sheet 已共享给服务账号

### Q: 如何自动化运行脚本?

**macOS/Linux**: 使用 cron
```bash
crontab -e
# 添加: 0 8 * * * cd /path/to/project && python3 process_files.py
```

**Windows**: 使用任务计划程序

更多问题请查看 [部署安装手册 - 常见问题](部署安装手册.md#常见问题)

## 📞 支持

如有问题,请:
1. 查看 [部署安装手册](部署安装手册.md)
2. 提交 [GitHub Issue](https://github.com/你的用户名/HermitongDashboard/issues)

---

**祝你交易顺利! 📈**