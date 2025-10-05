# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based trading record processing system that automatically reads Excel files containing trading data from a specified directory, processes and cleans the data, and uploads it to a Google Sheet dashboard named "HermitongDashboard".

## Architecture

The codebase consists of a single main Python script (`process_files.py`) that handles:

1. **File Monitoring**: Scans the `TradeRecord/` directory for new Excel files
2. **Data Processing**: Extracts and cleans trading data, handling both stock and option trades
3. **Google Sheets Integration**: Uploads processed data to a Google Sheet using service account authentication

### Key Components

- `process_files.py`: Main script containing all processing logic
  - `clean_and_transform_orders()`: Core data cleaning function that filters for completed orders, parses option symbols, and formats data
  - `get_new_files()`: Identifies unprocessed Excel files
  - `process_single_file()`: Handles individual file processing
  - `update_google_sheet()`: Manages Google Sheets operations
  - `main()`: Orchestrates the entire workflow

## Configuration

The system uses these key configuration variables in `process_files.py`:
- `SOURCE_FOLDER_PATH`: Path to the TradeRecord directory (default: `/Users/edwin/Desktop/Study/HermitongDashboard(Google)/TradeRecord`)
- `GOOGLE_SHEET_NAME`: Target Google Sheet name (default: 'HermitongDashboard')
- `CREDENTIALS_FILE`: Google service account credentials (default: 'credentials.json')
- `PROCESSED_FILES_LOG`: Tracks processed files (default: 'processed_files.txt')

## Development Commands

### Running the Main Script
```bash
python3 process_files.py
```

### Installing Dependencies
The script requires these Python packages:
```bash
pip3 install pandas gspread numpy openpyxl
```

### Testing Individual Components
You can test individual functions by importing them in a Python shell:
```python
python3 -c "from process_files import clean_and_transform_orders; print('Import successful')"
```

## Data Processing Details

The system specifically processes trading data with these transformations:
- Filters for orders with status "已成交" (completed)
- Parses option symbols in format like "AAPL251217C00150000" to extract stock, expiry, direction, and strike price
- Converts order quantities (handles Chinese "张" units for options)
- Standardizes date/time formats
- Replaces NaN values with empty strings for Google Sheets compatibility

## Security Notes

- The `credentials.json` file contains sensitive Google service account credentials
- Ensure this file is never committed to version control
- The Google Sheet must be shared with the service account email: `hermitongdashboard@tradingrecord.iam.gserviceaccount.com`