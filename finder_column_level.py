"""
Column Level Classifier
=======================
โปรแกรมหาคอลัมน์ที่เป็น Order Level และ Item Level

Order Level: คอลัมน์ที่มีค่าเหมือนกันทุกแถว (ข้อมูลระดับ Order)
Item Level: คอลัมน์ที่มีค่าต่างกันในแถวต่างๆ (ข้อมูลระดับ Item)

Workflow:
1. โหลดข้อมูลจาก Excel/CSV
2. (Optional) หา search key value ที่ซ้ำมากที่สุดเพื่อวิเคราะห์
3. แยกคอลัมน์เป็น Order level vs Item level
4. แสดงผลและส่งออกข้อมูล
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import json
from datetime import datetime
import sys
import io

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


# ========================================
# 📋 CONFIGURATION
# ========================================

# Data Source
FILE_PATH = r'D:\Documents\Downloads\AllLiteDetailOrder20251114173910365.xlsx'
SHEET_NAME = None  # None = sheet แรก, หรือใส่ชื่อ sheet / index
SAMPLE_ROWS = None  # None = ทั้งหมด, หรือใส่จำนวนแถว

# Analysis Settings
SEARCH_KEY = 'หมายเลขออเดอร์ภายใน'  # คอลัมน์ที่ต้องการวิเคราะห์
ANALYZE_MOST_DUPLICATED = True  # True = วิเคราะห์กลุ่มที่ซ้ำมากที่สุด, False = วิเคราะห์ทั้งหมด
PROTECTED_COLUMNS = []  # คอลัมน์ที่ต้องการยกเว้นจากการจัดกลุ่ม (จะแสดงแยกต่างหาก)

# Processing Options
DROP_FULL_DUPLICATES = True  # ลบแถวที่ซ้ำทั้งแถวก่อนวิเคราะห์

# Export Options
SAVE_JSON_REPORT = True  # บันทึกรายงานเป็น JSON
EXPORT_TO_EXCEL = True  # บันทึกผลลัพธ์เป็น Excel (แยก sheet: order_level, item_level)
SHOW_SAMPLE_VALUES = True  # แสดงตัวอย่างค่าของแต่ละคอลัมน์


# ========================================
# 🔧 DATA LOADING
# ========================================

def load_data(file_path: str, sheet_name: Optional[str] = None, sample_rows: Optional[int] = None) -> pd.DataFrame:
    """โหลดข้อมูลจาก Excel หรือ CSV"""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f'ไม่พบไฟล์: {file_path}')

    suffix = path.suffix.lower()
    if suffix in {'.xls', '.xlsx', '.xlsm', '.xlsb'}:
        sheet = sheet_name if sheet_name is not None else 0
        df = pd.read_excel(file_path, sheet_name=sheet)
    elif suffix == '.csv':
        df = pd.read_csv(file_path)
    else:
        raise ValueError('รองรับเฉพาะ Excel (.xlsx, .xls) และ CSV (.csv) เท่านั้น')

    if sample_rows:
        df = df.head(sample_rows)

    return df


# ========================================
# 🔍 CORE ANALYSIS FUNCTIONS
# ========================================

def find_most_duplicated_value(df: pd.DataFrame, search_key: str) -> Optional[Dict[str, Any]]:
    """
    หาค่าใน search_key ที่มีจำนวนแถวมากที่สุด (TOP 1)

    Returns:
        dict with {'value': ..., 'count': ..., 'row_indices': [...]}
        or None if all values appear only once
    """
    grouped = df.groupby(search_key, dropna=False).size()
    duplicates = grouped[grouped > 1]

    if len(duplicates) == 0:
        return None

    top_1 = duplicates.nlargest(1)
    top_value = top_1.index[0]
    top_count = int(top_1.iloc[0])

    if pd.isnull(top_value):
        mask = df[search_key].isnull()
    else:
        mask = df[search_key] == top_value

    row_indices = df[mask].index.tolist()

    return {
        'value': top_value,
        'count': top_count,
        'row_indices': row_indices
    }


def get_subset_by_value(df: pd.DataFrame, search_key: str, value: Any) -> pd.DataFrame:
    """ดึงแถวที่มี search_key = value"""
    if pd.isnull(value):
        mask = df[search_key].isnull()
    else:
        mask = df[search_key] == value
    return df[mask].copy()


def classify_columns(df_subset: pd.DataFrame, protected_columns: List[str] = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    แยกคอลัมน์เป็น Order level (ค่าเหมือนกัน) vs Item level (ค่าต่างกัน)

    Returns:
        {
            'order_level': [
                {'column': str, 'unique_count': int, 'sample_value': any, 'total_rows': int},
                ...
            ],
            'item_level': [
                {'column': str, 'unique_count': int, 'sample_values': list, 'total_rows': int},
                ...
            ],
            'protected': [
                {'column': str, 'unique_count': int, 'sample_values': list, 'total_rows': int},
                ...
            ]
        }
    """
    if protected_columns is None:
        protected_columns = []

    protected_set = set(protected_columns)
    order_level = []
    item_level = []
    protected = []

    total_rows = len(df_subset)

    for col in df_subset.columns:
        # นับจำนวน unique values (รวม NULL)
        unique_count = df_subset[col].nunique(dropna=False)

        # ข้อมูลพื้นฐาน
        col_info = {
            'column': col,
            'unique_count': unique_count,
            'total_rows': total_rows,
            'null_count': int(df_subset[col].isnull().sum()),
            'null_percentage': float(df_subset[col].isnull().sum() / total_rows * 100)
        }

        # Protected columns
        if col in protected_set:
            # เพิ่มตัวอย่างค่า
            sample_values = df_subset[col].dropna().head(5).tolist()
            col_info['sample_values'] = sample_values
            protected.append(col_info)
            continue

        # Order level vs Item level
        if unique_count <= 1:
            # Order level - ค่าเหมือนกันทั้งหมด
            sample_value = df_subset[col].iloc[0] if len(df_subset) > 0 else None
            col_info['sample_value'] = sample_value
            order_level.append(col_info)
        else:
            # Item level - ค่าต่างกัน
            sample_values = df_subset[col].dropna().unique()[:5].tolist()
            col_info['sample_values'] = sample_values
            col_info['coverage_percentage'] = float(unique_count / total_rows * 100)  # % ของ unique values
            item_level.append(col_info)

    return {
        'order_level': sorted(order_level, key=lambda x: x['column']),
        'item_level': sorted(item_level, key=lambda x: x['column']),
        'protected': sorted(protected, key=lambda x: x['column'])
    }


# ========================================
# 📊 REPORTING & OUTPUT
# ========================================

def print_header():
    """พิมพ์ header ของรายงาน"""
    print('\n' + '=' * 80)
    print('COLUMN LEVEL CLASSIFIER')
    print('=' * 80)
    print(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 80)


def print_classification_report(
    classification: Dict[str, List[Dict[str, Any]]],
    analysis_scope: str,
    total_rows: int,
    show_samples: bool = True
):
    """แสดงรายงานการจำแนกคอลัมน์"""

    print('\n' + '=' * 80)
    print(f'ANALYSIS SCOPE')
    print('=' * 80)
    print(f'Scope: {analysis_scope}')
    print(f'Total rows analyzed: {total_rows:,}')
    print(f'Total columns: {len(classification["order_level"]) + len(classification["item_level"]) + len(classification["protected"])}')

    # Protected Columns
    if classification['protected']:
        print('\n' + '=' * 80)
        print(f'🔒 PROTECTED COLUMNS: {len(classification["protected"])} columns')
        print('=' * 80)
        print('These columns are protected and shown separately')
        print('-' * 80)

        for col_info in classification['protected']:
            print(f'\n• {col_info["column"]}')
            print(f'  Unique values: {col_info["unique_count"]:,}')
            print(f'  NULL values: {col_info["null_count"]:,} ({col_info["null_percentage"]:.2f}%)')
            if show_samples and col_info.get('sample_values'):
                sample_str = ', '.join([str(v)[:50] for v in col_info['sample_values'][:3]])
                print(f'  Sample: {sample_str}{"..." if len(col_info["sample_values"]) > 3 else ""}')

    # Order Level Columns
    print('\n' + '=' * 80)
    print(f'📋 ORDER LEVEL COLUMNS: {len(classification["order_level"])} columns')
    print('=' * 80)
    print('These columns have the SAME value across all rows')
    print('(ข้อมูลระดับ Order - ค่าเหมือนกันทุกแถว)')
    print('-' * 80)

    if classification['order_level']:
        for col_info in classification['order_level']:
            print(f'\n• {col_info["column"]}')
            if show_samples:
                value_display = str(col_info['sample_value'])[:100]
                print(f'  Value: {value_display}')
            if col_info['null_count'] > 0:
                print(f'  NULL values: {col_info["null_count"]:,} ({col_info["null_percentage"]:.2f}%)')
    else:
        print('(none)')

    # Item Level Columns
    print('\n' + '=' * 80)
    print(f'📦 ITEM LEVEL COLUMNS: {len(classification["item_level"])} columns')
    print('=' * 80)
    print('These columns have DIFFERENT values across rows')
    print('(ข้อมูลระดับ Item - ค่าต่างกันในแต่ละแถว)')
    print('-' * 80)

    if classification['item_level']:
        for col_info in classification['item_level']:
            print(f'\n• {col_info["column"]}')
            print(f'  Unique values: {col_info["unique_count"]:,} ({col_info["coverage_percentage"]:.2f}% coverage)')
            print(f'  NULL values: {col_info["null_count"]:,} ({col_info["null_percentage"]:.2f}%)')
            if show_samples and col_info.get('sample_values'):
                sample_str = ', '.join([str(v)[:50] for v in col_info['sample_values'][:3]])
                print(f'  Sample: {sample_str}{"..." if len(col_info["sample_values"]) > 3 else ""}')
    else:
        print('(none)')

    # Summary
    print('\n' + '=' * 80)
    print('SUMMARY')
    print('=' * 80)
    print(f'Total columns: {len(classification["order_level"]) + len(classification["item_level"]) + len(classification["protected"])}')
    print(f'  • Protected columns: {len(classification["protected"])}')
    print(f'  • Order level: {len(classification["order_level"])} ({len(classification["order_level"]) / (len(classification["order_level"]) + len(classification["item_level"])) * 100:.1f}% of non-protected)')
    print(f'  • Item level: {len(classification["item_level"])} ({len(classification["item_level"]) / (len(classification["order_level"]) + len(classification["item_level"])) * 100:.1f}% of non-protected)')
    print('=' * 80 + '\n')


def save_json_report(
    output_path: str,
    classification: Dict[str, List[Dict[str, Any]]],
    metadata: Dict[str, Any]
):
    """บันทึกรายงานเป็น JSON"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    report = {
        'generated_at': datetime.now().isoformat(),
        'metadata': metadata,
        'results': {
            'protected_columns': classification['protected'],
            'order_level_columns': classification['order_level'],
            'item_level_columns': classification['item_level'],
            'summary': {
                'total_columns': len(classification['order_level']) + len(classification['item_level']) + len(classification['protected']),
                'protected_count': len(classification['protected']),
                'order_level_count': len(classification['order_level']),
                'item_level_count': len(classification['item_level'])
            }
        }
    }

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f'[SAVED] JSON report: {path}')


def export_to_excel_detailed(
    output_path: str,
    classification: Dict[str, List[Dict[str, Any]]],
    df_analyzed: pd.DataFrame
):
    """
    บันทึกผลลัพธ์เป็น Excel โดยแยกเป็น sheets:
    - Summary: สรุปผลการวิเคราะห์
    - OrderLevel: รายละเอียดคอลัมน์ Order Level
    - ItemLevel: รายละเอียดคอลัมน์ Item Level
    - Protected: รายละเอียดคอลัมน์ Protected
    - Data_OrderLevel: ข้อมูลจริงของคอลัมน์ Order Level
    - Data_ItemLevel: ข้อมูลจริงของคอลัมน์ Item Level
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = {
            'Category': ['Protected Columns', 'Order Level Columns', 'Item Level Columns', 'Total Columns'],
            'Count': [
                len(classification['protected']),
                len(classification['order_level']),
                len(classification['item_level']),
                len(classification['protected']) + len(classification['order_level']) + len(classification['item_level'])
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

        # Protected columns detail
        if classification['protected']:
            df_protected = pd.DataFrame(classification['protected'])
            df_protected.to_excel(writer, sheet_name='Protected', index=False)

        # Order level columns detail
        if classification['order_level']:
            df_order = pd.DataFrame(classification['order_level'])
            df_order.to_excel(writer, sheet_name='OrderLevel', index=False)

            # Data - Order level columns
            order_cols = [c['column'] for c in classification['order_level']]
            if order_cols:
                df_analyzed[order_cols].to_excel(writer, sheet_name='Data_OrderLevel', index=False)

        # Item level columns detail
        if classification['item_level']:
            df_item = pd.DataFrame(classification['item_level'])
            df_item.to_excel(writer, sheet_name='ItemLevel', index=False)

            # Data - Item level columns
            item_cols = [c['column'] for c in classification['item_level']]
            if item_cols:
                df_analyzed[item_cols].to_excel(writer, sheet_name='Data_ItemLevel', index=False)

    print(f'[SAVED] Excel report: {path}')


# ========================================
# 🚀 MAIN EXECUTION
# ========================================

def main():
    """Main execution function"""
    try:
        print_header()

        # Step 1: Load data
        print(f'\n[LOADING] Reading data from: {FILE_PATH}')
        df = load_data(FILE_PATH, sheet_name=SHEET_NAME, sample_rows=SAMPLE_ROWS)
        print(f'[OK] Loaded: {len(df):,} rows × {len(df.columns):,} columns')

        # Step 2: Remove full duplicate rows
        if DROP_FULL_DUPLICATES:
            print(f'\n[CLEANING] Checking for full duplicate rows...')
            original_count = len(df)
            df = df.drop_duplicates().reset_index(drop=True)
            duplicates_removed = original_count - len(df)

            if duplicates_removed > 0:
                print(f'[OK] Removed {duplicates_removed:,} full duplicate rows ({duplicates_removed/original_count*100:.2f}%)')
                print(f'[OK] Remaining: {len(df):,} rows')
            else:
                print(f'[OK] No full duplicate rows found')

        # Step 3: Determine analysis scope
        df_to_analyze = df
        analysis_scope = 'All data'
        search_value = None

        if ANALYZE_MOST_DUPLICATED and SEARCH_KEY:
            print(f'\n[ANALYZING] Searching for most duplicated {SEARCH_KEY}...')

            if SEARCH_KEY not in df.columns:
                print(f'[WARNING] SEARCH_KEY "{SEARCH_KEY}" not found in data')
                print(f'[INFO] Analyzing all data instead')
            else:
                top_search = find_most_duplicated_value(df, SEARCH_KEY)

                if top_search:
                    print(f'[OK] Found: {SEARCH_KEY} = {top_search["value"]} ({top_search["count"]:,} rows)')
                    df_to_analyze = get_subset_by_value(df, SEARCH_KEY, top_search['value'])
                    analysis_scope = f'{SEARCH_KEY} = {top_search["value"]}'
                    search_value = top_search['value']
                else:
                    print(f'[INFO] No duplicates found for {SEARCH_KEY}')
                    print(f'[INFO] Analyzing all data instead')

        print(f'\n[ANALYZING] Scope: {analysis_scope}')
        print(f'[ANALYZING] Rows to analyze: {len(df_to_analyze):,}')

        # Step 4: Classify columns
        print(f'\n[CLASSIFYING] Analyzing columns...')
        classification = classify_columns(df_to_analyze, PROTECTED_COLUMNS)
        print(f'[OK] Classification complete')
        print(f'     Protected: {len(classification["protected"])} columns')
        print(f'     Order level: {len(classification["order_level"])} columns')
        print(f'     Item level: {len(classification["item_level"])} columns')

        # Step 5: Print report
        print_classification_report(
            classification,
            analysis_scope,
            len(df_to_analyze),
            show_samples=SHOW_SAMPLE_VALUES
        )

        # Step 6: Export results
        base_path = Path(FILE_PATH).parent
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Metadata for export
        metadata = {
            'file_path': str(FILE_PATH),
            'sheet_name': SHEET_NAME,
            'total_rows_in_file': len(df),
            'analyzed_rows': len(df_to_analyze),
            'analysis_scope': analysis_scope,
            'search_key': SEARCH_KEY,
            'search_value': str(search_value) if search_value is not None else None,
            'protected_columns': PROTECTED_COLUMNS,
            'drop_full_duplicates': DROP_FULL_DUPLICATES
        }

        if SAVE_JSON_REPORT:
            json_filename = f'column_classification_{timestamp}.json'
            json_path = base_path / json_filename
            save_json_report(str(json_path), classification, metadata)

        if EXPORT_TO_EXCEL:
            excel_filename = f'column_classification_{timestamp}.xlsx'
            excel_path = base_path / excel_filename
            export_to_excel_detailed(str(excel_path), classification, df_to_analyze)

        print('\n[COMPLETE] Column classification finished!\n')

    except Exception as e:
        print(f'\n[ERROR] {str(e)}')
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
