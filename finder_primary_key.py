"""
Primary Key Discovery Tool - Advanced Edition
==============================================
โปรแกรมค้นหา Primary Key แบบอัจฉริยะ โดยวิเคราะห์กลุ่มข้อมูลที่ซ้ำกันมากที่สุด

Workflow:
1. หา SEARCH_KEY value ที่ซ้ำกันมากที่สุด
2. แยกคอลัมน์เป็น Order level (ค่าเหมือนกัน) vs Item level (ค่าต่างกัน)
3. ตรวจสอบ BASE_KEY ว่าเป็น valid PK หรือไม่
4. หา minimal composite key โดยลดคอลัมน์ทีละตัวจนไม่สามารถลดได้
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import json
from datetime import datetime
import sys
import io
import hashlib

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

# Primary Key Discovery Settings
SEARCH_KEY = 'หมายเลขออเดอร์ภายใน'  # คอลัมน์ที่ต้องการวิเคราะห์
BASE_KEY = ['หมายเลขออเดอร์ภายใน', 'รหัสสินค้า']  # คอลัมน์พื้นฐานของ PK

# Processing Options
DROP_FULL_DUPLICATES = True  # ลบแถวที่ซ้ำทั้งแถวก่อนตรวจสอบ

# Export Options
SAVE_TEXT_REPORT = True  # บันทึกรายงานเป็น Text
SAVE_JSON_REPORT = True  # บันทึกรายงานเป็น JSON
EXPORT_DUPLICATES_TO_EXCEL = True  # บันทึกแถวที่ซ้ำเป็น Excel


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


def validate_columns_exist(df: pd.DataFrame, columns: List[str]) -> Tuple[bool, List[str]]:
    """ตรวจสอบว่าคอลัมน์ที่ระบุมีในข้อมูลหรือไม่"""
    missing = [col for col in columns if col not in df.columns]
    return len(missing) == 0, missing


# ========================================
# 🔍 CORE DISCOVERY FUNCTIONS
# ========================================

def calculate_row_hash(row: pd.Series) -> str:
    """
    คำนวณ SHA-256 hash ของแถว (ทุกคอลัมน์)

    Returns:
        SHA-256 hash string (hex)
    """
    # รวมค่าทุกคอลัมน์เป็น string เดียว
    row_string = '|'.join([str(val) for val in row.values])
    # คำนวณ SHA-256
    hash_object = hashlib.sha256(row_string.encode('utf-8'))
    return hash_object.hexdigest()


def analyze_row_hashes(df_subset: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
    """
    คำนวณและวิเคราะห์ SHA-256 hash ของแถวทั้งหมด

    Args:
        df_subset: DataFrame ที่จะวิเคราะห์
        columns: คอลัมน์ที่จะใช้คำนวณ hash

    Returns:
        {
            'total_rows': int,
            'unique_hashes': int,
            'duplicate_hash_count': int,
            'duplicate_hash_groups': list  # กลุ่มของแถวที่มี hash ซ้ำกัน
        }
    """
    # คำนวณ hash ของแต่ละแถว (เฉพาะคอลัมน์ที่ระบุ)
    df_hash = df_subset[columns].copy()
    df_hash['row_hash'] = df_hash.apply(calculate_row_hash, axis=1)

    # วิเคราะห์ hash ที่ซ้ำกัน
    total_rows = len(df_hash)
    unique_hashes = df_hash['row_hash'].nunique()
    duplicate_hash_count = total_rows - unique_hashes

    # หากลุ่มของแถวที่มี hash ซ้ำกัน
    duplicate_hash_groups = []
    if duplicate_hash_count > 0:
        hash_counts = df_hash.groupby('row_hash').size()
        duplicate_hashes = hash_counts[hash_counts > 1]

        for hash_value, count in duplicate_hashes.items():
            row_indices = df_subset[df_hash['row_hash'] == hash_value].index.tolist()
            duplicate_hash_groups.append({
                'hash': hash_value,
                'count': int(count),
                'row_indices': row_indices
            })

        # เรียงตาม count จากมากไปน้อย
        duplicate_hash_groups.sort(key=lambda x: -x['count'])

    return {
        'total_rows': total_rows,
        'unique_hashes': unique_hashes,
        'duplicate_hash_count': duplicate_hash_count,
        'duplicate_hash_groups': duplicate_hash_groups
    }


def find_most_duplicated_search_key(df: pd.DataFrame, search_key: str) -> Optional[Dict[str, Any]]:
    """
    หา SEARCH_KEY value ที่มีจำนวนแถวมากที่สุด (TOP 1)

    Returns:
        dict with {'value': ..., 'count': ..., 'row_indices': [...]}
        or None if all values appear only once
    """
    # Group by SEARCH_KEY และนับจำนวนแถว
    grouped = df.groupby(search_key, dropna=False).size()

    # กรองเฉพาะที่มีมากกว่า 1 แถว (ซ้ำ)
    duplicates = grouped[grouped > 1]

    if len(duplicates) == 0:
        return None

    # หา TOP 1 โดยใช้ nlargest
    top_1 = duplicates.nlargest(1)
    top_value = top_1.index[0]
    top_count = int(top_1.iloc[0])

    # หา row indices ของ value นี้
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


def get_subset_by_search_key_value(df: pd.DataFrame, search_key: str, value: Any) -> pd.DataFrame:
    """ดึงแถวที่มี SEARCH_KEY = value"""
    if pd.isnull(value):
        mask = df[search_key].isnull()
    else:
        mask = df[search_key] == value
    return df[mask].copy()


def classify_columns_by_variance(df_subset: pd.DataFrame, base_key: List[str]) -> Dict[str, List[str]]:
    """
    แยกคอลัมน์เป็น Order level (ค่าเหมือนกันทุกแถว) vs Item level (ค่าต่างกัน)

    Returns:
        {
            'order_level': [...],  # คอลัมน์ที่มีค่าเหมือนกันทั้งหมด (ตัดออก)
            'item_level': [...],   # คอลัมน์ที่มีค่าแตกต่างกัน (เก็บไว้)
            'base_key': [...]      # BASE_KEY columns (protected, ไม่ตัด)
        }
    """
    order_level = []
    item_level = []
    base_key_set = set(base_key)

    for col in df_subset.columns:
        # ข้าม BASE_KEY columns
        if col in base_key_set:
            continue

        # นับจำนวน unique values (รวม NULL)
        unique_count = df_subset[col].nunique(dropna=False)

        if unique_count <= 1:
            # ค่าเหมือนกันทั้งหมด → Order level
            order_level.append(col)
        else:
            # ค่าต่างกัน → Item level
            item_level.append(col)

    return {
        'order_level': sorted(order_level),
        'item_level': sorted(item_level),
        'base_key': base_key
    }


def validate_key_on_subset(df_subset: pd.DataFrame, key_columns: List[str]) -> Dict[str, Any]:
    """
    ตรวจสอบว่า key_columns เป็น valid PK บน df_subset หรือไม่

    Returns:
        {
            'is_valid': bool,
            'total_rows': int,
            'unique_count': int,
            'duplicate_count': int,
            'has_nulls': bool,
            'null_counts': dict,
            'duplicate_rows': list  # row indices ที่ซ้ำกัน
        }
    """
    total_rows = len(df_subset)

    # Check NULL values
    null_counts = {}
    total_nulls = 0
    for col in key_columns:
        null_count = int(df_subset[col].isnull().sum())
        if null_count > 0:
            null_counts[col] = null_count
            total_nulls += null_count

    has_nulls = total_nulls > 0

    # Check uniqueness และหา duplicate rows
    subset = df_subset[key_columns]
    unique_count = len(subset.drop_duplicates())
    duplicate_count = total_rows - unique_count

    # หา row indices ของแถวที่ซ้ำกัน
    duplicate_rows = []
    if duplicate_count > 0:
        is_duplicate = subset.duplicated(keep=False)
        duplicate_rows = df_subset[is_duplicate].index.tolist()

    is_valid = (duplicate_count == 0) and (not has_nulls)

    return {
        'is_valid': is_valid,
        'total_rows': total_rows,
        'unique_count': unique_count,
        'duplicate_count': duplicate_count,
        'has_nulls': has_nulls,
        'null_counts': null_counts,
        'duplicate_rows': duplicate_rows
    }


def find_minimal_pk(df_subset: pd.DataFrame, base_key: List[str], item_level_columns: List[str]) -> Dict[str, Any]:
    """
    หา minimal composite key โดยเริ่มจาก BASE_KEY + all item_level_columns
    แล้วค่อยๆ ลดคอลัมน์ออกทีละตัวจนไม่สามารถลดได้

    Returns:
        {
            'minimal_key': [...],
            'is_valid': bool,
            'added_columns': [...],  # คอลัมน์ที่เพิ่มจาก BASE_KEY
            'removed_columns': [...],  # คอลัมน์ที่ลบออกได้
            'iterations': [...]  # ประวัติการลด
        }
    """
    # เริ่มต้นด้วย BASE_KEY + all item_level_columns
    current_key = base_key + item_level_columns
    removed_columns = []
    iterations = []

    # ตรวจสอบว่า current_key เป็น valid PK หรือไม่
    validation = validate_key_on_subset(df_subset, current_key)
    iterations.append({
        'columns': current_key.copy(),
        'count': len(current_key),
        'is_valid': validation['is_valid'],
        'unique_count': validation['unique_count'],
        'duplicate_count': validation['duplicate_count']
    })

    if not validation['is_valid']:
        # DEBUG: แสดงรายละเอียด
        print(f'[DEBUG] Initial validation failed:')
        print(f'  Columns tested: {len(current_key)}')
        print(f'  Total rows: {validation["total_rows"]}')
        print(f'  Unique: {validation["unique_count"]}')
        print(f'  Duplicates: {validation["duplicate_count"]}')
        print(f'  Has NULLs: {validation["has_nulls"]}')

        if validation['has_nulls']:
            print(f'  NULL columns:')
            for col, count in validation['null_counts'].items():
                print(f'    - {col}: {count} NULLs')

            # ถ้ามี NULL แต่ unique แล้ว → ลองตัดคอลัมน์ที่มี NULL ออก
            if validation['duplicate_count'] == 0:
                print(f'[INFO] Unique combinations found, but some columns have NULLs')
                print(f'[INFO] Removing columns with NULLs and retrying...')

                # ตัดคอลัมน์ที่มี NULL ออก
                columns_with_nulls = set(validation['null_counts'].keys())
                current_key_no_null = [c for c in current_key if c not in columns_with_nulls]

                # ลองตรวจสอบอีกครั้ง
                validation_no_null = validate_key_on_subset(df_subset, current_key_no_null)
                if validation_no_null['is_valid']:
                    print(f'[OK] Found valid PK after removing NULL columns!')
                    print(f'     Columns: {len(current_key_no_null)} (removed {len(columns_with_nulls)} NULL columns)')

                    # ใช้ current_key ที่ไม่มี NULL แทน
                    current_key = current_key_no_null
                    validation = validation_no_null
                    iterations.append({
                        'action': f'Removed {len(columns_with_nulls)} columns with NULLs',
                        'columns': current_key.copy(),
                        'count': len(current_key),
                        'is_valid': True
                    })
                    # ดำเนินการต่อ (ไม่ return)
                else:
                    # ยังไม่ valid → return
                    return {
                        'minimal_key': current_key_no_null,
                        'is_valid': False,
                        'added_columns': [c for c in current_key_no_null if c in item_level_columns],
                        'removed_columns': list(columns_with_nulls),
                        'iterations': iterations,
                        'debug_info': {
                            'total_rows': validation_no_null['total_rows'],
                            'unique_count': validation_no_null['unique_count'],
                            'duplicate_count': validation_no_null['duplicate_count']
                        }
                    }
            else:
                # มี duplicates และ NULLs → ไม่สามารถหา PK ได้
                return {
                    'minimal_key': current_key,
                    'is_valid': False,
                    'added_columns': item_level_columns,
                    'removed_columns': [],
                    'iterations': iterations,
                    'debug_info': {
                        'total_rows': validation['total_rows'],
                        'unique_count': validation['unique_count'],
                        'duplicate_count': validation['duplicate_count']
                    }
                }
        else:
            # ไม่มี NULLs แต่มี duplicates → ไม่สามารถหา PK ได้
            return {
                'minimal_key': current_key,
                'is_valid': False,
                'added_columns': item_level_columns,
                'removed_columns': [],
                'iterations': iterations,
                'debug_info': {
                    'total_rows': validation['total_rows'],
                    'unique_count': validation['unique_count'],
                    'duplicate_count': validation['duplicate_count']
                }
            }

    # ลดคอลัมน์ออกทีละตัว (ยกเว้น BASE_KEY)
    improved = True
    while improved:
        improved = False

        # ลองลบแต่ละคอลัมน์ที่ไม่ใช่ BASE_KEY
        for col in current_key[:]:
            if col in base_key:
                continue  # ไม่ลด BASE_KEY columns

            # ลองลบคอลัมน์นี้
            temp_key = [c for c in current_key if c != col]
            temp_validation = validate_key_on_subset(df_subset, temp_key)

            if temp_validation['is_valid']:
                # ลบได้! ใช้ temp_key แทน
                current_key = temp_key
                removed_columns.append(col)
                iterations.append({
                    'action': f'Removed: {col}',
                    'columns': current_key.copy(),
                    'count': len(current_key),
                    'is_valid': True
                })
                improved = True
                break  # เริ่ม iteration ใหม่

    # คำนวณคอลัมน์ที่เหลือจาก item_level
    added_columns = [col for col in current_key if col in item_level_columns]

    return {
        'minimal_key': current_key,
        'is_valid': True,
        'added_columns': added_columns,
        'removed_columns': removed_columns,
        'iterations': iterations
    }


# ========================================
# 📊 REPORTING & OUTPUT
# ========================================

def print_header():
    """พิมพ์ header ของรายงาน"""
    print('\n' + '=' * 80)
    print('PRIMARY KEY DISCOVERY TOOL - ADVANCED EDITION')
    print('=' * 80)
    print(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 80)


def print_detailed_analysis_report(
    search_key: str,
    base_key: List[str],
    top_search: Dict[str, Any],
    classification: Dict[str, List[str]],
    hash_analysis: Dict[str, Any],
    base_validation: Dict[str, Any],
    minimal_pk_result: Optional[Dict[str, Any]]
):
    """แสดงรายงานการวิเคราะห์แบบละเอียด"""

    print('\n' + '=' * 80)
    print('[STEP 1] MOST DUPLICATED SEARCH_KEY')
    print('=' * 80)
    print(f'Search Key Column : {search_key}')
    print(f'Most Duplicated Value : {top_search["value"]}')
    print(f'Duplicate Count : {top_search["count"]:,} rows')
    print(f'Row Indices : {top_search["row_indices"][:10]}{"..." if len(top_search["row_indices"]) > 10 else ""}')

    print('\n' + '=' * 80)
    print(f'[STEP 2] COLUMN CLASSIFICATION (analyzing {top_search["count"]} rows)')
    print('=' * 80)

    # Order Level Columns
    print(f'\n📋 Order Level Columns (REMOVED): {len(classification["order_level"])} columns')
    print('   (These have the same value across all rows)')
    print('-' * 80)
    if classification['order_level']:
        for col in classification['order_level'][:10]:  # แสดง 10 อันแรก
            print(f'   • {col}')
        if len(classification['order_level']) > 10:
            print(f'   ... and {len(classification["order_level"]) - 10} more columns')
    else:
        print('   (none)')

    # Item Level Columns
    print(f'\n📦 Item Level Columns (KEPT): {len(classification["item_level"])} columns')
    print('   (These have different values across rows)')
    print('-' * 80)
    if classification['item_level']:
        for col in classification['item_level'][:10]:
            print(f'   • {col}')
        if len(classification['item_level']) > 10:
            print(f'   ... and {len(classification["item_level"]) - 10} more columns')
    else:
        print('   (none)')

    # Base Key Columns
    print(f'\n🔑 Base Key Columns (PROTECTED): {len(classification["base_key"])} columns')
    print('   (These are always included in the final key)')
    print('-' * 80)
    for col in classification['base_key']:
        print(f'   • {col}')

    # Step 2.5: SHA-256 Hash Analysis
    print('\n' + '=' * 80)
    print(f'[STEP 2.5] SHA-256 HASH ANALYSIS (BASE_KEY + item_level columns)')
    print('=' * 80)
    print(f'Total rows analyzed : {hash_analysis["total_rows"]:,}')
    print(f'Unique hashes (SHA-256) : {hash_analysis["unique_hashes"]:,}')
    print(f'Duplicate hashes : {hash_analysis["duplicate_hash_count"]:,}')

    if hash_analysis['duplicate_hash_groups']:
        print(f'\n🔍 Duplicate Hash Groups Found: {len(hash_analysis["duplicate_hash_groups"])}')
        print('-' * 80)
        for i, group in enumerate(hash_analysis['duplicate_hash_groups'][:5], 1):  # แสดง 5 กลุ่มแรก
            print(f'\n#{i}. Hash: {group["hash"][:16]}...{group["hash"][-16:]}')
            print(f'    Duplicate count: {group["count"]} rows')
            print(f'    Row indices: {group["row_indices"]}')

        if len(hash_analysis['duplicate_hash_groups']) > 5:
            print(f'\n   ... and {len(hash_analysis["duplicate_hash_groups"]) - 5} more duplicate hash groups')

        print(f'\n⚠️  These rows are TRULY IDENTICAL across all {len(classification["base_key"]) + len(classification["item_level"])} columns!')
        print(f'   This indicates actual data duplication that needs to be resolved.')
    else:
        print('\n✓ No duplicate hashes found - all rows are unique!')

    # Step 3: BASE_KEY Validation
    print('\n' + '=' * 80)
    print('[STEP 3] BASE_KEY VALIDATION')
    print('=' * 80)
    key_display = ' + '.join(base_key)
    print(f'Testing Key : {key_display}')
    print(f'Scope : {base_validation["total_rows"]:,} rows (SEARCH_KEY value = {top_search["value"]})')

    status = '✓ PASS' if base_validation['is_valid'] else '✗ FAIL'
    print(f'Result : {status}')
    print(f'  Unique combinations : {base_validation["unique_count"]:,}')
    print(f'  Duplicates : {base_validation["duplicate_count"]:,} rows')

    if base_validation['duplicate_rows']:
        print(f'  Duplicate row indices : {base_validation["duplicate_rows"][:10]}{"..." if len(base_validation["duplicate_rows"]) > 10 else ""}')

    if base_validation['has_nulls']:
        print(f'  NULL values : {sum(base_validation["null_counts"].values())}')
        for col, count in base_validation['null_counts'].items():
            print(f'    - {col}: {count} rows')

    # Step 4: Minimal PK Discovery
    if minimal_pk_result:
        print('\n' + '=' * 80)
        print('[STEP 4] MINIMAL PRIMARY KEY DISCOVERY')
        print('=' * 80)

        if minimal_pk_result['is_valid']:
            starting_count = len(base_key) + len(classification['item_level'])
            print(f'Starting with : BASE_KEY + all item_level columns ({starting_count} columns)')
            print(f'  → {" + ".join(base_key + classification["item_level"][:3])}{"..." if len(classification["item_level"]) > 3 else ""}')

            print(f'\nReducing columns iteratively:')
            for iteration in minimal_pk_result['iterations'][1:]:  # ข้าม iteration แรก
                print(f'  - {iteration["action"]} → {iteration["count"]} columns remain')

            print(f'\n✓ MINIMAL PK FOUND: {len(minimal_pk_result["minimal_key"])} columns')
            print('-' * 80)
            for col in minimal_pk_result['minimal_key']:
                source = '(BASE_KEY)' if col in base_key else '(item_level)'
                print(f'   • {col} {source}')

            added_count = len(minimal_pk_result['added_columns'])
            removed_count = len(minimal_pk_result['removed_columns'])
            print(f'\nSummary:')
            print(f'  • BASE_KEY columns : {len(base_key)}')
            print(f'  • Added from item_level : {added_count} columns')
            if added_count > 0:
                print(f'    → {", ".join(minimal_pk_result["added_columns"])}')
            print(f'  • Removed (redundant) : {removed_count} columns')
            if removed_count > 0:
                print(f'    → {", ".join(minimal_pk_result["removed_columns"])}')
        else:
            print('✗ UNABLE TO FIND VALID PK')
            print(f'Even with BASE_KEY + all {len(classification["item_level"])} item_level columns,')
            print('the key is still not valid. This may indicate data quality issues.')

    # Final Conclusion
    print('\n' + '=' * 80)
    if base_validation['is_valid']:
        print('✓ CONCLUSION: BASE_KEY is already a VALID Primary Key!')
        print(f'   No additional columns needed.')
    elif minimal_pk_result and minimal_pk_result['is_valid']:
        added = len(minimal_pk_result['added_columns'])
        print(f'✓ CONCLUSION: Found minimal Primary Key with {added} additional column(s)')
        print(f'   BASE_KEY + {", ".join(minimal_pk_result["added_columns"])}')
    else:
        print('✗ CONCLUSION: Cannot find a valid Primary Key')
        print('   Recommendations:')
        print('   • Review data quality for duplicates')
        print('   • Consider adding more identifying columns')
        print('   • Check for missing or inconsistent data')
    print('=' * 80 + '\n')


def save_detailed_json_report(
    output_path: str,
    search_key: str,
    base_key: List[str],
    top_search: Dict[str, Any],
    classification: Dict[str, List[str]],
    base_validation: Dict[str, Any],
    minimal_pk_result: Optional[Dict[str, Any]]
):
    """บันทึกรายงานแบบละเอียดเป็น JSON"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    report = {
        'generated_at': datetime.now().isoformat(),
        'search_key': {
            'column': search_key,
            'most_duplicated_value': str(top_search['value']),
            'count': top_search['count'],
            'row_indices': top_search['row_indices']
        },
        'column_classification': {
            'order_level': classification['order_level'],
            'item_level': classification['item_level'],
            'base_key': classification['base_key']
        },
        'base_key_validation': {
            'columns': base_key,
            'is_valid': base_validation['is_valid'],
            'total_rows': base_validation['total_rows'],
            'unique_count': base_validation['unique_count'],
            'duplicate_count': base_validation['duplicate_count'],
            'has_nulls': base_validation['has_nulls'],
            'null_counts': base_validation['null_counts']
        },
        'minimal_pk_discovery': minimal_pk_result if minimal_pk_result else None
    }

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f'[SAVED] JSON report: {path}')


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

        # Step 3: Validate SEARCH_KEY exists
        print(f'\n[VALIDATING] Checking configuration...')
        if SEARCH_KEY not in df.columns:
            print(f'[ERROR] SEARCH_KEY column "{SEARCH_KEY}" not found in data')
            print(f'Available columns: {list(df.columns)[:10]}...')
            return
        print(f'[OK] SEARCH_KEY column found: {SEARCH_KEY}')

        # Validate BASE_KEY exists
        columns_exist, missing = validate_columns_exist(df, BASE_KEY)
        if not columns_exist:
            print(f'[ERROR] Missing BASE_KEY columns: {missing}')
            return
        print(f'[OK] All {len(BASE_KEY)} BASE_KEY columns found')

        # Step 4: Find most duplicated SEARCH_KEY
        print(f'\n[ANALYZING] Finding most duplicated SEARCH_KEY value...')
        top_search = find_most_duplicated_search_key(df, SEARCH_KEY)

        if not top_search:
            print(f'[INFO] No duplicates found for SEARCH_KEY "{SEARCH_KEY}"')
            print('All values appear only once. No PK analysis needed.')
            return

        print(f'[OK] Found: {SEARCH_KEY} = {top_search["value"]} ({top_search["count"]:,} rows)')

        # Step 5: Get subset of most duplicated SEARCH_KEY
        df_subset = get_subset_by_search_key_value(df, SEARCH_KEY, top_search['value'])
        print(f'[OK] Extracted subset: {len(df_subset):,} rows')

        # Step 6: Classify columns
        print(f'\n[ANALYZING] Classifying columns by variance...')
        classification = classify_columns_by_variance(df_subset, BASE_KEY)
        print(f'[OK] Order level: {len(classification["order_level"])} columns')
        print(f'[OK] Item level: {len(classification["item_level"])} columns')
        print(f'[OK] Base key: {len(classification["base_key"])} columns')

        # Step 7: SHA-256 Hash Analysis
        print(f'\n[ANALYZING] Computing SHA-256 hashes...')
        all_columns = classification['base_key'] + classification['item_level']
        hash_analysis = analyze_row_hashes(df_subset, all_columns)
        print(f'[OK] Hash analysis complete')
        print(f'     Total rows: {hash_analysis["total_rows"]}')
        print(f'     Unique hashes: {hash_analysis["unique_hashes"]}')
        print(f'     Duplicate hashes: {hash_analysis["duplicate_hash_count"]}')

        # Step 8: Validate BASE_KEY
        print(f'\n[VALIDATING] Testing BASE_KEY...')
        base_validation = validate_key_on_subset(df_subset, BASE_KEY)
        if base_validation['is_valid']:
            print(f'[OK] BASE_KEY is a valid PK (no additional columns needed)')
        else:
            print(f'[INFO] BASE_KEY is not valid ({base_validation["duplicate_count"]} duplicates)')

            # แสดงรายละเอียดของแถวที่ซ้ำกัน
            if base_validation['duplicate_rows']:
                print(f'\n[DEBUG] Analyzing duplicate rows...')
                dup_indices = base_validation['duplicate_rows']
                df_dup = df_subset.loc[dup_indices]

                # แสดงข้อมูลของแถวที่ซ้ำกัน (BASE_KEY columns)
                print(f'Duplicate rows detail (BASE_KEY columns):')
                for idx, row in df_dup[BASE_KEY].iterrows():
                    values = ', '.join([f'{col}={row[col]}' for col in BASE_KEY])
                    print(f'  Row {idx}: {values}')

                # เช็คว่าแถวไหนซ้ำกันจริงๆ
                print(f'\nChecking for exact duplicates in BASE_KEY:')
                dup_groups = df_dup[BASE_KEY].duplicated(keep=False)
                print(f'  Total duplicate rows: {dup_groups.sum()}')
                if dup_groups.sum() > 0:
                    # แสดงกลุ่มที่ซ้ำกัน
                    for key_values, group in df_dup[BASE_KEY].groupby(BASE_KEY, dropna=False):
                        if len(group) > 1:
                            print(f'  → Duplicate group ({len(group)} rows): {key_values}')
                            print(f'     Row indices: {group.index.tolist()}')

                            # แสดงข้อมูล item_level columns ของแถวที่ซ้ำกัน
                            if classification['item_level']:
                                print(f'\n     Comparing item_level columns:')
                                for item_col in classification['item_level'][:5]:  # แสดง 5 คอลัมน์แรก
                                    values = df_subset.loc[group.index, item_col].tolist()
                                    values_str = ', '.join([str(v) for v in values])
                                    all_same = len(set(str(v) for v in values)) == 1
                                    status = '(SAME)' if all_same else '(DIFFERENT)'
                                    print(f'       {item_col}: [{values_str}] {status}')

                                # เช็คว่าซ้ำกันทุกคอลัมน์หรือไม่
                                all_columns = classification['base_key'] + classification['item_level']
                                exact_dup = df_subset.loc[group.index, all_columns].duplicated(keep=False)
                                if exact_dup.all():
                                    print(f'\n     ⚠️  WARNING: These rows are IDENTICAL across ALL columns!')
                                    print(f'     This indicates a data quality issue (true duplicates).')
                                else:
                                    print(f'\n     ℹ️  INFO: Rows differ in some item_level columns.')

        # Step 8: Find minimal PK (if BASE_KEY is not valid)
        minimal_pk_result = None
        if not base_validation['is_valid'] and classification['item_level']:
            print(f'\n[DISCOVERING] Finding minimal primary key...')
            minimal_pk_result = find_minimal_pk(df_subset, BASE_KEY, classification['item_level'])

            if minimal_pk_result['is_valid']:
                print(f'[OK] Found minimal PK with {len(minimal_pk_result["minimal_key"])} columns')
            else:
                print(f'[WARNING] Cannot find valid PK even with all available columns')

        # Step 9: Print detailed report
        print_detailed_analysis_report(
            SEARCH_KEY, BASE_KEY, top_search, classification,
            hash_analysis, base_validation, minimal_pk_result
        )

        # Step 10: Export reports
        base_path = Path(FILE_PATH).parent
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if SAVE_JSON_REPORT:
            json_path = base_path / f'pk_discovery_{timestamp}.json'
            save_detailed_json_report(
                str(json_path), SEARCH_KEY, BASE_KEY, top_search,
                classification, base_validation, minimal_pk_result
            )

        # Export subset to Excel (if requested)
        if EXPORT_DUPLICATES_TO_EXCEL:
            print(f'\n[EXPORTING] Saving analyzed subset to Excel...')

            # ส่งออกเฉพาะคอลัมน์ที่เป็นผลลัพธ์: BASE_KEY + item_level (ไม่รวม order_level)
            export_columns = classification['base_key'] + classification['item_level']
            df_export = df_subset[export_columns]

            excel_path = base_path / f'pk_subset_{SEARCH_KEY}_{top_search["value"]}_{timestamp}.xlsx'
            df_export.to_excel(str(excel_path), index=False)

            print(f'[SAVED] Subset: {excel_path}')
            print(f'        Rows: {len(df_export):,}')
            print(f'        Columns: {len(df_export.columns):,} (BASE_KEY: {len(classification["base_key"])}, Item level: {len(classification["item_level"])})')

        print('\n[COMPLETE] Primary key discovery finished!\n')

    except Exception as e:
        print(f'\n[ERROR] {str(e)}')
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
