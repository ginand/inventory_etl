import os
from dotenv import load_dotenv

from extract import read_sheet_raw, _normalize_table

# 👇 nếu muốn test normalize thì bật thêm
from transform import normalize_column_names


def debug_sheet(spreadsheet_id: str, sheet_name: str, header_first_cell: str):
    print(f"\n===== DEBUG SHEET: {sheet_name} =====")

    range_name = f"{sheet_name}!A:ZZ"
    values = read_sheet_raw(spreadsheet_id, range_name)

    if not values:
        print("❌ No data found")
        return

    # =========================
    # 🔍 Detect header
    # =========================
    header_idx = None
    for i, row in enumerate(values):
        row_upper = [str(c).strip().upper() for c in row]

        if header_first_cell.strip().upper() in row_upper:
            header_idx = i
            print(f"✅ Header found at row index: {i}")
            print("HEADER ROW:", row)
            break

    if header_idx is None:
        print("❌ Header not found")
        return

    # =========================
    # 📊 Normalize table
    # =========================
    df = _normalize_table(values, header_idx)

    print("\n📌 RAW COLUMNS:")
    print(df.columns.tolist())

    # =========================
    # 🔍 Check updated_at BEFORE normalize
    # =========================
    print("\n🔍 Check BEFORE normalize:")
    print("updated_at in columns:", "updated_at" in df.columns)

    # =========================
    # 🔄 Apply normalize_column_names
    # =========================
    df_norm = normalize_column_names(df)

    print("\n📌 NORMALIZED COLUMNS:")
    print(df_norm.columns.tolist())

    # =========================
    # 🔍 Check AFTER normalize
    # =========================
    print("\n🔍 Check AFTER normalize:")
    print("updated_at in columns:", "updated_at" in df_norm.columns)

    # =========================
    # 👀 Preview data
    # =========================
    print("\n📊 SAMPLE DATA:")
    print(df_norm.head(5))


def main():
    load_dotenv()
    spreadsheet_id = os.environ["SPREADSHEET_ID"]

    debug_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="ORDER_DETAILS",
        header_first_cell="ORDER NUMBER",
    )


if __name__ == "__main__":
    main()