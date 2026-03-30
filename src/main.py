from __future__ import annotations

import os
import logging
from dotenv import load_dotenv

from extract import (
    read_sheet_by_header_keyword,
    save_checkpoint,   # 👈 thêm
)
from transform import (
    transform_products,
    transform_prices,
    transform_partners,
    transform_order_headers,
    transform_order_details,
)
from validate import validate_all
from load import load_dataframe_to_bq


logging.basicConfig(level=logging.INFO)


def main() -> None:
    load_dotenv()
    spreadsheet_id = os.environ["SPREADSHEET_ID"]

    # =========================
    # 📥 EXTRACT (INCREMENTAL)
    # =========================
    raw_products = read_sheet_by_header_keyword(spreadsheet_id, "PRODUCTS", "ID", incremental=True)
    raw_prices = read_sheet_by_header_keyword(spreadsheet_id, "PRICES", "PRODUCT ID", incremental=True)
    raw_partners = read_sheet_by_header_keyword(spreadsheet_id, "PARTNERS", "ID", incremental=True)
    raw_order_headers = read_sheet_by_header_keyword(spreadsheet_id, "ORDER_HEADERS", "ORDER NUMBER", incremental=True)
    raw_order_details = read_sheet_by_header_keyword(spreadsheet_id, "ORDER_DETAILS", "ORDER NUMBER", incremental=True)

    logging.info(f"Extracted: products={len(raw_products)}, prices={len(raw_prices)}, partners={len(raw_partners)}, orders={len(raw_order_headers)}, details={len(raw_order_details)}")

    # Nếu không có data mới → skip
    if all(df.empty for df in [
        raw_products, raw_prices, raw_partners, raw_order_headers, raw_order_details
    ]):
        logging.info("No new data. Skip pipeline.")
        return

    # =========================
    # 🔄 TRANSFORM
    # =========================
    stg_products = transform_products(raw_products)
    stg_prices = transform_prices(raw_prices)
    stg_partners = transform_partners(raw_partners)
    stg_order_headers = transform_order_headers(raw_order_headers)
    stg_order_details = transform_order_details(raw_order_details)

    # =========================
    # ✅ VALIDATE
    # =========================
    results, error_report = validate_all(
        stg_products=stg_products,
        stg_prices=stg_prices,
        stg_partners=stg_partners,
        stg_order_headers=stg_order_headers,
        stg_order_details=stg_order_details,
    )

    logging.info("Validation summary:")
    for table_name, result in results.items():
        logging.info(f"{table_name}: valid={len(result.valid_df)} | error={len(result.error_df)}")

    # =========================
    # 📦 LOAD
    # =========================
    valid_products = results["stg_products"].valid_df
    valid_prices = results["stg_prices"].valid_df
    valid_partners = results["stg_partners"].valid_df
    valid_order_headers = results["stg_order_headers"].valid_df
    valid_order_details = results["stg_order_details"].valid_df

    # load từng bảng
    if not valid_products.empty:
        load_dataframe_to_bq(valid_products, "stg_products")
        save_checkpoint("PRODUCTS", raw_products)

    if not valid_prices.empty:
        load_dataframe_to_bq(valid_prices, "stg_prices")
        save_checkpoint("PRICES", raw_prices)

    if not valid_partners.empty:
        load_dataframe_to_bq(valid_partners, "stg_partners")
        save_checkpoint("PARTNERS", raw_partners)

    if not valid_order_headers.empty:
        load_dataframe_to_bq(valid_order_headers, "stg_order_headers")
        save_checkpoint("ORDER_HEADERS", raw_order_headers)

    if not valid_order_details.empty:
        load_dataframe_to_bq(valid_order_details, "stg_order_details")
        save_checkpoint("ORDER_DETAILS", raw_order_details)

    # error report
    if not error_report.empty:
        load_dataframe_to_bq(error_report, "validation_error_report")


if __name__ == "__main__":
    main()