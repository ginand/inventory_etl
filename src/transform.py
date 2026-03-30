from __future__ import annotations

import re
import pandas as pd


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def clean_col(col: str) -> str:
        col = str(col).strip().lower()
        col = col.replace("%", "pct")
        col = col.replace("&", "and")
        col = col.replace("'", "")
        col = col.replace('"', "")
        col = re.sub(r"[\n\r\t]+", " ", col)
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = re.sub(r"_+", "_", col).strip("_")
        return col

    out.columns = [clean_col(c) for c in out.columns]
    return out


def _drop_fully_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.dropna(how="all")
    out = out[
        out.astype(str)
        .apply(lambda s: s.str.strip())
        .ne("")
        .any(axis=1)
    ]
    return out.reset_index(drop=True)


def _to_numeric_if_exists(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _to_datetime_if_exists(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def _strip_if_exists(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
    return out


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_column_names(df)
    out = _drop_fully_empty_rows(out)

    rename_map = {
        "id": "product_id",
        "name": "product_name",
        "description": "description",
        "starting_inventory": "starting_inventory",
        "re_order_point": "reorder_point",
        "unit": "unit",
        "category": "category",
        "taxable": "taxable",
        "pr_cust_fld": "product_custom_field",
        "inventory_on_hand": "inventory_on_hand",
        "inventory_to_come": "inventory_to_come",
        "inventory_to_go": "inventory_to_go",
        "to_order": "to_order_flag",
        "inventory_value": "inventory_value",
        "sales": "sales_qty",
        "sales_rank": "sales_rank",
    }
    out = out.rename(columns=rename_map)

    out = _strip_if_exists(
        out,
        [
            "product_id",
            "product_name",
            "description",
            "unit",
            "category",
            "taxable",
            "product_custom_field",
            "to_order_flag",
        ],
    )

    out = _to_numeric_if_exists(
        out,
        [
            "starting_inventory",
            "reorder_point",
            "inventory_on_hand",
            "inventory_to_come",
            "inventory_to_go",
            "inventory_value",
            "sales_qty",
            "sales_rank",
        ],
    )

    if "taxable" in out.columns:
        out["taxable"] = out["taxable"].astype(str).str.strip().str.upper()

    if "to_order_flag" in out.columns:
        out["to_order_flag"] = out["to_order_flag"].astype(str).str.strip().str.upper()

    return out.reset_index(drop=True)


def transform_prices(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_column_names(df)
    out = _drop_fully_empty_rows(out)

    rename_map = {
        "product_id": "product_id",
        "effective_from_date": "effective_from_date",
        "purchase_price": "purchase_price",
        "sales_price": "sales_price",
    }
    out = out.rename(columns=rename_map)

    out = _strip_if_exists(out, ["product_id"])
    out = _to_datetime_if_exists(out, ["effective_from_date"])
    out = _to_numeric_if_exists(out, ["purchase_price", "sales_price"])

    return out.reset_index(drop=True)


def transform_partners(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_column_names(df)
    out = _drop_fully_empty_rows(out)

    rename_map = {
        "id": "partner_id",
        "name": "partner_name",
        "shipping_address": "shipping_address",
        "billing_address": "billing_address",
        "email": "email",
        "phone": "phone",
        "contact": "contact_name",
        "pa_cust_fld": "partner_custom_field",
        "sales": "sales_amount",
        "sales_rank": "sales_rank",
        "purchases": "purchase_amount",
        "purchase_rank": "purchase_rank",
    }
    out = out.rename(columns=rename_map)

    out = _strip_if_exists(
        out,
        [
            "partner_id",
            "partner_name",
            "shipping_address",
            "billing_address",
            "email",
            "phone",
            "contact_name",
            "partner_custom_field",
        ],
    )

    out = _to_numeric_if_exists(
        out,
        ["sales_amount", "sales_rank", "purchase_amount", "purchase_rank"],
    )

    return out.reset_index(drop=True)


def transform_order_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_column_names(df)
    out = _drop_fully_empty_rows(out)

    rename_map = {
        "order_number": "order_number",
        "order_date": "order_date",
        "expected_date": "expected_date",
        "order_type": "order_type",
        "partner_name": "partner_name",
        "other_charges": "other_charges",
        "order_discount": "order_discount",
        "tax_rate": "tax_rate",
        "order_notes": "order_notes",
        "o_cust_fld": "order_custom_field",
        "total_order_amount": "total_order_amount",
    }
    out = out.rename(columns=rename_map)

    if "order_number" in out.columns:
        out["order_number"] = out["order_number"].astype(str).str.strip()
        out = out[out["order_number"].str.upper() != "ORDER NUMBER"].copy()

    out = _strip_if_exists(
        out,
        [
            "order_number",
            "order_type",
            "partner_name",
            "tax_rate",
            "order_notes",
            "order_custom_field",
        ],
    )

    out = _to_datetime_if_exists(out, ["order_date", "expected_date"])
    out = _to_numeric_if_exists(
        out,
        ["other_charges", "order_discount", "total_order_amount"],
    )

    if "order_type" in out.columns:
        out["order_type"] = out["order_type"].astype(str).str.strip().str.upper()

    if "tax_rate" in out.columns:
        out["tax_rate_raw"] = out["tax_rate"]
        out["tax_rate_pct"] = (
            out["tax_rate"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.strip()
        )
        out["tax_rate_pct"] = pd.to_numeric(out["tax_rate_pct"], errors="coerce")

    return out.reset_index(drop=True)


def transform_order_details(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_column_names(df)
    out = _drop_fully_empty_rows(out)

    rename_map = {
        "order_number": "order_number",
        "id_of_product_on_the_order": "product_id",
        "quantity": "quantity",
        "any_discount_for_each_unit_of_product": "unit_discount",
        "use_this_to_enter_any_information_you_need": "detail_custom_field",
        "unit_price_before_any_discounts": "unit_price_before_discount",
        "quantity_unit_price_unit_discount": "line_amount_before_tax",
        "total_tax_amount": "total_tax_amount",
        "amount_before_tax_tax_amount": "line_total_amount",
        "name_of_product": "product_name",
        "orders_expected_date": "expected_date",
        "order_date": "order_date",
        "order_type": "order_type",
        "partner_name_on_the_order": "partner_name",
        "row_number_of_product": "product_row_number",
        "row_number_in_prices_sheet": "price_check_row",
        "row_number_of_order": "order_row_number",
        "purchase_price_of_product": "purchase_price",
    }
    out = out.rename(columns=rename_map)

    if "order_number" in out.columns:
        out["order_number"] = out["order_number"].astype(str).str.strip()
        out = out[out["order_number"].str.upper() != "ORDER NUMBER"].copy()

    out = _strip_if_exists(
        out,
        [
            "order_number",
            "product_id",
            "detail_custom_field",
            "product_name",
            "order_type",
            "partner_name",
        ],
    )

    out = _to_numeric_if_exists(
        out,
        [
            "quantity",
            "unit_discount",
            "unit_price_before_discount",
            "line_amount_before_tax",
            "total_tax_amount",
            "line_total_amount",
            "product_row_number",
            "price_check_row",
            "order_row_number",
            "purchase_price",
        ],
    )

    out = _to_datetime_if_exists(out, ["expected_date", "order_date"])

    if "order_type" in out.columns:
        out["order_type"] = out["order_type"].astype(str).str.strip().str.upper()

    return out.reset_index(drop=True)