from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import pandas as pd


@dataclass
class ValidationResult:
    valid_df: pd.DataFrame
    error_df: pd.DataFrame


def _init_error_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["error_reason"] = ""
    return out


def _append_error(df: pd.DataFrame, mask: pd.Series, reason: str) -> pd.DataFrame:
    if mask.any():
        current = df.loc[mask, "error_reason"].astype(str)
        df.loc[mask, "error_reason"] = current.where(
            current == "",
            current + "|"
        )
        df.loc[mask, "error_reason"] = df.loc[mask, "error_reason"] + reason
    return df


def split_valid_error(df: pd.DataFrame) -> ValidationResult:
    error_mask = df["error_reason"].astype(str).str.strip() != ""
    valid_df = df.loc[~error_mask].copy()
    error_df = df.loc[error_mask].copy()
    return ValidationResult(valid_df=valid_df, error_df=error_df)


def check_required_columns(
    df: pd.DataFrame,
    required_cols: list[str],
    table_name: str,
) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{table_name} missing required columns: {missing}")


def validate_products(df: pd.DataFrame) -> ValidationResult:
    required_cols = [
        "product_id",
        "product_name",
    ]
    check_required_columns(df, required_cols, "stg_products")

    out = _init_error_frame(df)

    out["product_id"] = out["product_id"].astype(str).str.strip()
    out["product_name"] = out["product_name"].astype(str).str.strip()

    out = _append_error(out, out["product_id"].eq(""), "missing_product_id")
    out = _append_error(out, out["product_name"].eq(""), "missing_product_name")

    duplicate_product_id = out["product_id"].duplicated(keep=False) & out["product_id"].ne("")
    out = _append_error(out, duplicate_product_id, "duplicate_product_id")

    numeric_cols = [
        "starting_inventory",
        "reorder_point",
        "inventory_value",
        "sales_qty",
        "sales_rank",
    ]
    for col in numeric_cols:
        if col in out.columns:
            invalid_numeric = out[col].notna() & (pd.to_numeric(out[col], errors="coerce").isna())
            out = _append_error(out, invalid_numeric, f"invalid_numeric_{col}")

    non_negative_cols = [
        "starting_inventory",
        "reorder_point",
        "inventory_value",
        "sales_qty",
        "sales_rank",
    ]
    for col in non_negative_cols:
        if col in out.columns:
            series = pd.to_numeric(out[col], errors="coerce")
            negative_mask = series.notna() & (series < 0)
            out = _append_error(out, negative_mask, f"negative_{col}")

    return split_valid_error(out)


def validate_prices(df: pd.DataFrame, products_df: pd.DataFrame) -> ValidationResult:
    required_cols = [
        "product_id",
        "effective_from_date",
        "purchase_price",
        "sales_price",
    ]
    check_required_columns(df, required_cols, "stg_prices")

    out = _init_error_frame(df)
    out["product_id"] = out["product_id"].astype(str).str.strip()

    out = _append_error(out, out["product_id"].eq(""), "missing_product_id")
    out = _append_error(out, out["effective_from_date"].isna(), "invalid_effective_from_date")

    for col in ["purchase_price", "sales_price"]:
        series = pd.to_numeric(out[col], errors="coerce")
        out = _append_error(out, series.isna(), f"invalid_numeric_{col}")
        out = _append_error(out, series.notna() & (series < 0), f"negative_{col}")

    valid_product_ids = set(products_df["product_id"].dropna().astype(str).str.strip())
    invalid_fk = ~out["product_id"].isin(valid_product_ids)
    out = _append_error(out, invalid_fk, "product_id_not_found_in_products")

    return split_valid_error(out)


def validate_partners(df: pd.DataFrame) -> ValidationResult:
    required_cols = [
        "partner_id",
        "partner_name",
    ]
    check_required_columns(df, required_cols, "stg_partners")

    out = _init_error_frame(df)
    out["partner_id"] = out["partner_id"].astype(str).str.strip()
    out["partner_name"] = out["partner_name"].astype(str).str.strip()

    out = _append_error(out, out["partner_id"].eq(""), "missing_partner_id")
    out = _append_error(out, out["partner_name"].eq(""), "missing_partner_name")

    dup_partner_id = out["partner_id"].duplicated(keep=False) & out["partner_id"].ne("")
    out = _append_error(out, dup_partner_id, "duplicate_partner_id")

    return split_valid_error(out)


def validate_order_headers(df: pd.DataFrame, partners_df: pd.DataFrame) -> ValidationResult:
    required_cols = [
        "order_number",
        "order_date",
        "order_type",
        "partner_name",
    ]
    check_required_columns(df, required_cols, "stg_order_headers")

    out = _init_error_frame(df)
    out["order_number"] = out["order_number"].astype(str).str.strip()
    out["order_type"] = out["order_type"].astype(str).str.strip().str.upper()
    out["partner_name"] = out["partner_name"].astype(str).str.strip()

    out = _append_error(out, out["order_number"].eq(""), "missing_order_number")
    out = _append_error(out, out["order_date"].isna(), "invalid_order_date")
    out = _append_error(out, out["partner_name"].eq(""), "missing_partner_name")

    valid_order_types = {"PURCHASE", "SALE"}
    invalid_type = ~out["order_type"].isin(valid_order_types)
    out = _append_error(out, invalid_type, "invalid_order_type")

    dup_order_number = out["order_number"].duplicated(keep=False) & out["order_number"].ne("")
    out = _append_error(out, dup_order_number, "duplicate_order_number")

    partner_names = set(partners_df["partner_name"].dropna().astype(str).str.strip())
    invalid_partner = ~out["partner_name"].isin(partner_names)
    out = _append_error(out, invalid_partner, "partner_name_not_found_in_partners")

    numeric_cols = ["other_charges", "order_discount", "total_order_amount"]
    for col in numeric_cols:
        if col in out.columns:
            invalid_numeric = out[col].notna() & (pd.to_numeric(out[col], errors="coerce").isna())
            out = _append_error(out, invalid_numeric, f"invalid_numeric_{col}")

    return split_valid_error(out)


def validate_order_details(
    df: pd.DataFrame,
    products_df: pd.DataFrame,
    order_headers_df: pd.DataFrame,
) -> ValidationResult:
    required_cols = [
        "order_number",
        "product_id",
        "quantity",
    ]
    check_required_columns(df, required_cols, "stg_order_details")

    out = _init_error_frame(df)
    out["order_number"] = out["order_number"].astype(str).str.strip()
    out["product_id"] = out["product_id"].astype(str).str.strip()

    out = _append_error(out, out["order_number"].eq(""), "missing_order_number")
    out = _append_error(out, out["product_id"].eq(""), "missing_product_id")

    qty = pd.to_numeric(out["quantity"], errors="coerce")
    out = _append_error(out, qty.isna(), "invalid_quantity")
    out = _append_error(out, qty.notna() & (qty <= 0), "non_positive_quantity")

    for col in ["unit_discount", "purchase_price"]:
        if col in out.columns:
            s = pd.to_numeric(out[col], errors="coerce")
            invalid_mask = out[col].notna() & s.isna()
            out = _append_error(out, invalid_mask, f"invalid_numeric_{col}")
            out = _append_error(out, s.notna() & (s < 0), f"negative_{col}")

    valid_product_ids = set(products_df["product_id"].dropna().astype(str).str.strip())
    valid_order_numbers = set(order_headers_df["order_number"].dropna().astype(str).str.strip())

    out = _append_error(out, ~out["product_id"].isin(valid_product_ids), "product_id_not_found_in_products")
    out = _append_error(out, ~out["order_number"].isin(valid_order_numbers), "order_number_not_found_in_order_headers")

    return split_valid_error(out)


def build_error_report(results: Dict[str, ValidationResult]) -> pd.DataFrame:
    frames = []

    for table_name, result in results.items():
        if result.error_df.empty:
            continue

        err = result.error_df.copy()
        err.insert(0, "source_table", table_name)
        frames.append(err)

    if not frames:
        return pd.DataFrame(columns=["source_table", "error_reason"])

    return pd.concat(frames, ignore_index=True, sort=False)


def validate_all(
    stg_products: pd.DataFrame,
    stg_prices: pd.DataFrame,
    stg_partners: pd.DataFrame,
    stg_order_headers: pd.DataFrame,
    stg_order_details: pd.DataFrame,
) -> Tuple[Dict[str, ValidationResult], pd.DataFrame]:
    products_result = validate_products(stg_products)
    partners_result = validate_partners(stg_partners)

    prices_result = validate_prices(stg_prices, products_result.valid_df)
    order_headers_result = validate_order_headers(
        stg_order_headers,
        partners_result.valid_df,
    )
    order_details_result = validate_order_details(
        stg_order_details,
        products_result.valid_df,
        order_headers_result.valid_df,
    )

    results = {
        "stg_products": products_result,
        "stg_prices": prices_result,
        "stg_partners": partners_result,
        "stg_order_headers": order_headers_result,
        "stg_order_details": order_details_result,
    }

    error_report = build_error_report(results)
    return results, error_report