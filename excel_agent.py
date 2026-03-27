import re
import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
from rapidfuzz import process, fuzz

from config import DATA_DIR, SUPPORTED_EXTENSIONS


def normalize_text(x: Any) -> str:
    if pd.isna(x):
        return ""
    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)
    return x


def normalize_colname(col: str) -> str:
    col = normalize_text(col)
    col = col.replace("\n", " ").replace("<br/>", " ")
    col = re.sub(r"\s+", " ", col)
    return col.strip()


def safe_numeric(series: pd.Series) -> pd.Series:
    if series.dtype.kind in "biufc":
        return pd.to_numeric(series, errors="coerce")
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("₹", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("_", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


class ExcelAIAgent:
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = Path(data_dir)
        self.files: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.catalog: pd.DataFrame = pd.DataFrame()
        self.load_all_files()

    def load_all_files(self):
        file_map = {}
        catalog_rows = []

        for file_path in self.data_dir.iterdir():
            if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                continue

            try:
                xl = pd.ExcelFile(file_path)
                sheets = {}
                for sheet_name in xl.sheet_names:
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
                        df.columns = [normalize_colname(c) for c in df.columns]
                        df = df.dropna(axis=1, how="all")
                        df = df.dropna(axis=0, how="all")
                        df = df.reset_index(drop=True)
                        sheets[sheet_name] = df

                        catalog_rows.append({
                            "file": file_path.name,
                            "sheet": sheet_name,
                            "rows": len(df),
                            "cols": len(df.columns),
                            "columns": list(df.columns),
                        })
                    except Exception as e:
                        sheets[sheet_name] = pd.DataFrame({"_error": [str(e)]})

                file_map[file_path.name] = sheets
            except Exception as e:
                file_map[file_path.name] = {"_file_error": pd.DataFrame({"_error": [str(e)]})}

        self.files = file_map
        self.catalog = pd.DataFrame(catalog_rows)

    def list_files(self) -> List[str]:
        return list(self.files.keys())

    def list_sheets(self, file_name: str) -> List[str]:
        return list(self.files.get(file_name, {}).keys())

    def get_sheet(self, file_name: str, sheet_name: str) -> pd.DataFrame:
        return self.files[file_name][sheet_name].copy()

    def all_columns(self) -> List[str]:
        cols = set()
        for _, row in self.catalog.iterrows():
            for c in row["columns"]:
                cols.add(c)
        return sorted(cols)

    def match_file(self, text: str) -> Optional[str]:
        files = self.list_files()
        if not files:
            return None
        match = process.extractOne(text, files, scorer=fuzz.WRatio)
        return match[0] if match and match[1] >= 55 else None

    def match_sheet(self, file_name: str, text: str) -> Optional[str]:
        sheets = self.list_sheets(file_name)
        if not sheets:
            return None
        match = process.extractOne(text, sheets, scorer=fuzz.WRatio)
        return match[0] if match and match[1] >= 50 else None

    def match_column(self, text: str, file_name: Optional[str] = None, sheet_name: Optional[str] = None) -> Optional[str]:
        if file_name and sheet_name:
            cols = list(self.get_sheet(file_name, sheet_name).columns)
        else:
            cols = self.all_columns()

        if not cols:
            return None

        match = process.extractOne(text, cols, scorer=fuzz.WRatio)
        return match[0] if match and match[1] >= 50 else None

    def search_rows(
        self,
        keyword: str,
        file_name: Optional[str] = None,
        sheet_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        case: bool = False,
    ) -> pd.DataFrame:
        results = []

        file_targets = [file_name] if file_name else self.list_files()

        for f in file_targets:
            sheet_targets = [sheet_name] if sheet_name else self.list_sheets(f)

            for s in sheet_targets:
                df = self.get_sheet(f, s)
                if df.empty:
                    continue

                target_cols = columns if columns else df.columns.tolist()
                target_cols = [c for c in target_cols if c in df.columns]

                if not target_cols:
                    continue

                mask = pd.Series(False, index=df.index)
                for col in target_cols:
                    series = df[col].astype(str)
                    if case:
                        m = series.str.contains(keyword, na=False, regex=False)
                    else:
                        m = series.str.lower().str.contains(str(keyword).lower(), na=False, regex=False)
                    mask = mask | m

                subset = df[mask].copy()
                if not subset.empty:
                    subset.insert(0, "_sheet", s)
                    subset.insert(0, "_file", f)
                    results.append(subset)

        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()

    def aggregate(
        self,
        file_name: str,
        sheet_name: str,
        column: str,
        operation: str = "sum",
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        df = self.get_sheet(file_name, sheet_name)
        df_filtered = self.apply_filters(df, filters)

        if column not in df_filtered.columns:
            raise ValueError(f"Column '{column}' not found in {file_name} -> {sheet_name}")

        numeric = safe_numeric(df_filtered[column])

        op = operation.lower()
        if op == "sum":
            value = numeric.sum(skipna=True)
        elif op == "avg" or op == "mean":
            value = numeric.mean(skipna=True)
        elif op == "min":
            value = numeric.min(skipna=True)
        elif op == "max":
            value = numeric.max(skipna=True)
        elif op == "count":
            value = numeric.count()
        elif op == "median":
            value = numeric.median(skipna=True)
        elif op == "std":
            value = numeric.std(skipna=True)
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        return {
            "file": file_name,
            "sheet": sheet_name,
            "column": column,
            "operation": op,
            "filters": filters or {},
            "result": None if pd.isna(value) else float(value) if isinstance(value, (np.floating, np.integer)) else value,
            "rows_considered": int(len(df_filtered)),
        }

    def percentage(
        self,
        file_name: str,
        sheet_name: str,
        numerator_col: str,
        denominator_col: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        df = self.get_sheet(file_name, sheet_name)
        df_filtered = self.apply_filters(df, filters)

        if numerator_col not in df_filtered.columns or denominator_col not in df_filtered.columns:
            raise ValueError("Numerator or denominator column not found.")

        num = safe_numeric(df_filtered[numerator_col]).sum(skipna=True)
        den = safe_numeric(df_filtered[denominator_col]).sum(skipna=True)

        pct = (num / den * 100.0) if den not in [0, None] and not pd.isna(den) else np.nan

        return {
            "file": file_name,
            "sheet": sheet_name,
            "numerator": numerator_col,
            "denominator": denominator_col,
            "result_percent": None if pd.isna(pct) else float(pct),
            "numerator_sum": float(num) if not pd.isna(num) else None,
            "denominator_sum": float(den) if not pd.isna(den) else None,
            "rows_considered": int(len(df_filtered)),
        }

    def group_aggregate(
        self,
        file_name: str,
        sheet_name: str,
        group_by: str,
        value_col: str,
        operation: str = "sum",
        filters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        df = self.get_sheet(file_name, sheet_name)
        df = self.apply_filters(df, filters)

        if group_by not in df.columns or value_col not in df.columns:
            raise ValueError("Group or value column not found.")

        df = df.copy()
        df[value_col] = safe_numeric(df[value_col])

        op = operation.lower()
        if op == "sum":
            out = df.groupby(group_by, dropna=False)[value_col].sum().reset_index()
        elif op in ["avg", "mean"]:
            out = df.groupby(group_by, dropna=False)[value_col].mean().reset_index()
        elif op == "min":
            out = df.groupby(group_by, dropna=False)[value_col].min().reset_index()
        elif op == "max":
            out = df.groupby(group_by, dropna=False)[value_col].max().reset_index()
        elif op == "count":
            out = df.groupby(group_by, dropna=False)[value_col].count().reset_index()
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        out.columns = [group_by, f"{operation}_{value_col}"]
        return out.sort_values(by=f"{operation}_{value_col}", ascending=False).reset_index(drop=True)

    def apply_filters(self, df: pd.DataFrame, filters: Optional[Dict[str, Any]]) -> pd.DataFrame:
        if not filters:
            return df.copy()

        result = df.copy()

        for col, val in filters.items():
            if col not in result.columns:
                continue

            if isinstance(val, dict):
                series_num = safe_numeric(result[col])
                if "gt" in val:
                    result = result[series_num > val["gt"]]
                if "gte" in val:
                    result = result[series_num >= val["gte"]]
                if "lt" in val:
                    result = result[series_num < val["lt"]]
                if "lte" in val:
                    result = result[series_num <= val["lte"]]
                if "eq" in val:
                    result = result[series_num == val["eq"]]
            else:
                result = result[result[col].astype(str).str.lower() == str(val).lower()]

        return result

    def parse_simple_query(self, query: str) -> Dict[str, Any]:
        q = query.strip()

        result = {
            "intent": None,
            "file": None,
            "sheet": None,
            "column": None,
            "operation": None,
            "filters": {},
            "group_by": None,
            "keyword": None,
        }

        q_lower = q.lower()

        # operation
        if "sum" in q_lower or "total" in q_lower:
            result["intent"] = "aggregate"
            result["operation"] = "sum"
        elif "average" in q_lower or "avg" in q_lower or "mean" in q_lower:
            result["intent"] = "aggregate"
            result["operation"] = "mean"
        elif "minimum" in q_lower or re.search(r"\bmin\b", q_lower):
            result["intent"] = "aggregate"
            result["operation"] = "min"
        elif "maximum" in q_lower or re.search(r"\bmax\b", q_lower):
            result["intent"] = "aggregate"
            result["operation"] = "max"
        elif "count" in q_lower:
            result["intent"] = "aggregate"
            result["operation"] = "count"
        elif "find" in q_lower or "search" in q_lower or "show rows" in q_lower:
            result["intent"] = "search"

        # file match
        file_match = self.match_file(q)
        if file_match:
            result["file"] = file_match

        # sheet match
        if result["file"]:
            sheet_match = self.match_sheet(result["file"], q)
            if sheet_match:
                result["sheet"] = sheet_match

        # common patterns
        m_col = re.search(r"(?:sum|total|avg|average|mean|min|max|count)\s+of\s+(.+?)(?:\s+where|\s+in\s+sheet|\s+in\s+file|$)", q, re.I)
        if m_col:
            col_text = m_col.group(1).strip()
            result["column"] = self.match_column(col_text, result["file"], result["sheet"]) or col_text

        m_group = re.search(r"group by\s+(.+?)(?:$|\s+where)", q, re.I)
        if m_group:
            grp_text = m_group.group(1).strip()
            result["group_by"] = self.match_column(grp_text, result["file"], result["sheet"]) or grp_text
            result["intent"] = "group_aggregate"

        m_where = re.search(r"where\s+(.+?)\s*=\s*(.+)$", q, re.I)
        if m_where:
            filter_col = m_where.group(1).strip()
            filter_val = m_where.group(2).strip().strip("'\"")
            matched_filter_col = self.match_column(filter_col, result["file"], result["sheet"]) or filter_col
            result["filters"][matched_filter_col] = filter_val

        if result["intent"] == "search":
            m_find = re.search(r"(?:find|search|show rows for|show)\s+(.+)", q, re.I)
            if m_find:
                result["keyword"] = m_find.group(1).strip()

        return result

    def run_query(self, query: str) -> Dict[str, Any]:
        parsed = self.parse_simple_query(query)

        if parsed["intent"] == "search":
            keyword = parsed["keyword"] or query
            data = self.search_rows(
                keyword=keyword,
                file_name=parsed["file"],
                sheet_name=parsed["sheet"],
            )
            return {
                "type": "table",
                "query": query,
                "parsed": parsed,
                "data": data,
                "message": f"Found {len(data)} matching rows."
            }

        if parsed["intent"] == "group_aggregate":
            if not parsed["file"] or not parsed["sheet"] or not parsed["group_by"] or not parsed["column"]:
                return {
                    "type": "error",
                    "query": query,
                    "parsed": parsed,
                    "message": "For group aggregate, provide file, sheet, group by column and value column."
                }

            data = self.group_aggregate(
                file_name=parsed["file"],
                sheet_name=parsed["sheet"],
                group_by=parsed["group_by"],
                value_col=parsed["column"],
                operation=parsed["operation"] or "sum",
                filters=parsed["filters"]
            )
            return {
                "type": "chart_table",
                "query": query,
                "parsed": parsed,
                "data": data,
                "message": "Grouped aggregation completed."
            }

        if parsed["intent"] == "aggregate":
            if not parsed["file"] or not parsed["sheet"] or not parsed["column"]:
                return {
                    "type": "error",
                    "query": query,
                    "parsed": parsed,
                    "message": "For aggregation, provide file, sheet and column."
                }

            data = self.aggregate(
                file_name=parsed["file"],
                sheet_name=parsed["sheet"],
                column=parsed["column"],
                operation=parsed["operation"] or "sum",
                filters=parsed["filters"]
            )
            return {
                "type": "metric",
                "query": query,
                "parsed": parsed,
                "data": data,
                "message": "Aggregation completed."
            }

        return {
            "type": "info",
            "query": query,
            "parsed": parsed,
            "message": "Could not fully parse query. Use manual controls or a more explicit query."
        }
