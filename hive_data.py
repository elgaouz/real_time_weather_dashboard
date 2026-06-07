"""Load weather rows from Hive (HiveServer2) with CSV fallback."""
from __future__ import annotations

import os
from glob import glob
from pathlib import Path
from typing import Any, List

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output" / "weather_detail"

HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "weather_db")
HIVE_TABLE = os.getenv("HIVE_TABLE", "weather_detail_ext")


def _load_from_csv(max_files: int = 300) -> pd.DataFrame:
    files = sorted(glob(str(OUTPUT_DIR / "part-*.csv")))
    if not files:
        return pd.DataFrame(
            columns=["CityName", "Temperature", "Humidity", "CreationTime", "CreationDate"]
        )
    dfs = []
    for file_path in files[-max_files:]:
        try:
            df = pd.read_csv(file_path, header=None)
            if df.shape[1] >= 5:
                df = df.iloc[:, :5]
                df.columns = ["CityName", "Temperature", "Humidity", "CreationTime", "CreationDate"]
                dfs.append(df)
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame(
            columns=["CityName", "Temperature", "Humidity", "CreationTime", "CreationDate"]
        )
    return pd.concat(dfs, ignore_index=True)


def _load_from_hive(limit: int = 500) -> pd.DataFrame:
    from pyhive import hive

    conn = hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DATABASE,
        auth="NOSASL",
    )
    sql = f"""
        SELECT CityName, Temperature, Humidity, CreationTime, CreationDate
        FROM {HIVE_TABLE}
        ORDER BY CreationTime DESC
        LIMIT {limit}
    """
    return pd.read_sql(sql, conn)


def load_weather_df(prefer_hive: bool = True, limit: int = 500) -> pd.DataFrame:
    if prefer_hive:
        try:
            return _load_from_hive(limit=limit)
        except Exception:
            pass
    return _load_from_csv()


def latest_per_city(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "CreationTime" in df.columns:
        df = df.sort_values("CreationTime")
    return df.groupby("CityName", as_index=False).tail(1).sort_values("CityName")


def to_records(df: pd.DataFrame) -> List[dict]:
    return df.to_dict(orient="records")
