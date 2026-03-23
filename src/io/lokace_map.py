"""
Mapování kódů lokace mezi zdroji (např. přepočet „92.1“ vs. skutečný stav „JEN-PVP“).

Soubor `data_raw/lokace_map_prepocet.csv` se nemění při aktualizaci Parquetů — doplníte jen řádky,
kde se kódy neshodují.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..transform.normalize import normalize_lokace_short


def load_lokace_map_prepocet(path: Path | None) -> pd.DataFrame:
    """Načte mapu; prázdný rámec, pokud soubor chybí."""
    if path is None or not path.exists():
        return pd.DataFrame(columns=["pobocka_cislo", "lokace_short_zdroj", "lokace_short_cil"])
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    need = {"pobocka_cislo", "lokace_short_zdroj", "lokace_short_cil"}
    if not need.issubset(set(df.columns)):
        return pd.DataFrame(columns=["pobocka_cislo", "lokace_short_zdroj", "lokace_short_cil"])
    df = df.dropna(how="all")
    if df.empty:
        return pd.DataFrame(columns=["pobocka_cislo", "lokace_short_zdroj", "lokace_short_cil"])
    df["pobocka_cislo"] = pd.to_numeric(df["pobocka_cislo"], errors="coerce").astype("Int64")
    df["lokace_short_zdroj"] = df["lokace_short_zdroj"].astype(str).str.strip()
    df["lokace_short_cil"] = df["lokace_short_cil"].astype(str).str.strip()
    df = df.dropna(subset=["pobocka_cislo"])
    df = df[df["lokace_short_zdroj"] != ""]
    df = df[df["lokace_short_cil"] != ""]
    return df.drop_duplicates(subset=["pobocka_cislo", "lokace_short_zdroj"], keep="last")


def apply_prepocet_lokace_map(kap: pd.DataFrame, m: pd.DataFrame) -> pd.DataFrame:
    """
    Přepíše `lokace_short` a `lokace_short_norm` tam, kde (pobocka_cislo, norm(zdroj)) odpovídá mapě.

    Sloučení s dimenzí lokací ze Skutečného stavu pak proběhne na `lokace_short_cil`.
    """
    if kap.empty or m.empty:
        return kap
    out = kap.copy()
    if "lokace_short_norm" not in out.columns and "lokace_short" in out.columns:
        out["lokace_short_norm"] = out["lokace_short"].map(normalize_lokace_short)
    m = m.copy()
    m["__src"] = m["lokace_short_zdroj"].map(normalize_lokace_short)
    m["__dst"] = m["lokace_short_cil"].map(normalize_lokace_short)
    m = m.dropna(subset=["__src", "__dst"])
    m = m[m["__src"] != ""]
    m = m[m["__dst"] != ""]
    if m.empty:
        return out
    key = m.rename(columns={"__src": "lokace_short_norm"})[["pobocka_cislo", "lokace_short_norm", "__dst"]]
    merged = out.merge(key, on=["pobocka_cislo", "lokace_short_norm"], how="left")
    hit = merged["__dst"].notna()
    merged.loc[hit, "lokace_short"] = merged.loc[hit, "__dst"]
    merged.loc[hit, "lokace_short_norm"] = merged.loc[hit, "__dst"]
    return merged.drop(columns=["__dst"])
