"""Excel (více listů = pobočky) a CSV s kapacitou regálů."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd


def normalize_kapacita_dataframe(df: pd.DataFrame, pobocka_nazev: str = "") -> pd.DataFrame:
    """Veřejný wrapper — stejné mapování sloupců jako u načtení Excelu/CSV kapacity."""
    return _norm_kapacita_columns(df, pobocka_nazev)


def _norm_kapacita_columns(df: pd.DataFrame, pobocka_nazev: str) -> pd.DataFrame:
    """Sjednotí názvy sloupců na malá písmena a podtržítka."""
    m = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
    out = df.rename(columns=m)
    out["pobocka_nazev"] = pobocka_nazev
    # očekávané aliasy
    col_map = {
        "kapacita_cisloknih": "pobocka_cislo",
        "číslo_knihovny": "pobocka_cislo",
        "cislo_knihovny": "pobocka_cislo",
        "knoddel_cisloknih": "pobocka_cislo",
        "kapacita_lokace_short": "lokace_short",
        "kapacita_och": "och",
        "kapacita_plan": "kapacita_fyzicka",
        "kapacita_stav": "stav_na_regalu",
        "kapacita_oblast": "oblast",
        "označení": "oznaceni",
    }
    for old, new in col_map.items():
        if old in out.columns and new not in out.columns:
            out = out.rename(columns={old: new})
    return out


def load_kapacita_fyzicka_from_excel(path: Path) -> pd.DataFrame:
    """
    Načte všechny listy z Excelu, každý list = jedna pobočka (název listu = pobocka_nazev).
    """
    xl = pd.ExcelFile(path, engine="openpyxl")
    frames: list[pd.DataFrame] = []
    for sheet in xl.sheet_names:
        raw = pd.read_excel(xl, sheet_name=sheet)
        frames.append(_norm_kapacita_columns(raw, sheet.strip()))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _vyber_jednu_oblast(series: pd.Series) -> Union[str, float]:
    """Jedna hodnota oblasti na pobočku — při konfliktu dominantní modus."""
    s = series.dropna()
    s = s[s.astype(str).str.strip() != ""]
    if s.empty:
        return np.nan
    modes = s.astype(str).str.strip().mode()
    if len(modes):
        return str(modes.iloc[0])
    return str(s.iloc[0])


def extract_oblast_z_kapacity(kap: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    Z řádků kapacity (Excel: každý list = pobočka, sloupec oblast) odvodí oblast pro pobočku.

    Vrací:
    - tabulku ``pobocka_cislo`` + ``oblast`` (agregace podle čísla knihovny),
    - tabulku ``pobocka_nazev`` + ``oblast`` pro řádky bez čísla, ale s názvem (list),
    - seznam textových varování při rozporu hodnot oblasti v jedné pobočce.
    """
    empty_c = pd.DataFrame(columns=["pobocka_cislo", "oblast"])
    empty_n = pd.DataFrame(columns=["pobocka_nazev", "oblast"])
    if kap.empty or "oblast" not in kap.columns:
        return empty_c, empty_n, []

    k = kap.copy()
    k["oblast"] = k["oblast"].apply(
        lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != "" else pd.NA
    )
    k = k[k["oblast"].notna()]
    if k.empty:
        return empty_c, empty_n, []

    warnings: list[str] = []

    def check_conflict(g: pd.Series, label: str) -> None:
        u = g.dropna().astype(str).str.strip().unique()
        u = [x for x in u if x]
        if len(u) > 1:
            warnings.append(f"{label}: více různých oblastí v datech kapacity — {u}, použit modus.")

    rows_c: list[dict] = []
    if "pobocka_cislo" in k.columns:
        for cislo, g in k.dropna(subset=["pobocka_cislo"]).groupby("pobocka_cislo"):
            check_conflict(g["oblast"], f"pobocka_cislo={cislo}")
            o = _vyber_jednu_oblast(g["oblast"])
            if pd.notna(o):
                rows_c.append({"pobocka_cislo": cislo, "oblast": o})

    rows_n: list[dict] = []
    if "pobocka_nazev" in k.columns:
        if "pobocka_cislo" in k.columns:
            kn = k[k["pobocka_cislo"].isna()]
        else:
            kn = k
        kn = kn[kn["pobocka_nazev"].notna() & (kn["pobocka_nazev"].astype(str).str.strip() != "")]
        for naz, g in kn.groupby("pobocka_nazev"):
            nn = str(naz).strip()
            check_conflict(g["oblast"], f"pobocka_nazev={nn!r}")
            o = _vyber_jednu_oblast(g["oblast"])
            if pd.notna(o):
                rows_n.append({"pobocka_nazev": nn, "oblast": o})

    return (
        pd.DataFrame(rows_c) if rows_c else empty_c,
        pd.DataFrame(rows_n) if rows_n else empty_n,
        warnings,
    )


def load_kapacita_fyzicka_from_csv(path: Path) -> pd.DataFrame:
    """Jeden CSV soubor se stejnou strukturou jako listy Excelu (jako export kapacita.csv)."""
    last_err: Optional[Exception] = None
    df = None
    for enc in ("utf-8-sig", "utf-8", "cp1250", "latin-1"):
        try:
            df = pd.read_csv(path, sep=";", encoding=enc, dtype=str)
            break
        except UnicodeDecodeError as e:
            last_err = e
    if df is None:
        raise last_err or RuntimeError(path)
    # jeden soubor může obsahovat více poboček přes cislo knih — odvodíme název z mapy nebo prázdný
    df = _norm_kapacita_columns(df, "")
    if "pobocka_cislo" in df.columns:
        df["pobocka_cislo"] = pd.to_numeric(df["pobocka_cislo"], errors="coerce").astype("Int64")
    for c in ("kapacita_fyzicka", "stav_na_regalu"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df
