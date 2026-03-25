"""
Načítání exportů Parquet z kořenové složky projektu (MKP — kapacity, stav fondu).
"""

from __future__ import annotations

from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd

from ..transform.normalize import normalize_lokace_short


def discover_parquet_files(root: Path) -> dict[str, Path]:
    """Najde známé parquet soubory podle klíčových slov v názvu (odolné vůči diakritice v cestě)."""
    out: dict[str, Path] = {}
    for p in sorted(root.glob("*.parquet")):
        n = p.name.lower()
        if "realok" in n:
            out["stav_realokace"] = p
        # NFD v názvech souborů: „přepo“ nemusí být spojité znaky — stačí „kapacity“
        elif "kapacity" in n:
            out["prepocet"] = p
        elif n.startswith("pobo") and "stavy" not in n:
            out["pobocky"] = p
        elif ("skuteč" in n or "skutec" in n) and "realok" not in n:
            out["stav_fond"] = p
        elif "sklady" in n:
            out["sklady"] = p
        elif "stavy" in n or "pobocek" in n:
            out["stavy_agreg"] = p
    return out


def load_pobocky_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df = df.rename(columns={"Pobočka č.": "pobocka_cislo", "Pobočka": "pobocka_nazev"})
    df["pobocka_cislo"] = pd.to_numeric(df["pobocka_cislo"], errors="coerce").astype("Int64")
    df["pobocka_nazev"] = df["pobocka_nazev"].astype(str).str.strip()
    df["pobocka_id"] = pd.to_numeric(df["pobocka_cislo"], errors="coerce").astype("Int64")
    return df


def oblast_z_prepocet(prepocet: pd.DataFrame) -> pd.Series:
    """Jedna oblast na číslo pobočky (modus)."""
    p = prepocet.copy()
    p["Pobočka č."] = pd.to_numeric(p["Pobočka č."], errors="coerce")

    def _pick(s: pd.Series) -> Union[float, str]:
        t = s.dropna()
        if t.empty:
            return np.nan
        m = t.mode()
        return m.iloc[0] if len(m) else t.iloc[0]

    return p.groupby("Pobočka č.")["Oblast"].agg(_pick)


def load_prepocet_kapacity(path: Path) -> pd.DataFrame:
    """Přepočítané kapacity — řádky regálů / segmentů, agregujeme na lokaci + typ (OCH)."""
    raw = pd.read_parquet(path)
    oz_col = "Označení" if "Označení" in raw.columns else ("Oznaceni" if "Oznaceni" in raw.columns else None)
    kap = pd.DataFrame(
        {
            "pobocka_cislo": pd.to_numeric(raw["Pobočka č."], errors="coerce").astype("Int64"),
            "pobocka_nazev": raw["Pobočka"].astype(str).str.strip(),
            "oblast": raw["Oblast"].astype(str).str.strip(),
            "lokace_short": raw["Lokace - systémové označení (Koniáš)"].astype(str).str.strip(),
            "och": raw["Typ"].astype(str).str.strip(),
            "typ": raw["Typ"].astype(str).str.strip(),
            "oznaceni": (
                raw[oz_col].astype(str).str.strip()
                if oz_col
                else pd.Series("", index=raw.index, dtype=object)
            ),
            "kapacita_fyzicka": pd.to_numeric(raw["Kapacita svazky"], errors="coerce"),
            "stav_na_regalu": pd.to_numeric(raw["Kapacita - současný stav"], errors="coerce"),
        }
    )
    kap.loc[kap["och"] == "", "och"] = pd.NA
    kap.loc[kap["typ"] == "", "typ"] = pd.NA
    if "oznaceni" in kap.columns:
        kap.loc[kap["oznaceni"] == "", "oznaceni"] = pd.NA
    kap["lokace_short_norm"] = kap["lokace_short"].map(normalize_lokace_short)
    return kap


def load_sklady_kapacity(path: Path) -> pd.DataFrame:
    """Skladové kapacity (Parquet) ve stejném schématu jako Přepočítané kapacity."""
    raw = pd.read_parquet(path)
    oz_col = "Označení" if "Označení" in raw.columns else ("Oznaceni" if "Oznaceni" in raw.columns else None)
    kap = pd.DataFrame(
        {
            "pobocka_cislo": pd.to_numeric(raw.get("Pobočka č."), errors="coerce").astype("Int64"),
            "pobocka_nazev": raw.get("Pobočka", pd.Series("", index=raw.index)).astype(str).str.strip(),
            "oblast": raw.get("Oblast", pd.Series("", index=raw.index)).astype(str).str.strip(),
            "lokace_short": raw.get(
                "Lokace - systémové označení (Koniáš)",
                pd.Series("", index=raw.index),
            )
            .astype(str)
            .str.strip(),
            "och": raw.get("Typ", pd.Series("", index=raw.index)).astype(str).str.strip(),
            "typ": raw.get("Typ", pd.Series("", index=raw.index)).astype(str).str.strip(),
            "oznaceni": (
                raw[oz_col].astype(str).str.strip()
                if oz_col
                else pd.Series("", index=raw.index, dtype=object)
            ),
            "kapacita_fyzicka": pd.to_numeric(raw.get("Kapacita svazky"), errors="coerce"),
            "stav_na_regalu": pd.to_numeric(raw.get("Kapacita - současný stav"), errors="coerce"),
        }
    )
    kap.loc[kap["och"] == "", "och"] = pd.NA
    kap.loc[kap["typ"] == "", "typ"] = pd.NA
    kap.loc[kap["oznaceni"] == "", "oznaceni"] = pd.NA
    kap.loc[kap["oblast"] == "", "oblast"] = pd.NA
    kap["lokace_short_norm"] = kap["lokace_short"].map(normalize_lokace_short)
    return kap


def load_lokace_skutecny_stav(path: Path) -> pd.DataFrame:
    """Skutečný stav fondu (lokace × svazky) — stejný význam jako dřívější SQL export."""
    df = pd.read_parquet(path)
    df = df.rename(
        columns={
            "LOKACE_KEY": "lokace_key",
            "KNODDEL_cisloknih": "knoddel_cisloknih",
            "KNODDEL_nazev": "knoddel_nazev",
            "LOKACE_SHORT": "lokace_short",
            "POČET (kj)": "stav_fondu",
            "Datum": "datum",
        }
    )
    df["lokace_key"] = pd.to_numeric(df["lokace_key"], errors="coerce").astype("Int64")
    df["knoddel_cisloknih"] = pd.to_numeric(df["knoddel_cisloknih"], errors="coerce")
    df["knoddel_nazev"] = df["knoddel_nazev"].astype(str).str.strip()
    df["lokace_short"] = df["lokace_short"].astype(str).str.strip()
    df["stav_fondu"] = pd.to_numeric(df["stav_fondu"], errors="coerce").fillna(0).astype(int)
    df["datum"] = df["datum"].astype(str)
    return df


def parquet_bundle_ready(root: Path) -> bool:
    d = discover_parquet_files(root)
    return "prepocet" in d and "pobocky" in d and "stav_fond" in d
