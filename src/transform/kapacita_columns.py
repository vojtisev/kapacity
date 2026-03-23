"""
Sjednocení sloupců kapacitní tabulky (různé šablony Excelu / CSV → společný model).
"""

from __future__ import annotations

import pandas as pd


# Priorita: první existující sloupec se přejmenuje na pobocka_cislo
_POBOCKA_CISLO_ALIASES: tuple[str, ...] = (
    "pobocka_cislo",
    "knoddel_cisloknih",
    "kapacita_cisloknih",
    "cislo_knihovny",
    "číslo_knihovny",
    "cisloknih",
    "cislo_knih",
    "cislo_pobocky",
    "c_knihovny",
    "cisloknihovny",
)

_LOKACE_ALIASES: tuple[str, ...] = (
    "lokace_short",
    "kapacita_lokace_short",
    "lokace",
    "kod_lokace",
    "lokace_kod",
    "short",
)

_KAP_ALIASES: tuple[str, ...] = (
    "kapacita_fyzicka",
    "kapacita_plan",
    "plan",
    "planovana_kapacita",
    "kapacita",
    "mist",
    "mista",
)

_STAV_ALIASES: tuple[str, ...] = (
    "stav_na_regalu",
    "kapacita_stav",
    "stav",
    "obsazeno",
    "pocet_svazku",
    "pocet",
    "svazky",
)

_OCH_ALIASES: tuple[str, ...] = (
    "och",
    "kapacita_och",
    "typ_fondu",
    "typ_och",
)


def _rename_first(df: pd.DataFrame, aliases: tuple[str, ...], target: str) -> pd.DataFrame:
    if target in df.columns:
        return df
    for a in aliases:
        if a in df.columns and a != target:
            return df.rename(columns={a: target})
    return df


def canonicalize_kapacita_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Přejmenuje známé aliasy na standardní názvy používané v ETL."""
    if df.empty:
        return df
    out = df.copy()
    out = _rename_first(out, _POBOCKA_CISLO_ALIASES, "pobocka_cislo")
    out = _rename_first(out, _LOKACE_ALIASES, "lokace_short")
    out = _rename_first(out, _KAP_ALIASES, "kapacita_fyzicka")
    out = _rename_first(out, _STAV_ALIASES, "stav_na_regalu")
    out = _rename_first(out, _OCH_ALIASES, "och")
    if "oblast" not in out.columns:
        for a in ("oblast", "kapacita_oblast", "region", "kraj"):
            if a in out.columns:
                out = out.rename(columns={a: "oblast"})
                break
    return out


def dopln_pobocka_cislo_z_katalogu(kap: pd.DataFrame, lok: pd.DataFrame) -> pd.DataFrame:
    """
    Pokud v kapacitě chybí číslo pobočky, doplní ho podle shody názvu s ``knoddel_nazev``
    ze seznamu lokací (stejná čísla knihoven jako v provozní databázi).
    """
    if kap.empty or "pobocka_nazev" not in kap.columns:
        return kap
    out = kap.copy()
    if "pobocka_cislo" not in out.columns:
        out["pobocka_cislo"] = pd.NA
    need = out["pobocka_cislo"].isna() | (out["pobocka_cislo"].astype(str) == "")
    if not need.any():
        return out
    sub = lok.dropna(subset=["knoddel_cisloknih", "knoddel_nazev"]).copy()
    sub["__k"] = sub["knoddel_nazev"].astype(str).str.strip().str.lower()
    d = sub.drop_duplicates(subset=["__k"], keep="first").set_index("__k")["knoddel_cisloknih"].to_dict()
    out.loc[need, "pobocka_cislo"] = out.loc[need, "pobocka_nazev"].map(
        lambda x: d.get(str(x).strip().lower(), pd.NA)
    )
    return out


def dopln_pobocka_cislo_v_ramci_listu(kap: pd.DataFrame) -> pd.DataFrame:
    """Uvnitř jednoho listu (pobocka_nazev) propaguje první nenulové číslo knihovny na všechny řádky."""
    if kap.empty or "pobocka_nazev" not in kap.columns or "pobocka_cislo" not in kap.columns:
        return kap
    out = kap.copy()
    g = out.groupby(out["pobocka_nazev"].astype(str), dropna=False)["pobocka_cislo"]
    out["pobocka_cislo"] = g.transform(lambda s: s.ffill().bfill())
    return out
