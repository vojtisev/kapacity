"""Načítání CSV zdrojů (lokace, fond, realokace, mapa oblastí)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd

from ..config import DATA_RAW, LOKACE_FOND, LOKACE_FOND_NEPRAZDNE, LOKACE_NAZEV, OBLAST_MAP, REALOKACE
from ..transform.normalize import normalize_columns


def _is_lokace_data_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    low = s.lower()
    if low.startswith("select ") or "===" in s or "constant" in low:
        return False
    if s.startswith("Records") or s.startswith("Affected") or s.startswith("eSQL"):
        return False
    return bool(re.match(r"^\d+\s*,", s))


def _parse_lokace_line(line: str) -> dict | None:
    """Parsuje jeden řádek SQL/text exportu lokace."""
    if not _is_lokace_data_line(line):
        return None
    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 6:
        return None
    lokace_key = parts[0].strip()
    kn_cislo = parts[1].strip()
    kn_nazev = parts[2].strip()
    lok_short = parts[3].strip()
    cnt = parts[4].strip().replace(" ", "")
    datum = parts[5].strip()
    try:
        lokace_key_i = int(lokace_key)
    except ValueError:
        return None
    try:
        count_i = int(cnt)
    except ValueError:
        count_i = 0
    cislo_knih: int | None
    if kn_cislo.lower() in ("<null>", "null", ""):
        cislo_knih = None
    else:
        try:
            cislo_knih = int(kn_cislo)
        except ValueError:
            cislo_knih = None
    if kn_nazev.lower() in ("<null>", "null"):
        kn_nazev = ""
    return {
        "lokace_key": lokace_key_i,
        "knoddel_cisloknih": cislo_knih,
        "knoddel_nazev": kn_nazev,
        "lokace_short": lok_short,
        "stav_fondu": count_i,
        "datum": datum,
    }


def load_lokace_master(path: Path | None = None) -> pd.DataFrame:
    """Seznam lokací + stav fondu (svazky) z exportu lokace-vsechny-nazev / fond CSV."""
    p = path or (DATA_RAW / LOKACE_NAZEV)
    if not p.exists():
        raise FileNotFoundError(f"Chybí soubor lokací: {p}")
    rows: list[dict] = []
    with p.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            rec = _parse_lokace_line(line)
            if rec:
                rows.append(rec)
    if not rows:
        raise ValueError(f"Žádná data lokací v {p}")
    return pd.DataFrame(rows)


def load_fond_state(path: Path | None = None) -> pd.DataFrame | None:
    """Alternativní soubor stavu fondu (lokace-vsechny.csv / neprazdne) — stejný formát jako master."""
    p = path or (DATA_RAW / LOKACE_FOND)
    if not p.exists():
        alt = DATA_RAW / LOKACE_FOND_NEPRAZDNE
        p = alt if alt.exists() else None
    if p is None or not p.exists():
        return None
    return load_lokace_master(p)


def load_realokace(path: Path | None = None) -> pd.DataFrame:
    """
    realokace.csv — sloupce (normalizované níže):
    lokace_short, pobocka_cislo (volitelné), pobocka_nazev (volitelné),
    och, kapacita_realokace
    """
    p = path or (DATA_RAW / REALOKACE)
    if not p.exists():
        return pd.DataFrame(
            columns=[
                "lokace_short",
                "pobocka_cislo",
                "pobocka_nazev",
                "och",
                "kapacita_realokace",
            ]
        )
    df = pd.read_csv(p, sep=None, engine="python", encoding="utf-8")
    return df


def load_oblast_map(path: Path | None = None) -> pd.DataFrame:
    """
    Tabulka poboček s přiřazením do oblasti (stejná logika jako vzdrojový sloupec „oblast“).

    Očekávané sloupce:
    - `pobocka_cislo` (int) — číslo knihovny, join na dim_pobocka
    - `oblast` — jeden z kanonických názvů (viz `config.OBLASTI_KANONICKE`)
    - volitelně `pobocka_nazev` — doplnění mapování podle názvu, pokud číslo v mapě chybí
    """
    p = path or (DATA_RAW / OBLAST_MAP)
    if not p.exists():
        return pd.DataFrame(columns=["pobocka_cislo", "oblast"])
    last_err: Optional[Exception] = None
    df = None
    for enc in ("utf-8-sig", "utf-8", "cp1250", "latin-1"):
        try:
            df = pd.read_csv(p, sep=None, engine="python", encoding=enc)
            break
        except UnicodeDecodeError as e:
            last_err = e
    if df is None:
        raise last_err or RuntimeError(str(p))
    df = normalize_columns(df)
    if "oblast" in df.columns:
        df["oblast"] = df["oblast"].apply(lambda x: str(x).strip() if pd.notna(x) else x)
    if "pobocka_cislo" in df.columns:
        df["pobocka_cislo"] = pd.to_numeric(df["pobocka_cislo"], errors="coerce").astype("Int64")
    if "pobocka_nazev" in df.columns:
        df["pobocka_nazev"] = df["pobocka_nazev"].apply(lambda x: str(x).strip() if pd.notna(x) else x)
    return df
