"""Cesty a výchozí názvy souborů pro ETL."""

from pathlib import Path
from typing import Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data_raw"
DATA_PROCESSED = PROJECT_ROOT / "data_processed"
EXPORTS = DATA_PROCESSED / "exports"
DQ_REPORT = DATA_PROCESSED / "data_quality_report.md"

LOKACE_NAZEV = "lokace-vsechny-nazev.csv"
LOKACE_FOND = "lokace-vsechny.csv"
LOKACE_FOND_NEPRAZDNE = "lokace-neprazdne.csv"
REALOKACE = "realokace.csv"
KAPACITA_CSV = "kapacita.csv"
# Primární Excel se sběrem kapacity + oblastí poboček (název dle projektu MKP)
KAPACITA_XLSX_PRIMARY = "Kapacity - návrh pro sběr dat.xlsx"
# Záložní název, pokud primární soubor v data_raw není
KAPACITA_XLSX = "kapacita.xlsx"
OBLAST_MAP = "oblast_map.csv"
# Mapování lokace z přepočtu na kód ve Skutečném stavu (např. 92.1 → JEN-PVP), viz README
LOKACE_MAP_PREPOCET = "lokace_map_prepocet.csv"

# Kanonické názvy oblastí v síti (sloupec `oblast` v mapě poboček → `oblast_map.csv`)
OBLASTI_KANONICKE: Tuple[str, ...] = (
    "Ústřední knihovna",
    "Jih",
    "Sklad",
    "Jihozápad",
    "Jihovýchod",
    "Středozápad",
    "Severovýchod",
)
