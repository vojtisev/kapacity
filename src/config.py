"""Cesty a výchozí názvy souborů pro ETL."""

from pathlib import Path
from typing import Optional, Tuple

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

# Piktogramy — podmnožina sloupce „Označení“ v přepočítaných kapacitách.
# Klíč = kód v datech (po normalizaci), hodnota = popisek žánru (jen pro UI).
PIKTOGRAMY: dict[str, str] = {
    "LEBKA": "horory",
    "PISTOLE": "detektivky",
    "MILENCI": "pro ženy",
    "ÚSMĚV": "humoristická",
    "ERB": "historická",
    "DÍVKA": "dívčí četba",
    "PEGAS": "báje / mýty",
    "PRINCEZNA": "pohádky",
    "KOVBOJ": "dobrodružné",
    "E.T.": "sci-fi",
    "MÁG": "fantasy",
    "KMET": "životopisná",
    "PÍSMÁK": "paměti",
    "LYRA": "hudební",
    "MASKA": "divadelní",
    "BRÝLE": "knihy s velkými písmeny",
}


def normalize_oznaceni(value: object) -> str:
    """
    Normalizace pro porovnání označení/piktogramů napříč exporty:
    - trim
    - sjednocení vnitřních mezer
    - ponechání diakritiky (whitelist je včetně diakritiky)
    """
    s = "" if value is None else str(value)
    s = " ".join(s.strip().split())
    return s


def extract_piktogram_code(value: object) -> Optional[str]:
    """
    Vrátí kód piktogramu, pokud jde o piktogramový zápis.

    Podporuje i varianty z realokačních exportů typu:
    - `MÁG (piktogram)`
    - `E.T.(piktogram)`
    """
    s = normalize_oznaceni(value)
    if not s:
        return None
    if s in PIKTOGRAMY:
        return s
    low = s.casefold()
    if "piktogram" in low:
        # vezmeme prefix před "(" a znovu normalizujeme
        prefix = s.split("(", 1)[0].strip()
        if prefix in PIKTOGRAMY:
            return prefix
    return None


def is_piktogram(value: object) -> bool:
    """True pokud normalizované označení je jedním z 16 piktogramů."""
    return extract_piktogram_code(value) is not None
