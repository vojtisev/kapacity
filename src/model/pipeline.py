"""Sestavení dimenzí, faktů a analytických pohledů v DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from ..config import (
    DATA_PROCESSED,
    DATA_RAW,
    DQ_REPORT,
    EXPORTS,
    KAPACITA_CSV,
    KAPACITA_XLSX,
    KAPACITA_XLSX_PRIMARY,
    LOKACE_MAP_PREPOCET,
    LOKACE_NAZEV,
    OBLAST_MAP,
)
from ..io.excel import (
    extract_oblast_z_kapacity,
    load_kapacita_fyzicka_from_csv,
    load_kapacita_fyzicka_from_excel,
    normalize_kapacita_dataframe,
)
from ..io.loaders import load_lokace_master, load_oblast_map, load_realokace
from ..io.lokace_map import apply_prepocet_lokace_map, load_lokace_map_prepocet
from ..io.parquet_sources import (
    discover_parquet_files,
    load_lokace_skutecny_stav,
    load_pobocky_parquet,
    load_prepocet_kapacity,
    load_sklady_kapacity,
    oblast_z_prepocet,
    parquet_bundle_ready,
)
from ..transform.kapacita_columns import (
    canonicalize_kapacita_columns,
    dopln_pobocka_cislo_v_ramci_listu,
    dopln_pobocka_cislo_z_katalogu,
)
from ..transform.normalize import normalize_columns, normalize_lokace_short
from ..validation.quality import build_quality_report, write_quality_report


def _ensure_dirs() -> None:
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    EXPORTS.mkdir(parents=True, exist_ok=True)


def _resolve_kapacita_excel(data_raw: Path) -> Path | None:
    """Primárně „Kapacity - návrh pro sběr dat.xlsx“, jinak záložní ``kapacita.xlsx``."""
    for name in (KAPACITA_XLSX_PRIMARY, KAPACITA_XLSX):
        p = data_raw / name
        if p.exists():
            return p
    return None


def _load_kapacita(data_raw: Path, path_override: Path | None = None) -> pd.DataFrame:
    if path_override and path_override.exists():
        if path_override.suffix.lower() in (".xlsx", ".xls"):
            return load_kapacita_fyzicka_from_excel(path_override)
        return load_kapacita_fyzicka_from_csv(path_override)
    xlsx = _resolve_kapacita_excel(data_raw)
    if xlsx is not None:
        return load_kapacita_fyzicka_from_excel(xlsx)
    return load_kapacita_fyzicka_from_csv(data_raw / KAPACITA_CSV)


def _prepare_realokace(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "lokace_short_norm",
                "pobocka_cislo",
                "pobocka_nazev",
                "och",
                "kapacita_realokace",
            ]
        )
    r = normalize_columns(df)
    if "lokace_short" not in r.columns and "lokace" in r.columns:
        r = r.rename(columns={"lokace": "lokace_short"})
    r["lokace_short_norm"] = r["lokace_short"].map(normalize_lokace_short) if "lokace_short" in r.columns else ""
    if "pobocka_cislo" in r.columns:
        r["pobocka_cislo"] = pd.to_numeric(r["pobocka_cislo"], errors="coerce").astype("Int64")
    else:
        r["pobocka_cislo"] = pd.Series(pd.NA, index=r.index, dtype="Int64")
    if "pobocka_nazev" not in r.columns:
        r["pobocka_nazev"] = ""
    if "och" not in r.columns:
        r["och"] = ""
    r["och"] = r["och"].fillna("").astype(str).str.strip()
    r.loc[r["och"] == "", "och"] = np.nan
    if "kapacita_realokace" in r.columns:
        r["kapacita_realokace"] = pd.to_numeric(r["kapacita_realokace"], errors="coerce")
    else:
        r["kapacita_realokace"] = np.nan
    return r


def _pobocka_lokace_keys(df: pd.DataFrame, pob_col: str, loc_col: str) -> set[tuple[int, str]]:
    """Unikátní páry (číslo knihovny, normalizovaný LOKACE_SHORT) pro porovnání zdrojů."""
    if df.empty or pob_col not in df.columns or loc_col not in df.columns:
        return set()
    sub = df.dropna(subset=[pob_col, loc_col])
    sub = sub[sub[loc_col].astype(str).str.strip() != ""]
    out: set[tuple[int, str]] = set()
    for pc, loc in zip(
        pd.to_numeric(sub[pob_col], errors="coerce"),
        sub[loc_col].astype(str).str.strip(),
    ):
        if pd.isna(pc):
            continue
        out.add((int(pc), loc))
    return out


def _build_lookup_prepocet_dims(kap: pd.DataFrame, loc_map: pd.DataFrame) -> pd.DataFrame:
    """Řádky (lokace_id, oznaceni, typ) z Přepočítaných kapacit — pro filtry ostatních lokací."""
    need = ("pobocka_cislo", "lokace_short_norm", "oznaceni", "typ")
    if kap.empty or not all(c in kap.columns for c in need):
        return pd.DataFrame(columns=["lokace_id", "oznaceni", "typ"])
    d = kap[list(need)].drop_duplicates()
    d["oznaceni"] = d["oznaceni"].fillna("").astype(str).str.strip()
    d["typ"] = d["typ"].fillna("").astype(str).str.strip()
    d = d.merge(loc_map, on=["pobocka_cislo", "lokace_short_norm"], how="inner")
    d = d.dropna(subset=["lokace_id"])
    out = d[["lokace_id", "oznaceni", "typ"]].drop_duplicates()
    out["lokace_id"] = pd.to_numeric(out["lokace_id"], errors="coerce")
    return out.dropna(subset=["lokace_id"])


def _build_lookup_realok_dims(kr: pd.DataFrame | None, loc_map: pd.DataFrame) -> pd.DataFrame:
    """Řádky (lokace_id, kapacita_deskriptor, kapacita_och) ze Skutečný stav - realokace — pro filtry realokačních lokací."""
    if kr is None or kr.empty or "kapacita_deskriptor" not in kr.columns:
        return pd.DataFrame(columns=["lokace_id", "kapacita_deskriptor", "kapacita_och"])
    d = kr[["pobocka_cislo", "lokace_short_norm", "kapacita_deskriptor", "och"]].copy()
    d = d.rename(columns={"och": "kapacita_och"})
    d["kapacita_deskriptor"] = d["kapacita_deskriptor"].fillna("").astype(str).str.strip()
    d["kapacita_och"] = d["kapacita_och"].fillna("").astype(str).str.strip()
    d = d.merge(loc_map, on=["pobocka_cislo", "lokace_short_norm"], how="inner")
    d = d.dropna(subset=["lokace_id"])
    out = d[["lokace_id", "kapacita_deskriptor", "kapacita_och"]].drop_duplicates()
    out["lokace_id"] = pd.to_numeric(out["lokace_id"], errors="coerce")
    return out.dropna(subset=["lokace_id"])


def _register_duckdb_model(
    pb: pd.DataFrame,
    dim_lok: pd.DataFrame,
    fact_fyz: pd.DataFrame,
    real_g: pd.DataFrame,
    real: pd.DataFrame,
    lok: pd.DataFrame,
    kap: pd.DataFrame,
    oblast_kap_warnings: list[str],
    lookup_prepocet_dims: pd.DataFrame | None = None,
    lookup_realok_dims: pd.DataFrame | None = None,
) -> tuple[duckdb.DuckDBPyConnection, dict]:
    """Registrace tabulek a pohledů v DuckDB (společné pro Parquet i legacy ETL)."""
    fact_fond_total = dim_lok[["lokace_id", "datum", "stav_fondu_celkem_zdroj"]].copy()
    fact_fond_total["OCH"] = None
    fact_fond_total = fact_fond_total.rename(columns={"stav_fondu_celkem_zdroj": "stav_fondu"})

    fact_fond_och = fact_fyz[["lokace_id", "OCH", "stav_na_regalu"]].copy()
    fact_fond_och = fact_fond_och.rename(columns={"stav_na_regalu": "stav_fondu"})
    fact_fond_och["datum"] = fact_fond_total["datum"].iloc[0] if len(fact_fond_total) else ""

    con = duckdb.connect(database=":memory:")
    con.register("dim_pobocka", pb)
    con.register("dim_lokace", dim_lok)
    con.register("fact_kapacita_fyzicka", fact_fyz)
    con.register("fact_kapacita_realokace", real_g)
    con.register("fact_fond_total", fact_fond_total)
    con.register("fact_fond_och", fact_fond_och)
    con.register("lokace_master", lok)
    con.register("kapacita_raw", kap)
    lp = (
        lookup_prepocet_dims
        if lookup_prepocet_dims is not None
        else pd.DataFrame(columns=["lokace_id", "oznaceni", "typ"])
    )
    lr = (
        lookup_realok_dims
        if lookup_realok_dims is not None
        else pd.DataFrame(columns=["lokace_id", "kapacita_deskriptor", "kapacita_och"])
    )
    con.register("lookup_prepocet_dims", lp)
    con.register("lookup_realok_dims", lr)

    con.execute(
        """
        CREATE OR REPLACE VIEW fact_fond AS
        SELECT lokace_id, datum, CAST(OCH AS VARCHAR) AS OCH, stav_fondu
        FROM fact_fond_total
        UNION ALL
        SELECT lokace_id, datum, CAST(OCH AS VARCHAR) AS OCH, stav_fondu
        FROM fact_fond_och
        WHERE OCH IS NOT NULL
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW fact_effective_capacity AS
        SELECT
            f.lokace_id,
            f.pobocka_cislo,
            f.lokace_short_norm,
            CAST(f.OCH AS VARCHAR) AS OCH,
            f.kapacita_fyzicka,
            r.kapacita_realokace,
            l.je_realokace,
            CASE
                WHEN p.oblast = 'Sklad' THEN f.kapacita_fyzicka
                WHEN l.je_realokace THEN r.kapacita_realokace
                ELSE f.kapacita_fyzicka
            END AS kapacita_effective
        FROM fact_kapacita_fyzicka f
        LEFT JOIN dim_lokace l ON f.lokace_id = l.lokace_id
        LEFT JOIN dim_pobocka p ON l.pobocka_cislo = p.pobocka_cislo
        LEFT JOIN fact_kapacita_realokace r
            ON f.lokace_id = r.lokace_id
            AND (f.OCH IS NOT DISTINCT FROM r.OCH)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW fact_effective_capacity_full AS
        SELECT * FROM fact_effective_capacity
        UNION ALL
        SELECT
            r.lokace_id,
            r.pobocka_cislo,
            r.lokace_short_norm,
            CAST(r.OCH AS VARCHAR),
            NULL::DOUBLE AS kapacita_fyzicka,
            r.kapacita_realokace,
            TRUE AS je_realokace,
            r.kapacita_realokace AS kapacita_effective
        FROM fact_kapacita_realokace r
        INNER JOIN dim_lokace l ON r.lokace_id = l.lokace_id AND l.je_realokace
            WHERE NOT EXISTS (
            SELECT 1 FROM fact_kapacita_fyzicka f
            WHERE f.lokace_id = r.lokace_id
            AND (f.OCH IS NOT DISTINCT FROM r.OCH)
        )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW metrics_lokace_och AS
        SELECT
            l.lokace_id,
            l.lokace_short,
            l.knoddel_nazev AS pobocka_nazev,
            l.pobocka_cislo,
            p.oblast,
            l.je_realokace,
            CAST(e.OCH AS VARCHAR) AS OCH,
            e.kapacita_fyzicka,
            e.kapacita_realokace,
            e.kapacita_effective,
            fo.stav_fondu AS stav_fondu_och
        FROM fact_effective_capacity_full e
        INNER JOIN dim_lokace l ON e.lokace_id = l.lokace_id
        LEFT JOIN dim_pobocka p ON l.pobocka_cislo = p.pobocka_cislo
        LEFT JOIN fact_fond_och fo
            ON e.lokace_id = fo.lokace_id
            AND (e.OCH IS NOT DISTINCT FROM fo.OCH)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW metrics_lokace AS
        SELECT
            l.lokace_id,
            l.lokace_short,
            l.knoddel_nazev AS pobocka_nazev,
            l.pobocka_cislo,
            p.oblast,
            l.je_realokace,
            l.stav_fondu_celkem_zdroj AS stav_fondu_celkem,
            SUM(e.kapacita_effective) AS kapacita_celkem,
            SUM(e.kapacita_fyzicka) AS kapacita_fyzicka_sum,
            SUM(e.kapacita_realokace) AS kapacita_realokace_sum
        FROM dim_lokace l
        LEFT JOIN dim_pobocka p ON l.pobocka_cislo = p.pobocka_cislo
        LEFT JOIN fact_effective_capacity_full e ON l.lokace_id = e.lokace_id
        GROUP BY 1, 2, 3, 4, 5, 6, 7
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW metrics_lokace_enriched AS
        SELECT
            m.*,
            (m.kapacita_celkem - m.stav_fondu_celkem) AS volna_kapacita,
            CASE
                WHEN m.kapacita_celkem IS NULL OR m.kapacita_celkem = 0 THEN NULL
                ELSE (m.stav_fondu_celkem * 1.0 / m.kapacita_celkem) * 100.0
            END AS naplnenost_pct,
            (m.stav_fondu_celkem - m.kapacita_celkem) AS rozdil,
            CASE WHEN m.kapacita_celkem IS NULL OR m.kapacita_celkem = 0 THEN NULL
                 WHEN (m.stav_fondu_celkem * 1.0 / m.kapacita_celkem) > 1.0 THEN TRUE
                 ELSE FALSE
            END AS pretizena,
            CASE WHEN m.kapacita_celkem IS NULL OR m.kapacita_celkem = 0 THEN NULL
                 WHEN (m.stav_fondu_celkem * 1.0 / m.kapacita_celkem) > 0.9 THEN TRUE
                 ELSE FALSE
            END AS rizikova
        FROM metrics_lokace m
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW metrics_oblast AS
        SELECT
            oblast,
            COUNT(*) AS pocet_lokaci,
            COUNT(kapacita_celkem) AS pocet_lokaci_s_kapacitou,
            SUM(stav_fondu_celkem) AS stav_fondu_celkem,
            SUM(kapacita_celkem) AS kapacita_celkem,
            SUM(CASE WHEN kapacita_celkem IS NOT NULL THEN stav_fondu_celkem END) AS stav_pri_pokryti_kapacitou,
            CASE
                WHEN SUM(kapacita_celkem) IS NULL OR SUM(kapacita_celkem) = 0 THEN NULL
                ELSE (
                    SUM(CASE WHEN kapacita_celkem IS NOT NULL THEN stav_fondu_celkem END) * 100.0
                    / SUM(kapacita_celkem)
                )
            END AS naplnenost_pct
        FROM metrics_lokace_enriched
        GROUP BY oblast
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW metrics_pobocka AS
        SELECT
            pobocka_cislo,
            MAX(pobocka_nazev) AS pobocka_nazev,
            MAX(oblast) AS oblast,
            COUNT(*) AS pocet_lokaci,
            COUNT(kapacita_celkem) AS pocet_lokaci_s_kapacitou,
            SUM(stav_fondu_celkem) AS stav_fondu_celkem,
            SUM(kapacita_celkem) AS kapacita_celkem,
            SUM(CASE WHEN kapacita_celkem IS NOT NULL THEN stav_fondu_celkem END) AS stav_pri_pokryti_kapacitou,
            CASE
                WHEN SUM(kapacita_celkem) IS NULL OR SUM(kapacita_celkem) = 0 THEN NULL
                ELSE (
                    SUM(CASE WHEN kapacita_celkem IS NOT NULL THEN stav_fondu_celkem END) * 100.0
                    / SUM(kapacita_celkem)
                )
            END AS naplnenost_pct
        FROM metrics_lokace_enriched
        GROUP BY pobocka_cislo
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW metrics_sit AS
        SELECT
            SUM(stav_fondu_celkem) AS stav_fondu_celkem,
            SUM(kapacita_celkem) AS kapacita_celkem,
            SUM(CASE WHEN kapacita_celkem IS NOT NULL THEN stav_fondu_celkem END) AS stav_pri_pokryti_kapacitou,
            COUNT(*) AS pocet_lokaci,
            COUNT(kapacita_celkem) AS pocet_lokaci_s_kapacitou,
            CASE
                WHEN SUM(kapacita_celkem) IS NULL OR SUM(kapacita_celkem) = 0 THEN NULL
                ELSE (
                    SUM(CASE WHEN kapacita_celkem IS NOT NULL THEN stav_fondu_celkem END) * 100.0
                    / SUM(kapacita_celkem)
                )
            END AS naplnenost_pct
        FROM metrics_lokace_enriched
        """
    )

    meta = {
        "dim_pobocka": pb,
        "dim_lokace": dim_lok,
        "fact_kapacita_fyzicka": fact_fyz,
        "fact_kapacita_realokace": real_g,
        "realokace_raw": real,
        "lokace_master": lok,
        "kapacita_raw": kap,
        "oblast_kapacita_warnings": oblast_kap_warnings,
        "lookup_prepocet_dims": lp,
        "lookup_realok_dims": lr,
    }
    return con, meta


def _build_from_parquet(
    project_root: Path,
    raw: Path,
    kapacita_path: Path | None,
) -> tuple[duckdb.DuckDBPyConnection, dict]:
    """
    Nový hlavní řetězec: Přepočítané kapacity + Pobočky + Skutečný stav z Parquet v kořeni projektu.
    """
    _ensure_dirs()
    disc = discover_parquet_files(project_root)
    if "prepocet" not in disc or "pobocky" not in disc or "stav_fond" not in disc:
        raise FileNotFoundError(
            "V kořeni projektu chybí potřebné parquet soubory (Přepočítané kapacity, Pobočky, Skutečný stav)."
        )

    lok = load_lokace_skutecny_stav(disc["stav_fond"])
    lok["lokace_short_norm"] = lok["lokace_short"].map(normalize_lokace_short)

    kap = load_prepocet_kapacity(disc["prepocet"])
    kap = apply_prepocet_lokace_map(kap, load_lokace_map_prepocet(raw / LOKACE_MAP_PREPOCET))
    sklady_stats: dict[str, int] | None = None
    if "sklady" in disc:
        sk = load_sklady_kapacity(disc["sklady"])
        sk = apply_prepocet_lokace_map(sk, load_lokace_map_prepocet(raw / LOKACE_MAP_PREPOCET))
        # Explicitní mapování z požadavku: Jenštejn -> Jeneč / Sklad.
        # `pobocka_cislo` držíme číselné (92), protože model a DuckDB s ním pracují jako INT.
        mask_jenstejn = sk["pobocka_nazev"].fillna("").astype(str).str.strip().str.lower().eq("jenštejn")
        if mask_jenstejn.any():
            sk.loc[mask_jenstejn, "pobocka_cislo"] = 92
            sk.loc[mask_jenstejn, "pobocka_nazev"] = "Jeneč"
            sk.loc[mask_jenstejn, "oblast"] = "Sklad"
            # Po změně pobočky aplikujeme mapu znovu, aby se propsaly klíče typu 92.8 -> JEN-...
            sk = apply_prepocet_lokace_map(sk, load_lokace_map_prepocet(raw / LOKACE_MAP_PREPOCET))

        sk = sk[
            sk["lokace_short_norm"].fillna("").astype(str).str.strip().ne("")
            & sk["kapacita_fyzicka"].fillna(0).ne(0)
        ].copy()
        key_cols = ["pobocka_cislo", "lokace_short_norm", "och"]
        kap_key = pd.DataFrame(
            {c: kap[c].fillna("").astype(str).str.strip() for c in key_cols}
        )
        sk_key = pd.DataFrame(
            {c: sk[c].fillna("").astype(str).str.strip() for c in key_cols}
        )
        existing_keys = set(zip(kap_key["pobocka_cislo"], kap_key["lokace_short_norm"], kap_key["och"]))
        sk_merge_key = list(zip(sk_key["pobocka_cislo"], sk_key["lokace_short_norm"], sk_key["och"]))
        sk_new = sk[[k not in existing_keys for k in sk_merge_key]]
        kap = pd.concat([kap, sk_new[kap.columns]], ignore_index=True)
        sklady_stats = {
            "rows_source": int(len(sk)),
            "new_keys_added": int(len(sk_new)),
            "existing_keys_ignored": int(len(sk) - len(sk_new)),
        }
    _, _, oblast_kap_warnings = extract_oblast_z_kapacity(kap)

    pb = load_pobocky_parquet(disc["pobocky"])
    obl_s = oblast_z_prepocet(pd.read_parquet(disc["prepocet"]))
    pb = pb.merge(obl_s.rename("oblast"), left_on="pobocka_cislo", right_index=True, how="left")
    pb["oblast"] = pb["oblast"].fillna("Neurčeno")

    omap = raw / OBLAST_MAP
    oblast = load_oblast_map(omap if omap.exists() else None)
    if not oblast.empty and "oblast" in oblast.columns:
        if "pobocka_cislo" in oblast.columns:
            obl_c = oblast.dropna(subset=["pobocka_cislo"]).drop_duplicates(subset=["pobocka_cislo"], keep="last")
            pb = pb.merge(obl_c[["pobocka_cislo", "oblast"]], on="pobocka_cislo", how="left", suffixes=("", "_map"))
            if "oblast_map" in pb.columns:
                pb["oblast"] = pb["oblast"].fillna(pb["oblast_map"])
                pb = pb.drop(columns=["oblast_map"])
        if "pobocka_nazev" in oblast.columns:
            obl_n = oblast.dropna(subset=["pobocka_nazev", "oblast"]).copy()
            obl_n["__oblast_key"] = obl_n["pobocka_nazev"].str.strip().str.lower()
            obl_n = obl_n.drop_duplicates(subset=["__oblast_key"], keep="last")
            pb["__oblast_key"] = pb["pobocka_nazev"].str.strip().str.lower()
            pb = pb.merge(obl_n[["__oblast_key", "oblast"]], on="__oblast_key", how="left", suffixes=("", "_podle_nazvu"))
            if "oblast_podle_nazvu" in pb.columns:
                pb["oblast"] = pb["oblast"].fillna(pb["oblast_podle_nazvu"])
                pb = pb.drop(columns=["oblast_podle_nazvu"])
            pb = pb.drop(columns=["__oblast_key"])
    pb["oblast"] = pb["oblast"].fillna("Neurčeno")

    # Realokace: příznak je_realokace výhradně podle souboru „Skutečný stav - realokace.parquet“
    # (klíč pobočka + LOKACE_SHORT normalizovaný). CSV realokace.csv se v parquet režimu nepoužije.
    kr: pd.DataFrame | None = None
    parquet_source_overlap: dict | None = None
    if "stav_realokace" in disc:
        kr = normalize_kapacita_dataframe(pd.read_parquet(disc["stav_realokace"]), "")
        kr = normalize_columns(kr)
        kr = canonicalize_kapacita_columns(kr)
        if "pobocka_cislo" in kr.columns:
            kr["pobocka_cislo"] = pd.to_numeric(kr["pobocka_cislo"], errors="coerce").astype("Int64")
        if "lokace_short" in kr.columns:
            kr["lokace_short_norm"] = kr["lokace_short"].map(normalize_lokace_short)
        else:
            kr["lokace_short_norm"] = ""
        if "och" not in kr.columns:
            kr["och"] = np.nan
        kr["och"] = kr["och"].fillna("").astype(str).str.strip()
        kr.loc[kr["och"] == "", "och"] = np.nan
        kr["kapacita_realokace"] = pd.to_numeric(kr.get("kapacita_fyzicka"), errors="coerce")
        real_parq = _prepare_realokace(kr)
        keys_fond = _pobocka_lokace_keys(lok, "knoddel_cisloknih", "lokace_short_norm")
        keys_realok = _pobocka_lokace_keys(kr, "pobocka_cislo", "lokace_short_norm")
        both = keys_fond & keys_realok
        parquet_source_overlap = {
            "zdroj_fond": "Skutečný stav.parquet",
            "zdroj_realok": "Skutečný stav - realokace.parquet",
            "klic": "pobočka (číslo knihovny) + LOKACE_SHORT (normalizovaný)",
            "pocet_klicu_fond": len(keys_fond),
            "pocet_klicu_realok": len(keys_realok),
            "prunik": len(both),
            "jen_ve_fondu_ostatni": len(keys_fond - keys_realok),
            "jen_v_realokaci": len(keys_realok - keys_fond),
        }
    else:
        real_parq = _prepare_realokace(pd.DataFrame())

    real = real_parq.copy()
    if not real.empty and not pb.empty:
        mask_empty_nazev = real["pobocka_nazev"].fillna("").astype(str).str.strip() == ""
        if mask_empty_nazev.any():
            pn = pb.drop_duplicates(subset=["pobocka_cislo"]).set_index("pobocka_cislo")["pobocka_nazev"]
            alt = real["pobocka_cislo"].map(pn)
            real.loc[mask_empty_nazev, "pobocka_nazev"] = alt[mask_empty_nazev].fillna("").to_numpy()

    dim_lok = lok[
        [
            "lokace_key",
            "lokace_short",
            "lokace_short_norm",
            "knoddel_cisloknih",
            "knoddel_nazev",
            "stav_fondu",
            "datum",
        ]
    ].copy()
    dim_lok = dim_lok.rename(columns={"lokace_key": "lokace_id", "stav_fondu": "stav_fondu_celkem_zdroj"})
    dim_lok["pobocka_cislo"] = pd.to_numeric(dim_lok["knoddel_cisloknih"], errors="coerce").astype("Int64")

    rel_keys = real[["lokace_short_norm", "pobocka_cislo"]].drop_duplicates()
    rel_keys = rel_keys.dropna(subset=["lokace_short_norm"])
    rel_keys = rel_keys[rel_keys["lokace_short_norm"] != ""]

    dim_lok["je_realokace"] = False
    for _, row in rel_keys.iterrows():
        m = dim_lok["lokace_short_norm"] == row["lokace_short_norm"]
        if pd.notna(row["pobocka_cislo"]):
            m = m & (dim_lok["pobocka_cislo"] == row["pobocka_cislo"])
        dim_lok.loc[m, "je_realokace"] = True

    if not real.empty and "pobocka_nazev" in real.columns:
        for _, row in real.drop_duplicates(subset=["lokace_short_norm", "pobocka_nazev"]).iterrows():
            if (not row["pobocka_nazev"]) or str(row["pobocka_nazev"]).strip() == "":
                continue
            naz = str(row["pobocka_nazev"]).strip().lower()
            m = (dim_lok["lokace_short_norm"] == row["lokace_short_norm"]) & (
                dim_lok["knoddel_nazev"].fillna("").str.lower().str.strip() == naz
            )
            dim_lok.loc[m, "je_realokace"] = True

    kap_g = (
        kap.groupby(["pobocka_cislo", "lokace_short_norm", "och"], dropna=False)
        .agg(
            kapacita_fyzicka=("kapacita_fyzicka", "sum"),
            stav_na_regalu=("stav_na_regalu", "sum"),
        )
        .reset_index()
    )

    fact_fyz = kap_g.rename(columns={"och": "OCH"}).copy()
    fact_fyz["OCH"] = fact_fyz["OCH"].astype(object)

    if kr is not None and not kr.empty:
        real_g = (
            kr.groupby(["pobocka_cislo", "lokace_short_norm", "och"], dropna=False)
            .agg(kapacita_realokace=("kapacita_realokace", "sum"))
            .reset_index()
        )
        real_g = real_g.rename(columns={"och": "OCH"})
    else:
        real_g = pd.DataFrame(columns=["pobocka_cislo", "lokace_short_norm", "OCH", "kapacita_realokace"])

    loc_map = (
        dim_lok.groupby(["lokace_short_norm", "pobocka_cislo"], as_index=False)
        .agg(lokace_id=("lokace_id", "min"))
        .dropna(subset=["lokace_short_norm", "pobocka_cislo"])
    )
    fact_fyz = fact_fyz.merge(loc_map, on=["lokace_short_norm", "pobocka_cislo"], how="left")
    if not real_g.empty:
        real_g = real_g.merge(loc_map, on=["lokace_short_norm", "pobocka_cislo"], how="left")

    lookup_prepocet_dims = _build_lookup_prepocet_dims(kap, loc_map)
    lookup_realok_dims = _build_lookup_realok_dims(kr, loc_map)

    con, meta = _register_duckdb_model(
        pb,
        dim_lok,
        fact_fyz,
        real_g,
        real,
        lok,
        kap,
        oblast_kap_warnings,
        lookup_prepocet_dims=lookup_prepocet_dims,
        lookup_realok_dims=lookup_realok_dims,
    )
    if parquet_source_overlap is not None:
        meta["parquet_source_overlap"] = parquet_source_overlap
    if sklady_stats is not None:
        meta["sklady_merge_stats"] = sklady_stats
    return con, meta


def build_analytical_model(
    data_raw: Path | None = None,
    kapacita_path: Path | None = None,
) -> tuple[duckdb.DuckDBPyConnection, dict]:
    """
    Vrátí DuckDB spojení s nahranými tabulkami a pohledy + slovník metadat pro DQ.
    """
    _ensure_dirs()
    raw = data_raw or Path(__file__).resolve().parents[2] / "data_raw"
    project_root = Path(__file__).resolve().parents[2]
    if parquet_bundle_ready(project_root):
        return _build_from_parquet(project_root, raw, kapacita_path)

    lok_path = raw / "lokace-vsechny-nazev.csv"
    if not lok_path.exists():
        lok_path = DATA_RAW / LOKACE_NAZEV
    lok = load_lokace_master(lok_path if lok_path.exists() else None)
    lok["lokace_short_norm"] = lok["lokace_short"].map(normalize_lokace_short)

    omap = raw / OBLAST_MAP
    oblast = load_oblast_map(omap if omap.exists() else None)

    real_raw = load_realokace(raw / "realokace.csv" if (raw / "realokace.csv").exists() else None)
    real = _prepare_realokace(real_raw)

    kap = _load_kapacita(raw, kapacita_path)
    kap = normalize_columns(kap)
    kap = canonicalize_kapacita_columns(kap)
    if "kapacita_oblast" in kap.columns and "oblast" not in kap.columns:
        kap = kap.rename(columns={"kapacita_oblast": "oblast"})
    kap = dopln_pobocka_cislo_v_ramci_listu(kap)
    kap = dopln_pobocka_cislo_z_katalogu(kap, lok)
    if "pobocka_cislo" not in kap.columns:
        raise ValueError(
            "V kapacitní tabulce chybí sloupec s číslem knihovny (pobočka / číslo knihovny). "
            f"Dostupné sloupce: {sorted(map(str, kap.columns))}. "
            "Použijte např. KAPACITA_CISLOKNIH, číslo knihovny, knoddel_cisloknih, nebo doplnění z názvu listu přes katalog lokací."
        )
    if kap["pobocka_cislo"].isna().all():
        raise ValueError(
            "Sloupec čísla knihovny je ve všech řádcích prázdný — nešlo ho odvodit z názvu listu ani z katalogu poboček. "
            f"Sloupce: {sorted(map(str, kap.columns))}."
        )
    if "lokace_short" in kap.columns:
        kap["lokace_short_norm"] = kap["lokace_short"].map(normalize_lokace_short)
    else:
        kap["lokace_short_norm"] = ""
    kap = apply_prepocet_lokace_map(kap, load_lokace_map_prepocet(raw / LOKACE_MAP_PREPOCET))
    if "och" not in kap.columns:
        kap["och"] = np.nan
    kap["och"] = kap["och"].fillna("").astype(str).str.strip()
    kap.loc[kap["och"] == "", "och"] = np.nan
    if "pobocka_cislo" in kap.columns:
        kap["pobocka_cislo"] = pd.to_numeric(kap["pobocka_cislo"], errors="coerce").astype("Int64")
    if "kapacita_fyzicka" in kap.columns:
        kap["kapacita_fyzicka"] = pd.to_numeric(kap["kapacita_fyzicka"], errors="coerce")
    else:
        kap["kapacita_fyzicka"] = np.nan
    if "stav_na_regalu" in kap.columns:
        kap["stav_na_regalu"] = pd.to_numeric(kap["stav_na_regalu"], errors="coerce")
    else:
        kap["stav_na_regalu"] = np.nan

    if "oznaceni" not in kap.columns:
        kap["oznaceni"] = np.nan
    if "typ" not in kap.columns and "och" in kap.columns:
        kap["typ"] = kap["och"].astype(str)
    elif "typ" not in kap.columns:
        kap["typ"] = np.nan

    obl_z_kap_c, obl_z_kap_n, oblast_kap_warnings = extract_oblast_z_kapacity(kap)

    # dim pobočka
    pb = (
        lok.dropna(subset=["knoddel_cisloknih"])
        .drop_duplicates(subset=["knoddel_cisloknih", "knoddel_nazev"])[["knoddel_cisloknih", "knoddel_nazev"]]
        .rename(columns={"knoddel_cisloknih": "pobocka_cislo", "knoddel_nazev": "pobocka_nazev"})
    )
    pb["pobocka_nazev"] = pb["pobocka_nazev"].fillna("").str.strip()
    pb["pobocka_id"] = pb["pobocka_cislo"].astype(int)
    pb["oblast"] = np.nan

    # 1) Oblast z kapacitní tabulky (Excel: sloupec oblast na řádcích; list = pobočka)
    if not obl_z_kap_c.empty:
        pb = pb.merge(obl_z_kap_c.rename(columns={"oblast": "oblast_z_kap"}), on="pobocka_cislo", how="left")
    if "oblast_z_kap" not in pb.columns:
        pb["oblast_z_kap"] = np.nan
    if not obl_z_kap_n.empty:
        obl_z_kap_n = obl_z_kap_n.copy()
        obl_z_kap_n["__oblast_n"] = obl_z_kap_n["pobocka_nazev"].str.strip().str.lower()
        pb["__oblast_n"] = pb["pobocka_nazev"].str.strip().str.lower()
        pb = pb.merge(
            obl_z_kap_n[["__oblast_n", "oblast"]].rename(columns={"oblast": "oblast_z_kap_n"}),
            on="__oblast_n",
            how="left",
        )
        pb = pb.drop(columns=["__oblast_n"])
    if "oblast_z_kap_n" not in pb.columns:
        pb["oblast_z_kap_n"] = np.nan
    pb["oblast"] = pb["oblast_z_kap"].fillna(pb["oblast_z_kap_n"])
    pb = pb.drop(columns=[c for c in ("oblast_z_kap", "oblast_z_kap_n") if c in pb.columns])

    # 2) Doplnění z oblast_map.csv, kde v kapacitě chybí
    if not oblast.empty and "oblast" in oblast.columns:
        if "pobocka_cislo" in oblast.columns:
            obl_c = oblast.dropna(subset=["pobocka_cislo"]).drop_duplicates(subset=["pobocka_cislo"], keep="last")
            pb = pb.merge(obl_c[["pobocka_cislo", "oblast"]], on="pobocka_cislo", how="left", suffixes=("", "_map"))
            if "oblast_map" in pb.columns:
                pb["oblast"] = pb["oblast"].fillna(pb["oblast_map"])
                pb = pb.drop(columns=["oblast_map"])
        if "pobocka_nazev" in oblast.columns:
            obl_n = oblast.dropna(subset=["pobocka_nazev", "oblast"]).copy()
            obl_n["__oblast_key"] = obl_n["pobocka_nazev"].str.strip().str.lower()
            obl_n = obl_n.drop_duplicates(subset=["__oblast_key"], keep="last")
            pb["__oblast_key"] = pb["pobocka_nazev"].str.strip().str.lower()
            pb = pb.merge(obl_n[["__oblast_key", "oblast"]], on="__oblast_key", how="left", suffixes=("", "_podle_nazvu"))
            if "oblast_podle_nazvu" in pb.columns:
                pb["oblast"] = pb["oblast"].fillna(pb["oblast_podle_nazvu"])
                pb = pb.drop(columns=["oblast_podle_nazvu"])
            pb = pb.drop(columns=["__oblast_key"])
    if "oblast" not in pb.columns:
        pb["oblast"] = np.nan
    pb["oblast"] = pb["oblast"].fillna("Neurčeno")

    # dim lokace
    dim_lok = lok[
        [
            "lokace_key",
            "lokace_short",
            "lokace_short_norm",
            "knoddel_cisloknih",
            "knoddel_nazev",
            "stav_fondu",
            "datum",
        ]
    ].copy()
    dim_lok = dim_lok.rename(columns={"lokace_key": "lokace_id", "stav_fondu": "stav_fondu_celkem_zdroj"})
    dim_lok["pobocka_cislo"] = pd.to_numeric(dim_lok["knoddel_cisloknih"], errors="coerce").astype("Int64")

    # příznak realokace: shoda lokace_short + pobocka_cislo nebo název
    rel_keys = real[["lokace_short_norm", "pobocka_cislo"]].drop_duplicates()
    rel_keys = rel_keys.dropna(subset=["lokace_short_norm"])
    rel_keys = rel_keys[rel_keys["lokace_short_norm"] != ""]

    dim_lok["je_realokace"] = False
    for _, row in rel_keys.iterrows():
        m = dim_lok["lokace_short_norm"] == row["lokace_short_norm"]
        if pd.notna(row["pobocka_cislo"]):
            m = m & (dim_lok["pobocka_cislo"] == row["pobocka_cislo"])
        dim_lok.loc[m, "je_realokace"] = True

    # doplnění realokace podle názvu pobočky (když chybí číslo)
    if not real.empty and "pobocka_nazev" in real.columns:
        for _, row in real.drop_duplicates(subset=["lokace_short_norm", "pobocka_nazev"]).iterrows():
            if (not row["pobocka_nazev"]) or str(row["pobocka_nazev"]).strip() == "":
                continue
            naz = str(row["pobocka_nazev"]).strip().lower()
            m = (dim_lok["lokace_short_norm"] == row["lokace_short_norm"]) & (
                dim_lok["knoddel_nazev"].fillna("").str.lower().str.strip() == naz
            )
            dim_lok.loc[m, "je_realokace"] = True

    # agregace fyzické kapacity
    kap_g = (
        kap.groupby(["pobocka_cislo", "lokace_short_norm", "och"], dropna=False)
        .agg(
            kapacita_fyzicka=("kapacita_fyzicka", "sum"),
            stav_na_regalu=("stav_na_regalu", "sum"),
        )
        .reset_index()
    )

    fact_fyz = kap_g.rename(columns={"och": "OCH"}).copy()
    fact_fyz["OCH"] = fact_fyz["OCH"].astype(object)

    real_g = (
        real.groupby(["pobocka_cislo", "lokace_short_norm", "och"], dropna=False)
        .agg(kapacita_realokace=("kapacita_realokace", "sum"))
        .reset_index()
    )
    real_g = real_g.rename(columns={"och": "OCH"})

    loc_map = (
        dim_lok.groupby(["lokace_short_norm", "pobocka_cislo"], as_index=False)
        .agg(lokace_id=("lokace_id", "min"))
        .dropna(subset=["lokace_short_norm", "pobocka_cislo"])
    )
    fact_fyz = fact_fyz.merge(loc_map, on=["lokace_short_norm", "pobocka_cislo"], how="left")
    real_g = real_g.merge(loc_map, on=["lokace_short_norm", "pobocka_cislo"], how="left")

    lookup_prepocet_dims = _build_lookup_prepocet_dims(kap, loc_map)
    lookup_realok_dims = _build_lookup_realok_dims(None, loc_map)

    return _register_duckdb_model(
        pb,
        dim_lok,
        fact_fyz,
        real_g,
        real,
        lok,
        kap,
        oblast_kap_warnings,
        lookup_prepocet_dims=lookup_prepocet_dims,
        lookup_realok_dims=lookup_realok_dims,
    )


def run_etl(
    data_raw: Path | None = None,
    kapacita_path: Path | None = None,
) -> tuple[duckdb.DuckDBPyConnection, Path]:
    """Spustí ETL, uloží exporty CSV a DQ report."""
    con, meta = build_analytical_model(data_raw=data_raw, kapacita_path=kapacita_path)
    _ensure_dirs()

    exp = [
        ("dim_pobocka", "dim_pobocka.csv"),
        ("dim_lokace", "dim_lokace.csv"),
        ("fact_kapacita_fyzicka", "fact_kapacita_fyzicka.csv"),
        ("fact_kapacita_realokace", "fact_kapacita_realokace.csv"),
        ("fact_fond", "fact_fond.csv"),
        ("metrics_lokace_enriched", "metrics_lokace.csv"),
        ("metrics_lokace_och", "metrics_lokace_och.csv"),
        ("metrics_sit", "metrics_sit.csv"),
        ("metrics_oblast", "metrics_oblast.csv"),
        ("metrics_pobocka", "metrics_pobocka.csv"),
        ("lookup_prepocet_dims", "lookup_prepocet_dims.csv"),
        ("lookup_realok_dims", "lookup_realok_dims.csv"),
    ]
    for view, fn in exp:
        df = con.execute(f"SELECT * FROM {view}").df()
        df.to_csv(EXPORTS / fn, index=False, encoding="utf-8-sig")

    dq = build_quality_report(con, meta)
    write_quality_report(dq, DQ_REPORT)
    return con, DQ_REPORT


if __name__ == "__main__":
    _, report = run_etl()
    print(f"ETL dokončeno. Report: {report}")
