"""Kontroly datové kvality a Markdown report."""

from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

from ..config import OBLASTI_KANONICKE


def build_quality_report(con: duckdb.DuckDBPyConnection, meta: dict) -> dict:
    dim_lok: pd.DataFrame = meta["dim_lokace"]
    fact_fyz: pd.DataFrame = meta["fact_kapacita_fyzicka"]
    kap_raw: pd.DataFrame = meta["kapacita_raw"]
    lok_master: pd.DataFrame = meta["lokace_master"]
    real_raw: pd.DataFrame = meta["realokace_raw"]

    lines: list[str] = []

    pso = meta.get("parquet_source_overlap")
    if pso:
        lines.append(
            "### Překryv zdrojů: Skutečný stav vs realokace (Parquet)\n"
            f"Klíč: **{pso.get('klic', 'pobočka + LOKACE_SHORT')}**.\n\n"
            f"| | Počet unikátních klíčů |\n|---|--:|\n"
            f"| Pouze v „{pso.get('zdroj_fond', 'Skutečný stav.parquet')}“ (ostatní lokace) | **{pso.get('jen_ve_fondu_ostatni', 0)}** |\n"
            f"| Pouze v „{pso.get('zdroj_realok', 'realokace.parquet')}“ (bez řádku ve fondu) | **{pso.get('jen_v_realokaci', 0)}** |\n"
            f"| V obou souborech (průnik) | **{pso.get('prunik', 0)}** |\n\n"
            f"Celkem klíčů ve fondu: **{pso.get('pocet_klicu_fond', 0)}**, v realokaci: **{pso.get('pocet_klicu_realok', 0)}**."
        )

    # Konflikty oblasti uvnitř kapacitních řádků (Excel)
    okw = meta.get("oblast_kapacita_warnings") or []
    if okw:
        lines.append(
            "### Oblast z kapacitní tabulky (varování)\n"
            + "\n".join(f"- {w}" for w in okw)
        )

    # Oblasti — kontrola vůči kanonické množině
    pb_meta = meta.get("dim_pobocka")
    if pb_meta is not None and "oblast" in pb_meta.columns:
        hodnoty = pb_meta["oblast"].dropna().unique()
        neznam = [h for h in hodnoty if h not in OBLASTI_KANONICKE and str(h) != "Neurčeno"]
        lines.append(
            "### Oblasti (mapa poboček)\n"
            f"Kanonické oblasti: {', '.join(OBLASTI_KANONICKE)}.\n\n"
            f"Počet poboček s hodnotou mimo kanon (kromě „Neurčeno“): **{len(neznam)}**."
            + (f" Hodnoty: {', '.join(map(str, neznam))}." if neznam else "")
        )

    # 1 duplicity lokací (stejné lokace_id by neměly)
    dup_id = dim_lok[dim_lok.duplicated(subset=["lokace_id"], keep=False)]
    lines.append(f"### Duplicity lokace_id\nPočet řádků s duplicitním lokace_id: **{len(dup_id)}**.")

    # duplicita klíč pobočka + short
    dup_key = dim_lok[dim_lok.duplicated(subset=["pobocka_cislo", "lokace_short_norm"], keep=False)]
    lines.append(
        f"### Duplicity (pobocka_cislo + lokace_short)\nPočet řádků: **{len(dup_key)}** "
        "(více záznamů se stejným klíčem — při joinu se použije min(lokace_id))."
    )

    # 2 nepropojené lokace mezi zdroji
    fyz_keys = fact_fyz.dropna(subset=["lokace_id"])[["lokace_id"]].drop_duplicates()
    lok_ids = set(dim_lok["lokace_id"].astype(int))
    f_ids = set(fyz_keys["lokace_id"].dropna().astype(int))
    bez_kapacity = len(lok_ids - f_ids)
    lines.append(
        f"### Lokace bez kapacitních řádků (ze souboru kapacity)\n"
        f"Lokací ve fondu bez shody v kapacitní tabulce: **{bez_kapacity}** "
        f"(kapacita_celkem bude NULL, pokud neexistuje ani realokační řádek)."
    )

    km = lok_master.copy()
    km["pobocka_cislo"] = pd.to_numeric(km["knoddel_cisloknih"], errors="coerce")
    km["lokace_short_norm"] = km["lokace_short"].map(lambda x: str(x).strip().upper())
    kap_branch_loc = kap_raw.drop_duplicates(subset=["pobocka_cislo", "lokace_short_norm"])
    merged = kap_branch_loc.merge(
        km,
        on=["pobocka_cislo", "lokace_short_norm"],
        how="left",
        indicator=True,
    )
    kap_bez_lokace = int((merged["_merge"] == "left_only").sum())
    lines.append(
        f"### Kapacitní řádky bez shody ve fondu (lokace master)\nPočet: **{kap_bez_lokace}**."
    )

    # 3 lokace bez kapacity (žádný řádek ve fyzické ani realok po ETL)
    m = con.execute(
        """
        SELECT COUNT(*) FROM dim_lokace l
        WHERE NOT EXISTS (
            SELECT 1 FROM fact_effective_capacity_full e WHERE e.lokace_id = l.lokace_id
        )
        """
    ).fetchone()[0]
    lines.append(f"### Lokace bez jakékoli efektivní kapacity (pohled ETL)\nPočet: **{m}**.")

    # 4 lokace bez stavu fondu
    bez_stavu = int((dim_lok["stav_fondu_celkem_zdroj"].fillna(0) == 0).sum())
    lines.append(
        f"### Lokace se stavem fondu 0 nebo NULL\nPočet: **{bez_stavu}** (informační — může být legální)."
    )

    # 5 rozdíly realokační vs fyzická (u realokačních lokací, kde jsou obě)
    diff = con.execute(
        """
        SELECT
            l.lokace_id,
            l.lokace_short,
            SUM(f.kapacita_fyzicka) AS kf,
            SUM(r.kapacita_realokace) AS kr
        FROM dim_lokace l
        JOIN fact_kapacita_fyzicka f ON l.lokace_id = f.lokace_id
        LEFT JOIN fact_kapacita_realokace r
            ON l.lokace_id = r.lokace_id AND (f.OCH IS NOT DISTINCT FROM r.OCH)
        WHERE l.je_realokace AND f.kapacita_fyzicka IS NOT NULL AND r.kapacita_realokace IS NOT NULL
        GROUP BY 1, 2
        HAVING ABS(kf - kr) > 0.01
        """
    ).df()
    lines.append(
        f"### Realokační lokace s odlišnou fyzickou a realokační kapacitou (součty podle OCH)\n"
        f"Počet lokací s rozdílem: **{len(diff)}** (očekáváno — rozhoduje realokační hodnota)."
    )

    # 6 pokrytí sítě
    total_lok = len(dim_lok)
    with_kap = con.execute(
        "SELECT COUNT(DISTINCT lokace_id) FROM fact_effective_capacity_full WHERE kapacita_effective IS NOT NULL"
    ).fetchone()[0]
    lines.append(
        f"### Pokrytí kapacitními daty\n"
        f"Lokací celkem: **{total_lok}**, s nenulovou / definovanou efektivní kapacitou: **{with_kap}**."
    )

    # validace vazby na pobočku
    bez_pobocky = int(dim_lok["pobocka_cislo"].isna().sum())
    lines.append(f"### Lokace bez čísla pobočky (KNODDEL)\nPočet: **{bez_pobocky}**.")

    body = "\n\n".join(lines)
    return {
        "sections": lines,
        "duplicity_lokace_id": len(dup_id),
        "duplicity_klic": len(dup_key),
        "lokace_bez_kapacity_rows": bez_kapacity,
        "kapacity_bez_lokace": kap_bez_lokace,
        "lokace_bez_efektivni_kapacity": m,
        "markdown_body": body,
        "diff_realok_vs_fyz": diff,
    }


def write_quality_report(dq: dict, path: Path) -> None:
    md = (
        "# Report datové kvality — kapacity fondu\n\n"
        "Vygenerováno automaticky po ETL.\n\n"
        f"{dq['markdown_body']}\n\n"
        "---\n\n"
        "## Poznámky\n\n"
        "- Chybějící kapacita se **nepovažuje za nulu** — metriky `naplnenost_pct` jsou NULL.\n"
        "- U realokačních lokací platí **kapacita_realokace**; fyzická kapacita slouží jen k porovnání v tomto reportu.\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(md, encoding="utf-8")
