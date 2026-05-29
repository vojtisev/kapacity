"""Kontroly datové kvality a Markdown report."""

from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

from ..config import OBLASTI_KANONICKE, PIKTOGRAMY, extract_piktogram_code, is_piktogram, normalize_oznaceni


def _df_block(df: pd.DataFrame, max_rows: int = 30) -> str:
    """Textová tabulka bez závislosti na `tabulate` (pandas.to_markdown)."""
    if df.empty:
        return "_(prázdné)_"
    view = df.head(max_rows)
    return "```text\n" + view.to_string(index=False) + "\n```"


def build_quality_report(con: duckdb.DuckDBPyConnection, meta: dict) -> dict:
    dim_lok: pd.DataFrame = meta["dim_lokace"]
    fact_fyz: pd.DataFrame = meta["fact_kapacita_fyzicka"]
    kap_raw: pd.DataFrame = meta["kapacita_raw"]
    lok_master: pd.DataFrame = meta["lokace_master"]
    real_raw: pd.DataFrame = meta["realokace_raw"]
    lookup_prepocet: pd.DataFrame | None = meta.get("lookup_prepocet_dims")
    lookup_realok: pd.DataFrame | None = meta.get("lookup_realok_dims")

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

    sks = meta.get("sklady_merge_stats")
    if sks:
        lines.append(
            "### Integrace Sklady.parquet\n"
            f"Řádků po vyčištění ve zdroji: **{sks.get('rows_source', 0)}**.\n\n"
            f"- Nově přidané skladové klíče: **{sks.get('new_keys_added', 0)}**\n"
            f"- Ignorované klíče (už existovaly v Přepočítaných kapacitách): **{sks.get('existing_keys_ignored', 0)}**"
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

    # OCH / TYP / piktogramy — přehled hodnot (pomáhá interpretaci podílů)
    try:
        och_top = con.execute(
            """
            SELECT OCH, SUM(kapacita_realokace) AS kap_realok
            FROM metrics_lokace_och
            GROUP BY 1
            ORDER BY kap_realok DESC NULLS LAST
            LIMIT 20
            """
        ).df()
    except Exception:
        och_top = pd.DataFrame()

    if not och_top.empty:
        t_present = (och_top["OCH"].astype(str) == "T").any()
        lines.append(
            "### OCH — top podle kapacity realokace\n"
            + ("(Pozn.: hodnota `T` v OCH nebyla v top 20 nalezena.)\n\n" if not t_present else "\n")
            + _df_block(och_top, max_rows=20)
        )

    if not kap_raw.empty and "typ" in kap_raw.columns and "kapacita_fyzicka" in kap_raw.columns:
        typ_sum = (
            kap_raw.assign(typ=kap_raw["typ"].fillna("").astype(str).str.strip())
            .groupby("typ", dropna=False)["kapacita_fyzicka"]
            .sum()
            .reset_index()
            .sort_values("kapacita_fyzicka", ascending=False)
            .head(30)
        )
        lines.append("### TYP (přepočet) — top podle fyzické kapacity\n\n" + _df_block(typ_sum, max_rows=30))

    # Piktogramy — pokrytí whitelistu + podezřelé varianty.
    # Primárně z `oznaceni` (přepočet), ale umíme odhalit i „X (piktogram)“ v reallok deskriptoru.
    if not kap_raw.empty and "kapacita_fyzicka" in kap_raw.columns and "oznaceni" in kap_raw.columns:
        k = kap_raw.copy()
        k["pikto_code"] = k["oznaceni"].map(extract_piktogram_code)
        k["je_piktogram"] = k["pikto_code"].notna()
        pik_sum = (
            k[k["je_piktogram"]]
            .groupby("pikto_code", dropna=False)["kapacita_fyzicka"]
            .sum()
            .reset_index()
            .sort_values("kapacita_fyzicka", ascending=False)
        )
        missing = [code for code in PIKTOGRAMY.keys() if code not in set(pik_sum["pikto_code"].tolist())]
        total_realok = float(real_raw["kapacita_realokace"].sum())
        pik_total = float(pik_sum["kapacita_realokace"].sum()) if not pik_sum.empty else 0.0
        share_note = (
            f"\n\n**Podíl v dashboardu (metrics_share_piktogram_*):** "
            f"`podil_pct` = kapacita daného piktogramu / **celková kapacita realokace** "
            f"(pobočka nebo síť), ne jen součet piktogramových řádků. "
            f"V této dávce: celá realokace = {total_realok:,.0f} svazků, "
            f"řádky s rozpoznaným piktogramem = {pik_total:,.0f} svazků "
            f"({(100.0 * pik_total / total_realok if total_realok else 0):.1f} % realokace)."
        )
        lines.append(
            "### Piktogramy (whitelist 16) — součty kapacity\n\n"
            + (_df_block(pik_sum, max_rows=30) if not pik_sum.empty else "_Nenalezen žádný řádek s piktogramem._")
            + (f"\n\nChybějící kódy z whitelistu: **{', '.join(missing)}**." if missing else "")
            + share_note
        )

        # „Téměř shody“: normalize_oznaceni + odstranění teček/mezer + upper
        def _near_key(x: object) -> str:
            s = normalize_oznaceni(x)
            s = s.replace(".", "").replace(" ", "").upper()
            return s

        whitelist_near = { _near_key(code): code for code in PIKTOGRAMY.keys() }
        k["ozn_norm"] = k["oznaceni"].map(normalize_oznaceni)
        k["near"] = k["oznaceni"].map(_near_key)
        near = k[(~k["je_piktogram"]) & (k["near"].isin(set(whitelist_near.keys())))]
        if not near.empty:
            near_tbl = (
                near.groupby(["ozn_norm", "near"], dropna=False)["kapacita_fyzicka"]
                .sum()
                .reset_index()
                .sort_values("kapacita_fyzicka", ascending=False)
                .head(30)
            )
            lines.append(
                "### Piktogramy — podezřelé varianty zápisu (téměř shoda s whitelistem)\n\n"
                + _df_block(near_tbl, max_rows=30)
            )

    # KAPACITA_DESKRIPTOR — duplicity a varianty zápisu (používá se ve filtru realokace)
    if lookup_realok is not None and not lookup_realok.empty and "kapacita_deskriptor" in lookup_realok.columns:
        lr = lookup_realok.copy()
        lr["deskr_norm"] = lr["kapacita_deskriptor"].fillna("").astype(str).str.strip().str.casefold()
        # 1) více OCH na stejný deskriptor v rámci lokace
        multi_och = (
            lr.groupby(["lokace_id", "deskr_norm"], dropna=False)["kapacita_och"]
            .nunique()
            .reset_index(name="pocet_och")
        )
        bad = multi_och[multi_och["pocet_och"] > 1].sort_values("pocet_och", ascending=False).head(50)
        lines.append(
            "### KAPACITA_DESKRIPTOR — více OCH na stejný deskriptor v rámci lokace\n"
            + (f"Počet problémových klíčů: **{len(bad)}**.\n\n" if len(bad) else "Počet problémových klíčů: **0**.\n")
            + (_df_block(bad, max_rows=50) if len(bad) else "")
        )

        # 2) varianty zápisu, které se po normalizaci slučují
        variants = (
            lr.groupby(["deskr_norm"], dropna=False)["kapacita_deskriptor"]
            .nunique()
            .reset_index(name="varianty")
        )
        var_bad = variants[variants["varianty"] > 1].sort_values("varianty", ascending=False).head(50)
        if len(var_bad):
            # ukázat příklady konkrétních variant pro top deskr_norm
            examples = (
                lr[lr["deskr_norm"].isin(set(var_bad["deskr_norm"].head(15).tolist()))]
                .groupby("deskr_norm")["kapacita_deskriptor"]
                .apply(lambda s: ", ".join(sorted(set(s.astype(str).tolist()))[:10]))
                .reset_index(name="priklady")
            )
            lines.append(
                "### KAPACITA_DESKRIPTOR — varianty zápisu (po casefold/trim)\n\n"
                + _df_block(examples, max_rows=50)
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
