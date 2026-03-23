"""Streamlit dashboard — analýza kapacity fondu (názvy metrik sladěné s „Kapacity v MKP“ / Power BI)."""

from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from ..config import (
    DATA_RAW,
    KAPACITA_XLSX,
    KAPACITA_XLSX_PRIMARY,
    LOKACE_MAP_PREPOCET,
    OBLAST_MAP,
    PROJECT_ROOT,
)
from ..model.pipeline import build_analytical_model
from . import dashboard_labels as L

EMPTY_LABEL = "(prázdné)"


def _model_data_fingerprint() -> str:
    """
    Otisk vstupů ETL — při změně souborů se invaliduje Streamlit cache,
    jinak by dashboard držel starý DuckDB model (např. špatný podíl realokace).
    """
    parts: list[str] = []
    for p in sorted(PROJECT_ROOT.glob("*.parquet")):
        try:
            st_ = p.stat()
            parts.append(f"{p.name}:{st_.st_mtime_ns}:{st_.st_size}")
        except OSError:
            continue
    raw = DATA_RAW if DATA_RAW.exists() else (PROJECT_ROOT / "data_raw")
    for name in (
        "lokace-vsechny-nazev.csv",
        "realokace.csv",
        "kapacita.csv",
        KAPACITA_XLSX_PRIMARY,
        KAPACITA_XLSX,
        OBLAST_MAP,
        LOKACE_MAP_PREPOCET,
    ):
        p = raw / name
        if p.exists():
            try:
                st_ = p.stat()
                parts.append(f"data_raw/{name}:{st_.st_mtime_ns}")
            except OSError:
                continue
    return "|".join(parts) if parts else "no-inputs"


@st.cache_resource(show_spinner="Načítám a propojuji data…")
def _get_connection(data_fingerprint: str) -> duckdb.DuckDBPyConnection:
    """data_fingerprint mění cache při úpravě vstupních parquet/CSV — jinak by zůstal starý model v paměti."""
    assert data_fingerprint  # klíč pro st.cache_resource
    raw = DATA_RAW if DATA_RAW.exists() else (PROJECT_ROOT / "data_raw")
    con, _ = build_analytical_model(data_raw=raw)
    return con


def _apply_filters(
    df: pd.DataFrame,
    oblast: list,
    pobocka: list,
    lokace: list,
    och: list,
    jen_realok: str,
) -> pd.DataFrame:
    out = df.copy()
    if oblast and "oblast" in out.columns:
        out = out[out["oblast"].isin(oblast)]
    if pobocka and "pobocka_nazev" in out.columns:
        out = out[out["pobocka_nazev"].isin(pobocka)]
    if lokace and "lokace_short" in out.columns:
        out = out[out["lokace_short"].isin(lokace)]
    if och and "OCH" in out.columns:
        out = out[out["OCH"].astype(str).isin(och)]
    if jen_realok == "Ano" and "je_realokace" in out.columns:
        out = out[out["je_realokace"] == True]  # noqa: E712
    elif jen_realok == "Ne" and "je_realokace" in out.columns:
        out = out[out["je_realokace"] == False]  # noqa: E712
    return out


def _to_filter_values(sel: list[str]) -> list[str]:
    """Token (prázdné) z multiselectu mapuje na prázdný řetězec v datech."""
    return ["" if x == EMPTY_LABEL else x for x in sel]


def _unique_str_options(s: pd.Series) -> list[str]:
    u = sorted(s.dropna().astype(str).unique().tolist())
    out: list[str] = []
    for x in u:
        out.append(EMPTY_LABEL if str(x).strip() == "" else x)
    return sorted(set(out))


def _apply_source_dimension_filters(
    mloc: pd.DataFrame,
    prep: pd.DataFrame,
    real: pd.DataFrame,
    oz_sel: list[str],
    typ_sel: list[str],
    deskr_sel: list[str],
    och_sel: list[str],
) -> pd.DataFrame:
    """
    Označení + Typ (prepocet) zužují jen **ostatní** lokace.
    Deskriptor + KAPACITA_OCH (realok) jen **realokační** lokace.
    """
    oz_sel = _to_filter_values(oz_sel)
    typ_sel = _to_filter_values(typ_sel)
    deskr_sel = _to_filter_values(deskr_sel)
    och_sel = _to_filter_values(och_sel)

    ostatni = mloc[~mloc["je_realokace"]]
    realok = mloc[mloc["je_realokace"]]

    if oz_sel or typ_sel:
        if prep.empty:
            ostatni = ostatni.iloc[0:0]
        else:
            p = prep.copy()
            if oz_sel:
                p = p[p["oznaceni"].astype(str).isin(oz_sel)]
            if typ_sel:
                p = p[p["typ"].astype(str).isin(typ_sel)]
            allowed_o = pd.to_numeric(p["lokace_id"], errors="coerce").dropna().astype(int)
            ostatni = ostatni[ostatni["lokace_id"].isin(set(allowed_o.tolist()))]

    if deskr_sel or och_sel:
        if real.empty:
            realok = realok.iloc[0:0]
        else:
            r = real.copy()
            if deskr_sel:
                r = r[r["kapacita_deskriptor"].astype(str).isin(deskr_sel)]
            if och_sel:
                r = r[r["kapacita_och"].astype(str).isin(och_sel)]
            allowed_r = pd.to_numeric(r["lokace_id"], errors="coerce").dropna().astype(int)
            realok = realok[realok["lokace_id"].isin(set(allowed_r.tolist()))]

    return pd.concat([ostatni, realok], ignore_index=True)


def _fmt_num(x: float | None) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    return f"{x:,.0f}".replace(",", " ")


def _bool_to_anone(s: pd.Series) -> pd.Series:
    def one(x: object) -> str:
        if pd.isna(x):
            return "—"
        return "Ano" if bool(x) else "Ne"

    return s.map(one)


def _column_config_lokace(df: pd.DataFrame) -> dict[str, Any]:
    """Tisíce a % přes column_config — sloupce zůstávají číselné kvůli řazení v tabulce."""
    cfg: dict[str, Any] = {}
    int_cols = (
        L.COL_PREPOCITANA,
        L.COL_SKUTECNY_STAV,
        L.COL_VOLNA_KAPACITA,
        L.COL_ROZDIL,
        "lokace_id",
        "pobocka_cislo",
        "kapacita_fyzicka_sum",
        "kapacita_realokace_sum",
    )
    for c in int_cols:
        if c in df.columns:
            cfg[c] = st.column_config.NumberColumn(c, format="localized")
    if L.COL_NAPLNENI in df.columns:
        cfg[L.COL_NAPLNENI] = st.column_config.NumberColumn(L.COL_NAPLNENI, format="%.2f %%")
    return cfg


def _column_config_oblast(df: pd.DataFrame) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    for c in (
        L.COL_SKUTECNY_STAV,
        L.COL_PREPOCITANA,
        L.KPI_STAV_PRI_POKRYTI,
        "Počet lokací",
        "Počet lokací s kapacitou",
    ):
        if c in df.columns:
            cfg[c] = st.column_config.NumberColumn(c, format="localized")
    if L.COL_NAPLNENI in df.columns:
        cfg[L.COL_NAPLNENI] = st.column_config.NumberColumn(L.COL_NAPLNENI, format="%.2f %%")
    return cfg


def _column_config_och(df: pd.DataFrame) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    for c in (L.COL_KAP_FYZ, L.COL_KAP_REALOK, L.COL_KAP_EFEKTIVNI, L.COL_STAV_OCH):
        if c in df.columns:
            cfg[c] = st.column_config.NumberColumn(c, format="localized")
    for c in ("lokace_id", "pobocka_cislo"):
        if c in df.columns:
            cfg[c] = st.column_config.NumberColumn(c, format="localized")
    return cfg


def _lokace_table_display(df: pd.DataFrame) -> pd.DataFrame:
    """Sloupce podle kontrolního přehledu (svazky) / Power BI."""
    out = df.copy()
    if "je_realokace" in out.columns:
        out[L.COL_REALOKACE] = out["je_realokace"].map({True: "Ano", False: "Ne"})
        out = out.drop(columns=["je_realokace"])
    for bc in ("pretizena", "rizikova"):
        if bc in out.columns:
            out[bc] = _bool_to_anone(out[bc])
    rename = {
        "lokace_short": L.COL_LOKACE_SHORT,
        "pobocka_nazev": L.COL_POBOCKA,
        "oblast": L.FILTER_OBLAST,
        "kapacita_celkem": L.COL_PREPOCITANA,
        "stav_fondu_celkem": L.COL_SKUTECNY_STAV,
        "naplnenost_pct": L.COL_NAPLNENI,
        "volna_kapacita": L.COL_VOLNA_KAPACITA,
        "rozdil": L.COL_ROZDIL,
        "pretizena": L.COL_PRETIZENA,
        "rizikova": L.COL_RIZIKOVA,
    }
    return out.rename(columns={k: v for k, v in rename.items() if k in out.columns})


def _oblast_table_display(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "oblast": "Oblast",
            "pocet_lokaci": "Počet lokací",
            "pocet_lokaci_s_kapacitou": "Počet lokací s kapacitou",
            "stav_fondu_celkem": L.COL_SKUTECNY_STAV,
            "kapacita_celkem": L.COL_PREPOCITANA,
            "stav_pri_pokryti_kapacitou": L.KPI_STAV_PRI_POKRYTI,
            "naplnenost_pct": L.COL_NAPLNENI,
        }
    )


def _och_table_display(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(
        columns={
            "lokace_short": L.COL_LOKACE_SHORT,
            "pobocka_nazev": L.COL_POBOCKA,
            "oblast": L.FILTER_OBLAST,
            "OCH": L.FILTER_OCH,
            "kapacita_fyzicka": L.COL_KAP_FYZ,
            "kapacita_realokace": L.COL_KAP_REALOK,
            "kapacita_effective": L.COL_KAP_EFEKTIVNI,
            "stav_fondu_och": L.COL_STAV_OCH,
            "je_realokace": L.COL_REALOKACE,
        }
    )
    if L.COL_REALOKACE in out.columns:
        out[L.COL_REALOKACE] = out[L.COL_REALOKACE].map({True: "Ano", False: "Ne"})
    return out


def render_dashboard() -> None:
    st.set_page_config(page_title=L.PAGE_TITLE, layout="wide")
    st.title(L.MAIN_TITLE)
    st.caption(L.MAIN_CAPTION)

    con = _get_connection(_model_data_fingerprint())

    mloc = con.execute("SELECT * FROM metrics_lokace_enriched").df()
    moch = con.execute("SELECT * FROM metrics_lokace_och").df()
    msit = con.execute("SELECT * FROM metrics_sit").df()
    mobl = con.execute("SELECT * FROM metrics_oblast").df()
    mpob = con.execute("SELECT * FROM metrics_pobocka").df()
    lookup_prepocet = con.execute("SELECT * FROM lookup_prepocet_dims").df()
    lookup_realok = con.execute("SELECT * FROM lookup_realok_dims").df()

    with st.sidebar:
        st.subheader("Filtry")
        oblast_opts = sorted(mloc["oblast"].dropna().unique().tolist())
        oblast_sel = st.multiselect(L.FILTER_OBLAST, oblast_opts, default=[])
        pob_opts = sorted(mloc["pobocka_nazev"].dropna().unique().tolist())
        pob_sel = st.multiselect(L.FILTER_NAZEV_POBOCKY, pob_opts, default=[])
        lok_opts = sorted(mloc["lokace_short"].dropna().unique().tolist())
        lok_sel = st.multiselect(L.FILTER_LOKACE_SHORT, lok_opts, default=[])
        jen_realok = st.selectbox(L.FILTER_JEN_REALOK, ["Vše", "Ano", "Ne"], index=0)

        st.markdown(f"**{L.FILTER_GROUP_PREPOCET}**")
        oz_opts = _unique_str_options(lookup_prepocet["oznaceni"]) if not lookup_prepocet.empty else []
        typ_opts = _unique_str_options(lookup_prepocet["typ"]) if not lookup_prepocet.empty else []
        oz_sel = st.multiselect(L.FILTER_OZNACENI_PREPOCET, oz_opts, default=[])
        typ_sel = st.multiselect(L.FILTER_TYP_PREPOCET, typ_opts, default=[])

        st.markdown(f"**{L.FILTER_GROUP_REALOK}**")
        deskr_opts = _unique_str_options(lookup_realok["kapacita_deskriptor"]) if not lookup_realok.empty else []
        roch_opts = _unique_str_options(lookup_realok["kapacita_och"]) if not lookup_realok.empty else []
        deskr_sel = st.multiselect(L.FILTER_DESKRIPTOR_REALOK, deskr_opts, default=[])
        och_realok_sel = st.multiselect(L.FILTER_OCH_REALOK, roch_opts, default=[])

        st.caption(L.FILTER_DIM_CAPTION)
        st.caption(L.CACHE_CAPTION)

    mloc_f = _apply_filters(mloc, oblast_sel, pob_sel, lok_sel, [], jen_realok)
    mloc_f = _apply_source_dimension_filters(
        mloc_f,
        lookup_prepocet,
        lookup_realok,
        oz_sel,
        typ_sel,
        deskr_sel,
        och_realok_sel,
    )
    moch_f = _apply_filters(moch, oblast_sel, pob_sel, lok_sel, [], jen_realok)
    if not moch_f.empty and not mloc_f.empty and "lokace_id" in moch_f.columns:
        moch_f = moch_f[moch_f["lokace_id"].isin(mloc_f["lokace_id"])]

    kap_sum = mloc_f["kapacita_celkem"].sum()
    stav_sum = mloc_f["stav_fondu_celkem"].sum()
    pokryte = mloc_f[mloc_f["kapacita_celkem"].notna()]
    nap = (
        (pokryte["stav_fondu_celkem"].sum() / pokryte["kapacita_celkem"].sum() * 100.0)
        if len(pokryte) and pokryte["kapacita_celkem"].sum() not in (0, None)
        else None
    )
    zbyva = mloc_f["volna_kapacita"].sum() if "volna_kapacita" in mloc_f.columns else None

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric(L.KPI_PREPOCITANA_KAPACITA, _fmt_num(float(kap_sum)) if pd.notna(kap_sum) else "—")
    k2.metric(L.KPI_SKUTECNY_STAV_SVAZKY, _fmt_num(float(stav_sum)) if pd.notna(stav_sum) else "—")
    k3.metric(L.KPI_NAPLNENI_PCT, f"{nap:.2f} %" if nap is not None else "—")
    k4.metric(L.KPI_ZBYVAJICI_KAPACITA, _fmt_num(float(zbyva)) if zbyva is not None and pd.notna(zbyva) else "—")
    k5.metric(L.KPI_POCET_LOKACI_VYBER, str(len(mloc_f)))

    st.subheader(L.SECTION_VYBER_TRI)
    if mloc_f.empty:
        st.caption("Žádné lokace v aktuálním výběru — uvolněte filtry.")
    else:
        prep_tri = pd.to_numeric(mloc_f.get("kapacita_fyzicka_sum"), errors="coerce").fillna(0).sum()
        real_tri = pd.to_numeric(mloc_f.get("kapacita_realokace_sum"), errors="coerce").fillna(0).sum()
        stav_tri = pd.to_numeric(mloc_f["stav_fondu_celkem"], errors="coerce").fillna(0).sum()
        tri_df = pd.DataFrame(
            {
                "kategorie": [L.TRI_PREPOCET, L.TRI_STAV, L.TRI_REALOK],
                "Svazky": [float(prep_tri), float(stav_tri), float(real_tri)],
            }
        )
        fig_tri = px.bar(
            tri_df,
            x="Svazky",
            y="kategorie",
            orientation="h",
            color="kategorie",
            title=L.CHART_TRI_TITLE,
            labels={"kategorie": "", "Svazky": L.AXIS_SVAZKY},
        )
        fig_tri.update_layout(showlegend=False, yaxis=dict(categoryorder="array", categoryarray=tri_df["kategorie"]))
        st.plotly_chart(fig_tri, use_container_width=True)
        st.caption(L.TRI_CAPTION)

    st.subheader(L.SECTION_SIT)
    if not msit.empty:
        r = msit.iloc[0]
        a1, a2, a3, a4 = st.columns(4)
        a1.metric(
            L.KPI_CELKOVA_KAPACITA_SITE,
            _fmt_num(float(r["kapacita_celkem"])) if pd.notna(r["kapacita_celkem"]) else "—",
        )
        a2.metric(
            L.KPI_SKUTECNY_STAV_PRITOMNE,
            _fmt_num(float(r["stav_fondu_celkem"])) if pd.notna(r["stav_fondu_celkem"]) else "—",
        )
        a3.metric(
            L.KPI_STAV_PRI_POKRYTI,
            _fmt_num(float(r["stav_pri_pokryti_kapacitou"])) if pd.notna(r.get("stav_pri_pokryti_kapacitou")) else "—",
        )
        a4.metric(
            L.KPI_NAPLNENI_POKRYTE,
            f"{r['naplnenost_pct']:.2f} %" if pd.notna(r["naplnenost_pct"]) else "—",
        )

    st.subheader(L.SECTION_REALOK_PIE)
    pie_df = mloc_f.groupby("je_realokace", dropna=False).size().reset_index(name="pocet")
    pie_df["typ"] = pie_df["je_realokace"].map({True: L.PIE_REALOKACE, False: L.PIE_OSTATNI})
    if not pie_df.empty and pie_df["pocet"].sum() > 0:
        fig_pie = px.pie(pie_df, values="pocet", names="typ", title=L.PIE_TITLE)
        st.plotly_chart(fig_pie, use_container_width=True)

    st.subheader(L.SECTION_OBLASTI)
    if not mobl.empty:
        mobl_f = mobl[mobl["oblast"].isin(oblast_sel)] if oblast_sel else mobl
        fig_obl = px.bar(
            mobl_f.sort_values("naplnenost_pct", ascending=False),
            x="oblast",
            y="naplnenost_pct",
            title=L.CHART_OBLAST_TITLE,
            labels={"oblast": L.AXIS_OBLAST, "naplnenost_pct": L.COL_NAPLNENI},
        )
        st.plotly_chart(fig_obl, use_container_width=True)
        mobl_disp = _oblast_table_display(mobl_f)
        st.dataframe(
            mobl_disp,
            column_config=_column_config_oblast(mobl_disp),
            use_container_width=True,
        )

    st.subheader(L.SECTION_POBOCKY)
    if not mpob.empty:
        mpob_f = mpob.copy()
        if oblast_sel:
            mpob_f = mpob_f[mpob_f["oblast"].isin(oblast_sel)]
        fig_p = px.bar(
            mpob_f.sort_values("naplnenost_pct", ascending=False).head(40),
            x="pobocka_nazev",
            y="naplnenost_pct",
            title=L.CHART_POBOCKY_TITLE,
            labels={"pobocka_nazev": L.AXIS_NAZEV_POBOCKY, "naplnenost_pct": L.COL_NAPLNENI},
        )
        fig_p.update_xaxes(tickangle=45)
        st.plotly_chart(fig_p, use_container_width=True)

    st.subheader(L.SECTION_DETAIL_POBOCKY)
    detail_pob_opts = sorted(mloc_f["pobocka_nazev"].dropna().unique().tolist())
    if not detail_pob_opts:
        st.info(L.DETAIL_POBOCKA_EMPTY)
        pob_choice = ""
    elif len(detail_pob_opts) == 1:
        st.caption(L.DETAIL_POBOCKA_SINGLE)
        pob_choice = detail_pob_opts[0]
        sub = mloc_f[mloc_f["pobocka_nazev"] == pob_choice]
        sub_disp = _lokace_table_display(sub)
        st.dataframe(
            sub_disp,
            column_config=_column_config_lokace(sub_disp),
            use_container_width=True,
        )
    else:
        pob_choice = st.selectbox(L.SELECT_POBOCKA, [""] + detail_pob_opts)
        if pob_choice:
            sub = mloc_f[mloc_f["pobocka_nazev"] == pob_choice]
            sub_disp = _lokace_table_display(sub)
            st.dataframe(
                sub_disp,
                column_config=_column_config_lokace(sub_disp),
                use_container_width=True,
            )

    st.subheader(L.SECTION_DETAIL_OCH)
    lok_opts_f = sorted(mloc_f["lokace_short"].dropna().unique().tolist())
    lok_choice = st.selectbox(L.SELECT_LOKACE, [""] + lok_opts_f)
    if lok_choice:
        och_sub = moch_f[moch_f["lokace_short"] == lok_choice]
        disp = _och_table_display(och_sub)
        st.dataframe(
            disp,
            column_config=_column_config_och(disp),
            use_container_width=True,
        )

    st.subheader(L.SECTION_PRETIZENE)
    pret = mloc_f[mloc_f["pretizena"] == True].sort_values("naplnenost_pct", ascending=False)  # noqa: E712
    pret_disp = _lokace_table_display(pret.head(30))
    st.dataframe(
        pret_disp,
        column_config=_column_config_lokace(pret_disp),
        use_container_width=True,
    )

    st.subheader(L.SECTION_NEPOUZITE)
    st.caption(L.NEPOUZITE_CAPTION)
    free = mloc_f[mloc_f["naplnenost_pct"].notna() & (mloc_f["naplnenost_pct"] <= 100)].sort_values(
        "naplnenost_pct", ascending=True
    )
    free_disp = _lokace_table_display(free.head(30))
    st.dataframe(
        free_disp,
        column_config=_column_config_lokace(free_disp),
        use_container_width=True,
    )

    st.divider()
    st.caption(L.FOOTER_CAPTION)


def main() -> None:
    render_dashboard()
