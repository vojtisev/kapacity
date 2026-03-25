"""
Popisky metrik a filtrů sladěné s reportem „Kapacity v MKP“ (Power BI / PNG reference).
"""

# Obecné
PAGE_TITLE = "Kapacity v MKP"
MAIN_TITLE = "Kapacity v MKP"
MAIN_CAPTION = (
    "Přepočítané kapacity · skutečný stav fondu · realokace — oblast → pobočka → lokace → OCH"
)

# Filtry (postranní panel) — viz „Kontrola dat“, „Realokace“
FILTER_OBLAST = "Oblast"
FILTER_NAZEV_POBOCKY = "Název pobočky"
FILTER_LOKACE_SHORT = "Filtr lokace (LOKACE_SHORT)"
FILTER_OCH = "OCH (oborový charakter)"
FILTER_JEN_REALOK = "Jen realokační lokace"

# Filtry podle zdroje — Přepočítané kapacity (ostatní lokace)
FILTER_GROUP_PREPOCET = "Přepočítané kapacity (ostatní lokace)"
FILTER_OZNACENI_PREPOCET = "Označení"
FILTER_TYP_PREPOCET = "Typ"

# Filtry — Skutečný stav - realokace (realokační lokace)
FILTER_GROUP_REALOK = "Skutečný stav - realokace (realokační lokace)"
FILTER_DESKRIPTOR_REALOK = "KAPACITA_DESKRIPTOR"
FILTER_OCH_REALOK = "KAPACITA_OCH"

FILTER_DIM_CAPTION = (
    "Označení a Typ platí jen pro **ostatní** lokace (zdroj Přepočítané kapacity). "
    "Deskriptor a KAPACITA_OCH jen pro **realokační** lokace (zdroj Skutečný stav - realokace). "
    "Skupiny se nepřekrývají — každá zužuje jen svůj typ lokací."
)
CACHE_CAPTION = (
    "Data ETL se cachují podle data souborů v kořeni (*.parquet) a v data_raw/. "
    "Po změně exportů znovu načtěte stránku."
)

# KPI — výběr podle filtrů (kontrolní přehled / lokace)
KPI_PREPOCITANA_KAPACITA = "Přepočítaný kapacitní plán"
KPI_PLAN_REALOKACE = "Aktuální kapacitní plán"
KPI_SKUTECNY_STAV_SVAZKY = "Skutečný stav (počet svazků)"
KPI_NAPLNENI_PCT = "Naplněná kapacita (%)"
KPI_ZBYVAJICI_KAPACITA = "Zbývající kapacita (počet svazků)"
KPI_POCET_LOKACI_VYBER = "Počet lokací (výběr)"

# Přehled tří řad (vyfiltrovaný výběr)
SECTION_VYBER_TRI = "Přehled výběru — přepočítaný plán · skutečný stav · aktuální plán"
CHART_TRI_TITLE = "Součty ve svazcích (aktuální filtry)"
TRI_PREPOCET = "Přepočítaný kapacitní plán"
TRI_STAV = "Skutečný stav"
TRI_REALOK = "Aktuální kapacitní plán"
AXIS_SVAZKY = "Svazky"
TRI_CAPTION = (
    "Přes lokace v aktuálním výběru: **přepočítaný kapacitní plán** = součet fyzické kapacity z přepočtu, "
    "**skutečný stav** = svazky ve fondu, **aktuální kapacitní plán** = součet kapacity dle souboru realokace."
)

# KPI — celá síť (bez filtru) — viz přehled „Kapacity“
SECTION_SIT = "Celá síť — referenční KPI (bez filtru)"
KPI_CELKOVA_KAPACITA_SITE = "Přepočítaný kapacitní plán"
KPI_SKUTECNY_STAV_PRITOMNE = "Aktuální kapacitní plán"
KPI_STAV_PRI_POKRYTI = "Skutečný stav (pokryté lokace)"
KPI_NAPLNENI_POKRYTE = "Naplněná kapacita (%)"
KPI_ZBYVAJICI_SITE = "Zbývající kapacita (počet svazků)"
KPI_POCET_LOKACI_SITE = "Počet lokací"

# Realokace — výsečový graf
SECTION_REALOK_PIE = "Realokace vs ostatní lokace"
PIE_TITLE = "Počet lokací"
PIE_REALOKACE = "Realokace"
PIE_OSTATNI = "Ostatní lokace"

# Grafy
SECTION_OBLASTI = "Přehled oblastí"
CHART_OBLAST_TITLE = "Naplněná kapacita podle oblasti"
AXIS_OBLAST = "Oblast"

SECTION_POBOCKY = "Přehled poboček"
CHART_POBOCKY_TITLE = "Naplněná kapacita — top 40 poboček"
AXIS_NAZEV_POBOCKY = "Název pobočky"

# Detail tabulky
SECTION_DETAIL_POBOCKY = "Detail pobočky — lokace a naplněnost"
SELECT_POBOCKA = "Vyberte název pobočky (v rozsahu aktuálních filtrů)"
DETAIL_POBOCKA_EMPTY = "V aktuálním výběru filtrů nejsou žádné pobočky — uvolněte nebo upravte filtry v postranním panelu."
DETAIL_POBOCKA_SINGLE = "Jen jedna pobočka v rozsahu filtrů — zobrazení je automaticky."

SECTION_DETAIL_OCH = "Detail lokace — rozpad OCH"
SELECT_LOKACE = "Vyberte lokaci (LOKACE_SHORT)"

SECTION_PRETIZENE = "Nejvíce přetížené lokace"
SECTION_NEPOUZITE = "Nejvíce nevyužité lokace (nízká naplněnost)"
NEPOUZITE_CAPTION = (
    "Zobrazeny jsou jen lokace s naplněností nejvýše 100 %, seřazené od nejnižší k vyšší."
)

# Sloupce pro zobrazení (mapování z interních názvů)
COL_LOKACE_SHORT = "LOKACE_SHORT"
COL_PREPOCITANA = "Přepočítaný kapacitní plán"
COL_SKUTECNY_STAV = "Skutečný stav (počet svazků)"
COL_NAPLNENI = "Naplněná kapacita (%)"
COL_PRETIZENA = "Přetížená"
COL_RIZIKOVA = "Riziková"
COL_REALOKACE = "Realokační lokace"
COL_VOLNA_KAPACITA = "Zbývající kapacita (počet svazků)"
COL_ROZDIL = "Rozdíl (stav − kapacita)"

# Detail OCH (metrics_lokace_och)
COL_KAP_FYZ = "Přepočítaný kapacitní plán (fyzický)"
COL_KAP_REALOK = "Aktuální kapacitní plán"
COL_KAP_EFEKTIVNI = "Kapacita efektivní"
COL_STAV_OCH = "Skutečný stav (počet svazků)"
COL_POBOCKA = "Pobočka"

FOOTER_CAPTION = "Export agregací: `data_processed/exports/` · DQ report: `data_processed/data_quality_report.md`"
