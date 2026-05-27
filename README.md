# Analýza kapacity knihovního fondu (MKP)

Streamlit dashboard pro vizualizaci kapacity fondu napříč pobočkami (MKP).

Repo je primárně určený pro nasazení přes **Streamlit Community Cloud** (z GitHubu).  
Lokální/offline spouštění a portable balíčky v tomto projektu už nepoužíváme.

## Nasazení (Streamlit Community Cloud)

- **Zdroj**: repository + branch `main`
- **Testování**: změny dělejte na feature branch, otestujte, pak merge do `main`
- **Rollback**: nejbezpečněji přes *revert commit* v GitHub Desktop (vrátí ověřený stav bez přepisování historie)

## Uživatelská část (jak číst dashboard)

### K čemu model slouží

Model porovnává:

- kapacitu, kterou máme fyzicky k dispozici (přepočtený kapacitní plán),
- kapacitu plánovanou pro realokace (aktuální kapacitní plán),
- skutečný stav fondu (počet svazků).

Výsledek je přehled kapacitního vytížení po lokalitách, pobočkách, oblastech a OCH.

### Jak číst horní KPI (aktuální výběr filtrů)

- **Přepočítaný kapacitní plán** = fyzická kapacita ze zdrojů přepočtu.
- **Aktuální kapacitní plán** = kapacita z dat realokace.
- **Skutečný stav (počet svazků)** = skutečný stav fondu.
- **Naplněná kapacita (%)** = `Skutečný stav / Přepočítaný kapacitní plán * 100`.
- **Zbývající kapacita (počet svazků)** = `Přepočítaný kapacitní plán - Skutečný stav`.
- **Počet lokací (výběr)** = počet lokací po aplikaci filtrů.

### Co znamenají sloupce „Přetížená“ a „Riziková“

Tyto sloupce jsou odvozené z metriky **Naplněná kapacita (%)** na úrovni jedné lokace:

- **Přetížená** = `Naplněná kapacita (%) > 100 %`
- **Riziková** = `Naplněná kapacita (%) > 90 %`

Poznámka:

- pokud lokace nemá definovanou kapacitu (nelze spočítat naplnění), hodnota je prázdná.
- stav **Přetížená = Ano** současně znamená i **Riziková = Ano**.

### Jak číst blok „Celá síť — referenční KPI (bez filtru)“

Má stejné metriky jako horní řada, ale počítá je nad celou sítí bez UI filtrů.  
Je to referenční baseline pro porovnání s filtrovaným výběrem.

### Co znamenají kategorie a filtry

- **Oblast / Název pobočky / Lokace**: klasické prostorové filtry.
- **OCH (oborový charakter)**: filtruje rozpad OCH (detail lokace) a podílové pohledy; neomezuje lokace v hlavních KPI (ty jsou na úrovni lokace).
- **Jen realokační lokace**: omezuje dataset na lokace označené jako realokační.
- **Označení + Typ** (z přepočtu): filtrují jen nerealokační lokace.
- **KAPACITA_DESKRIPTOR + KAPACITA_OCH** (z realokace): filtrují jen realokační lokace.

Skupiny filtrů jsou záměrně oddělené, aby se nemíchaly dva různé datové zdroje.

### Podílové pohledy (% síť + odchylky poboček)

Dashboard obsahuje sekce, které ukazují:

- **OCH**: podíl oborů (OCH) na **kapacitě realokace** v síti + odchylky poboček (Δ v procentních bodech).
- **Typ**: podíl hodnot `Typ` na **fyzické kapacitě** v síti + odchylky poboček (užitečné např. pro „Doporučujeme“).
- **Piktogramy**: podíl **16 whitelistovaných piktogramů** (podmnožina `Označení`) na fyzické kapacitě + odchylky poboček.

Exporty jsou v `data_processed/exports/` jako:
`metrics_share_och_*.csv`, `metrics_share_typ_*.csv`, `metrics_share_piktogram_*.csv`.

### Priorita vstupů: Parquet v kořeni projektu

Pokud jsou v kořeni projektu tyto soubory `*.parquet`, mají prioritu před `data_raw`:

- **Pobočky** (`poboček` / `pobocek`)
- **Přepočítané kapacity** (`kapacity`)
- **Skutečný stav** (`skuteč` / `skutec`, ale ne `realok`)
- **Skutečný stav - realokace** (`realok`)
- **Sklady** (`sklady`) — volitelné doplnění chybějících skladových klíčů

`Sklady.parquet` se zapojuje v režimu **append-only**:

- přidají se jen nové klíče, které nejsou v hlavním přepočtu,
- existující klíče se nepřepisují,
- klíč je `(pobocka_cislo, lokace_short_norm, och)`,
- speciální mapování: `Jenštejn -> pobocka_cislo=92, pobocka_nazev=Jeneč, oblast=Sklad`.

### Vstupní data v `data_raw/`

| Soubor | Popis |
|--------|--------|
| `lokace-vsechny-nazev.csv` | Lokace a stav fondu (SQL export) |
| `lokace-vsechny.csv` / `lokace-neprazdne.csv` | Volitelný alternativní zdroj stavu fondu |
| `Kapacity - návrh pro sběr dat.xlsx` | Hlavní vstup kapacity (pobočka = list) |
| `kapacita.xlsx` | Záložní vstup kapacity |
| `kapacita.csv` | CSV fallback, pokud není Excel |
| `realokace.csv` | Legacy vstup pro realokace |
| `oblast_map.csv` | Doplňková mapa oblastí |
| `lokace_map_prepocet.csv` | Mapa lokací mezi přepočtem a stavem fondu |

### Mapování lokací z přepočtu

Parquet exporty často používají jiné kódy lokace než skutečný stav.  
Do `lokace_map_prepocet.csv` doplňte:

- `pobocka_cislo`
- `lokace_short_zdroj` (kód z přepočtu)
- `lokace_short_cil` (kód ze skutečného stavu)

Pro Jeneč (knihovna 92) je běžné mapovat kódy `92.x` na `JEN-*`.

### Pravidla výpočtu kapacity

- U běžných lokací se používá fyzická kapacita z přepočtu.
- U realokačních lokací se používá kapacita z realokačního plánu.
- **Oblast Sklad** je výjimka: používá se fyzická kapacita.
- Chybějící kapacita se nepřepisuje nulou.

### Oblasti poboček

Primární zdroj oblasti je kapacitní tabulka (sloupec `oblast`).  
`oblast_map.csv` se použije jen jako doplněk tam, kde oblast chybí.

Kanonické oblasti:

- Ústřední knihovna
- Jih
- Sklad
- Jihozápad
- Jihovýchod
- Středozápad
- Severovýchod

### Struktura projektu

```text
data_raw/           # vstupy
data_processed/     # exporty CSV + DQ report
src/
  config.py         # cesty
  io/               # načítání CSV / Excel / Parquet
  transform/        # normalizace klíčů
  model/            # DuckDB model, pohledy, ETL
  validation/       # datová kvalita
  ui/               # Streamlit
app.py              # vstupní bod Streamlit
```

### Datový model (výstupy ETL)

- dimenze: `dim_pobocka`, `dim_lokace`
- fakta: `fact_fond`, `fact_kapacita_fyzicka`, `fact_kapacita_realokace`
- hlavní pohledy: `metrics_lokace_enriched`, `metrics_lokace_och`, `metrics_oblast`, `metrics_pobocka`, `metrics_sit`

### Přesná mapa UI metrik na data

| Popisek v UI | Výpočet / sloupec |
|---|---|
| Přepočítaný kapacitní plán | `sum(kapacita_fyzicka_sum)` |
| Aktuální kapacitní plán | `sum(kapacita_realokace_sum)` |
| Skutečný stav (počet svazků) | `sum(stav_fondu_celkem)` |
| Naplněná kapacita (%) | `sum(stav_fondu_celkem) / sum(kapacita_fyzicka_sum) * 100` |
| Zbývající kapacita (počet svazků) | `sum(kapacita_fyzicka_sum) - sum(stav_fondu_celkem)` |

