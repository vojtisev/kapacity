# Analýza kapacity knihovního fondu (MKP)

Pythonová aplikace pro zpracování a vizualizaci kapacity fondu napříč pobočkami.  
Cesty k datům a ke skriptům jsou odvozené od **složky projektu** (kde leží `app.py`), ne od „aktuálního adresáře“ v CMD — stačí zkopírovat celý adresář a spouštět `.bat` soubory odtud.

README je rozdělené na:

- **část pro uživatele**: co model ukazuje, jak číst KPI a filtry,
- **technickou část**: zdroje dat, ETL, datový model, implementační pravidla.

### Struktura projektu (základ)

```text
KAPACITY/
├── app.py
├── requirements.txt
├── README.md
├── build_portable.bat      # jednorázově (IT): embeddable Python + závislosti → runtime\
├── download_wheels.bat     # volitelně: wheels\ pro úplně offline sestavení
├── setup_venv.bat          # jednorázově: venv + pip install (když je Python v PATH)
├── run_dashboard.bat       # Streamlit dashboard (runtime\ nebo .venv)
├── run_etl.bat             # ETL + exporty (runtime\ nebo .venv)
├── set_proxy_mlp.bat       # volaný z .bat — proxy MKP (pip)
├── data_raw/               # vstupy (CSV/Excel/mapování)
├── data_processed/         # výstupy ETL (po spuštění)
└── src/                    # kód aplikace
```

Parquet soubory v kořeni (vedle `app.py`) patří do stejné kopie složky jako data v `data_raw/`.

## 1) Uživatelská část

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
- **Jen realokační lokace**: omezuje dataset na lokace označené jako realokační.
- **Označení + Typ** (z přepočtu): filtrují jen nerealokační lokace.
- **KAPACITA_DESKRIPTOR + KAPACITA_OCH** (z realokace): filtrují jen realokační lokace.

Skupiny filtrů jsou záměrně oddělené, aby se nemíchaly dva různé datové zdroje.

### Spuštění po zkopírování ze síťového disku (doporučeno)

Postup je stejný v principu jako u souvisejících nástrojů (např. projekt **VYRAZOVANI**): **nejdřív celou složku zkopírujte na lokální disk**, pak spouštějte aplikaci.

**Uživatelé bez práva instalovat software (doporučená distribuce):**  
IT / správce **jednou** na počítači s internetem spustí **`build_portable.bat`** → vznikne složka **`runtime\python\`** s embeddable Pythonem a všemi balíky z `requirements.txt`. Tuto složku lze zkopírovat s celým projektem koncovým uživatelům — ti pak už **nic neinstalují**, jen **`run_dashboard.bat`** / **`run_etl.bat`**. Podrobně [docs/PORTABLE-OFFLINE-BALICEK.md](docs/PORTABLE-OFFLINE-BALICEK.md).

**Varianta s Pythonem na stanici:**  
1. **Zkopírujte celý adresář** `KAPACITY` z OneDrive / síťové složky na **lokální disk** (např. `C:\Projekty\KAPACITY`).  
   Důvod: vytváření virtuálního prostředí (`.venv`), pip a někdy i Streamlit bývají na dlouhých cestách UNC nebo synchronizovaném OneDrive spolehlivější z lokální kopie.
2. **Jednou** dvojklikem spusťte **`setup_venv.bat`** (nainstaluje balíky z `requirements.txt` do `.venv`).
3. Pak podle potřeby **`run_dashboard.bat`** (dashboard) nebo **`run_etl.bat`** (přepočet exportů).

Skripty **`run_dashboard.bat`** a **`run_etl.bat`** automaticky preferují **`runtime\python\`**, pokud existuje, jinak **`.venv`**.

**Alternativa:** spuštění přímo ze síťové cesty nebo z mapovaného disku může fungovat; pokud `setup_venv.bat` selže nebo je extrémně pomalý, použijte lokální kopii — podrobně [docs/SITOVY-DISK-WINDOWS.md](docs/SITOVY-DISK-WINDOWS.md).

**Síť MKP / firewall:** proxy pro pip je v `set_proxy_mlp.bat` (volají ji hlavní `.bat`); řešení problémů s pip: [docs/PIP-FIREWALL.md](docs/PIP-FIREWALL.md).

### Rychlé spuštění (shrnutí)

| Platforma | Postup |
|-----------|--------|
| **Windows** | S předanou složkou **`runtime\`** (po `build_portable.bat`): přímo `run_dashboard.bat` / `run_etl.bat`. Jinak `setup_venv.bat` → `run_dashboard.bat` / `run_etl.bat` |
| **Linux / macOS (vývoj)** | `python3 -m venv .venv`, aktivace, `pip install -r requirements.txt`, pak `python3 -m streamlit run app.py` / `python3 -m src.model.pipeline` |

## 2) Technická část

### Požadavky a instalace

- Python 3.11+ (na 3.9 je potřeba ověřit kompatibilitu typových anotací)
- závislosti: `requirements.txt`

```bash
cd KAPACITY
python3 -m pip install -r requirements.txt
```

**Windows ze síťového disku / OneDrive:** viz [docs/SITOVY-DISK-WINDOWS.md](docs/SITOVY-DISK-WINDOWS.md).  
Proxy v MKP síti je řešená přes `set_proxy_mlp.bat`; detaily jsou v [docs/PIP-FIREWALL.md](docs/PIP-FIREWALL.md).

**Offline portable:** sestavení **`runtime\python\`** je popsáno v [docs/PORTABLE-OFFLINE-BALICEK.md](docs/PORTABLE-OFFLINE-BALICEK.md) (`build_portable.bat`). V Gitu je jen zdroj; hotový `runtime\` se necommituje (velikost).

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

