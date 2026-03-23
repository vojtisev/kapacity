# Analýza kapacity knihovního fondu (MKP)

Pythonová datová aplikace pro **zpracování**, **propojení** a **vizualizaci** kapacity a stavu fondu napříč pobočkami. Rozlišuje **realokační kapacitu** a **fyzickou (přepočítanou) kapacitu**; v běžném režimu z Parquetů je realokace ze souboru *Skutečný stav - realokace*, v legacy režimu z `data_raw/realokace.csv`. **Význam metrik a vzorce** jsou popsány níže v sekci *Metriky: význam a výpočet*.

## Požadavky

- Python 3.11+ (doporučeno; na 3.9 je potřeba ověřit kompatibilitu typových anotací)
- Závislosti: viz `requirements.txt`

## Instalace

```bash
cd KAPACITY
python3 -m pip install -r requirements.txt
```

## Parquet v kořeni projektu (priorita)

Pokud v **kořeni** repozitáře (vedle `app.py`) leží tyto soubory `*.parquet`:

- **Pobočky** (název obsahuje `poboček` / `pobocek`),
- **Přepočítané kapacity** (název obsahuje `kapacity`),
- **Skutečný stav** (název obsahuje `skuteč` / `skutec` a ne `realok`),
- **Skutečný stav - realokace** (název obsahuje `realok`),

ETL je **nabídne přednostně** před Excel/CSV v `data_raw/`. Vyžaduje `pyarrow` (v `requirements.txt`).

V tomto režimu je příznak **realokace vs. ostatní** (`je_realokace`) odvozen **výhradně** ze souboru **Skutečný stav - realokace.parquet** (klíč číslo knihovny + `LOKACE_SHORT`); ostatní lokace vycházejí ze **Skutečný stav.parquet**. Překryv obou množin hlásí DQ report.

## Vstupní data (`data_raw/`)

| Soubor | Popis |
|--------|--------|
| `lokace-vsechny-nazev.csv` | Lokace a stav fondu (export z SQL — aplikace parsuje datové řádky) |
| `lokace-vsechny.csv` / `lokace-neprazdne.csv` | Volitelný alternativní zdroj stavu fondu |
| **`Kapacity - návrh pro sběr dat.xlsx`** | **Hlavní vstup kapacity** (pobočka = list, sloupce včetně **`oblast`**). Umístěte do `data_raw/` pod tímto přesným názvem. |
| `kapacita.xlsx` | Volitelná záloha: pokud soubor výše v `data_raw/` není, použije se `kapacita.xlsx` (stejná struktura). |
| `kapacita.csv` | Export kapacity (oddělovač `;`); použije se jen pokud **není žádný** z výše uvedených Excelů — lze doplnit sloupec `OBLAST` / `KAPACITA_OBLAST` |
| `realokace.csv` | Lokace pro realokaci + `kapacita_realokace` (OCH volitelné) |
| `oblast_map.csv` | Mapování poboček do oblasti (stejný význam jako sloupec **oblast** ve vaší tabulce poboček) |
| **`lokace_map_prepocet.csv`** | **Volitelné** sjednocení kódů lokace mezi *Přepočítanými kapacitami* a *Skutečným stavem* (viz níže) |

**Mapa lokací z přepočtu (`lokace_map_prepocet.csv`).** Parquet exporty často používají jiné kódy lokace než *Skutečný stav* (např. z přepočtu `92.1` vs. ve fondu `JEN-PVP`). Soubor **neupravujte v Parquetu** — při každém refreshi zůstane nesoulad. Doplňte řádky v `data_raw/`: `pobocka_cislo`, `lokace_short_zdroj` (kód z přepočtu), `lokace_short_cil` (cílový `LOKACE_SHORT` ze Skutečného stavu / realokace pro join). Jedna pobočka může mít libovolně řádků.

**Vzdálené sklady 1–9 (Jeneč / knihovna 92):** myslíme tím **všech devět** lokací, kde v názvu pobočky/lokace je „Vzdálený sklad 1“ až „Vzdálený sklad 9“ — v `LOKACE_SHORT` to odpovídá devíti kódům `JEN-*` (ne všechny ostatní `JEN-*` na stejné knihovně). Řádky **`JEN-KANC`** a **`JEN-PROST`** patří k názvu „Jeneč“ (provoz), **ne** k číslovaným vzdáleným skladům 1–9 — do té sady je nepočítejte, pokud je z přepočtu nemapujete zvlášť.

Referenční přiřazení (ověřte v aktuálním exportu):

| VS | `LOKACE_SHORT` |
|----|----------------|
| 1 | JEN-PVP |
| 2 | JEN-PV |
| 3 | JEN-DEPOZ |
| 4 | JEN-RFUK |
| 5 | JEN-RFOKF |
| 6 | JEN-ZASOB |
| 7 | JEN-MOFNAU |
| 8 | JEN-DEPJST |
| 9 | JEN-PVKONZ |

V přepočtu se může objevit jen část kódů (např. `92.1`, `92.2`, `92.6`) — pro každý **skutečně** vyskytující se zdrojový kód přidejte jeden řádek mapy na příslušný `JEN-*` z tabulky. Po doplnění spusťte znovu ETL.

**Pravidlo kapacity:** u lokace s `je_realokace = TRUE` se pro **běžné pobočky** použije **realokační** kapacita; jinde **fyzická** z přepočtu. **Oblast Sklad** (vzdálené sklady apod.) je výjimka — tam se v metrikách vždy bere **fyzická kapacita** z přepočtu, protože realokační údaj ve zdroji často neodpovídá sběru. Chybějící hodnota se **nepřepisuje nulou**.

### Oblasti poboček

**Primární zdroj:** soubor **`data_raw/Kapacity - návrh pro sběr dat.xlsx`** — na každém listu (pobočka) je ve sloupci **`oblast`** u řádků uvedena oblast; pobočka a kapacita jdou z této šablony. Při ETL se z řádků odvodí jedna hodnota oblasti na `pobocka_cislo` (případně podle názvu listu). Při více různých hodnotách u jedné pobočky se použije **modus** a do DQ reportu přibude varování.

**Doplnění:** `data_raw/oblast_map.csv` jen tam, kde z Excelu oblast nešla získat (priorita: nejdřív Excel/CSV kapacity, pak mapa).

Kanonické názvy oblastí (stejně jako ve vašich datech):

Ústřední knihovna, Jih, Sklad, Jihozápad, Jihovýchod, Středozápad, Severovýchod.

- Propojení z kapacity je podle **`pobocka_cislo`** (sloupec jako u ostatních sloupců KAPACITA_*).
- Volitelně lze v `oblast_map.csv` doplnit **`pobocka_nazev`** pro řádky bez čísla.

## Spuštění ETL a exportů

Vygeneruje tabulky do `data_processed/exports/` a report `data_processed/data_quality_report.md`:

```bash
python3 -m src.model.pipeline
```

## Streamlit dashboard

**Dva samostatné příkazy** — po ETL vždy na **novém řádku** spusťte dashboard (neslepovat s předchozím řádkem, jinak vznikne neexistující příkaz `pythonstreamlit`).

Z adresáře projektu (`KAPACITY`):

```bash
python3 -m streamlit run app.py
```

Alternativa, pokud máte `streamlit` v PATH:

```bash
streamlit run app.py
```

Dashboard obsahuje KPI, přehledy podle oblasti a pobočky, detail lokace podle OCH, tabulky přetížených a nevyužitých lokací a filtry.

**Filtry podle zdroje (Streamlit):** Ze **Přepočítaných kapacit** (parquet) se nabízejí **Označení** a **Typ** — po výběru zužují **jen ostatní** (nerealokační) lokace. Ze souboru **Skutečný stav - realokace** se nabízejí **KAPACITA_DESKRIPTOR** a **KAPACITA_OCH** — zužují **jen realokační** lokace. Obě skupiny se v datech záměrně nepřekrývají; horní KPI a tabulky lokací respektují tyto filtry. Agregované pohledy `metrics_oblast` / `metrics_pobocka` zůstávají bez těchto dimenzí (jen filtry oblast / pobočka v postranním panelu).

**Co znamená filtr Typ (nebo Označení) u tabulek „přetížené“ a „nevyužité“ lokace:**

- Filtr **Typ** / **Označení** odpovídá otázce: *které lokace mají v Přepočítaných kapacitách alespoň jeden řádek s daným typem* (případně s daným označením — pokud je vybrané obojí, musí sedět **oba** údaje na **jednom** řádku přepočtu). U **realokačních** lokací se tyto filtry z přepočtu **nepoužívají** (omezují je jen deskriptor / KAPACITA_OCH ze souboru realokace).
- Tabulky **Nejvíce přetížené lokace** a **Nejvíce nevyužité lokace** stále pracují s **jednou řádkou na lokaci** z `metrics_lokace_enriched`. Sloupce jako přetíženost, naplněnost %, kapacita a stav fondu jsou tedy vždy za **celou lokaci** (součet přes segmenty / OCH oproti celkovému stavu fondu na lokaci) — **ne** za izolovaný řez „jen vybraný Typ“.
- Prakticky: filtr Typ (např. AK) **vybere podmnožinu lokací**, kde je ten typ v přepočtu zastoupený; u každé takové lokace pak vidíte **globální** ukazatele za celou lokaci, nikoli „přetížení jen u segmentu AK“. Pokud byste potřebovali přetížení nebo naplněnost **v řezu jednoho typu**, šlo by o **jiný** pohled (agregace jen přes řádky s daným typem) — ten současný model záměrně neobsahuje.

## Struktura projektu

```
data_raw/           # vstupy
data_processed/     # exporty CSV + DQ report
src/
  config.py         # cesty
  io/               # načítání CSV / Excel
  transform/        # normalizace klíčů
  model/            # DuckDB model, pohledy, ETL
  metrics/          # dokumentační značka (metriky v SQL)
  validation/       # datová kvalita
  ui/               # Streamlit
app.py              # vstupní bod Streamlit
```

## Datový model (výstupy ETL)

- `dim_pobocka`, `dim_lokace`
- `fact_fond`, `fact_kapacita_fyzicka`, `fact_kapacita_realokace`
- Pohledy: `metrics_lokace_enriched`, `metrics_lokace_och`, `metrics_sit`, `metrics_oblast`, `metrics_pobocka`

## Metriky: význam a výpočet (zjednodušeně)

Tento blok je určený k tomu, abyste **stejnými slovy** vysvětlovali čísla kolegům. Technická pravda je v SQL pohledech v `src/model/pipeline.py` (DuckDB).

### Klíč a zdroje

- **Lokace** v modelu = spojení **čísla knihovny (pobočky)** a kódu **`LOKACE_SHORT`** (v kódu ještě normalizovaný tvar pro spolehlivé spojování).
- **Stav fondu (počet svazků)** na lokaci vychází ze zdroje **„Skutečný stav“** (parquet `Skutečný stav*.parquet` nebo CSV export lokací) — jde o **celkový** stav u lokace (agregace v dimenzi lokace).
- **Fyzická / přepočítaná kapacita** po **OCH** vychází z **„Přepočítané kapacity“** (parquet nebo Excel/CSV kapacity) — řádky regálů/segmentů se sčítají na úroveň `pobočka + lokace + OCH`.
- **Realokační kapacita** po **OCH** vychází ze **„Skutečný stav - realokace“** (parquet; v legacy režimu z `realokace.csv`). U realokačních lokací je to **plánovaná** kapacita pro daný OCH (v Parquetu z kapacitního plánu pro realokaci).

### Realokační lokace (`je_realokace`)

- V **parquet režimu** je lokace považovaná za realokační, **pokud se její pár (číslo knihovny + LOKACE_SHORT)** vyskytuje v souboru **Skutečný stav - realokace.parquet** (ne z `realokace.csv`).
- V **legacy režimu** se používá `data_raw/realokace.csv` (a případné doplnění podle názvu pobočky).

### Efektivní kapacita na úrovni OCH

Pro každou kombinaci **lokace × OCH**, která má řádek ve fyzické kapacitě:

- Pokud `je_realokace` u lokace **ano** → do výpočtu se bere **realokační** kapacita (`kapacita_realokace`) pro daný OCH (spárovaná s fyzickým řádkem).
- Pokud **ne** → bere se **fyzická** kapacita (`kapacita_fyzicka`) z přepočítaných kapacit.

**Kapacita lokace celkem** = součet těchto **efektivních** kapacit přes všechna OCH u lokace (`kapacita_celkem` v pohledu `metrics_lokace`). Odpovídá tomu, co v reportech označujete jako **přepočítanou / efektivní kapacitu** u lokace (v dashboardu podle Power BI: *Přepočítaná kapacita (tabulka)* u výběru).

### Stav fondu na úrovni OCH vs. celkem

- **Celkový stav** na lokaci (`stav_fondu_celkem`) je ze zdroje skutečného stavu fondu (jedna hodnota na lokaci).
- **Stav po OCH** (`stav_fondu_och` v `metrics_lokace_och`) vychází z řádků spojených s kapacitou (rozpad podle OCH z ETL — pro detailní srovnání s kapacitou po OCH).

Proto součet stavu po OCH nemusí být číselně stejný jako „celkový“ stav ze systému, pokud se rozpad liší od primárního výkazu — u prezentací **síťových součtů** používejte metriky z `metrics_lokace_enriched` / `metrics_sit`, u **rozpadu na OCH** pohled `metrics_lokace_och`.

### Odvozené veličiny na lokaci (`metrics_lokace_enriched`)

| Koncept | Jak se počítá |
|--------|----------------|
| **Zbývající kapacita** (`volna_kapacita`) | `kapacita_celkem − stav_fondu_celkem` (může být záporná = přetlak). |
| **% naplnění** (`naplnenost_pct`) | `(stav_fondu_celkem / kapacita_celkem) × 100`, jen pokud je `kapacita_celkem` známá a nenulová; jinak NULL. |
| **Rozdíl** (`rozdil`) | `stav_fondu_celkem − kapacita_celkem` (stejná informace jako u „zbývající“, se znaménkem opačným vůči volné kapacitě). |
| **Přetížená** | `naplnenost` > 100 %. |
| **Riziková** | `naplnenost` > 90 % (informační práh v modelu). |

### Agregace: pobočka, oblast, celá síť

- **Součty** stavu a kapacity jsou **sčítáním přes lokace** v daném řezu (pobočka, oblast, nebo celá síť).
- **% naplnění** na agregované úrovni (`metrics_oblast`, `metrics_pobocka`, `metrics_sit`) je **vážený poměr**:  
  `sum(stav u lokací, kde je kapacita) / sum(kapacita) × 100`, nikoli průměr procent z jednotlivých lokací.
- **Stav při pokrytí kapacitou** (`stav_pri_pokryti_kapacitou`) = součet stavu fondu **jen u lokací, které mají definovanou kapacitu** — používá se u síťového KPI, aby čitatel i jmenovatel u podílu odpovídaly stejné množině.

### Dashboard a názvy jako v Power BI

České popisky ve Streamlit aplikaci (`src/ui/dashboard_labels.py`) kopírují report **Kapacity v MKP**. Mapování:

| Popisek na dashboardu | Technický sloupec / pohled |
|------------------------|----------------------------|
| Přepočítaná kapacita (tabulka) | `kapacita_celkem` (součet efektivní kapacity) |
| Skutečný stav (počet svazků) | `stav_fondu_celkem` |
| % Naplnění | `naplnenost_pct` (u filtru lokací: poměr součtů; u celé sítě stejná logika jako výše) |
| Zbývající kapacita | `volna_kapacita` |
| Celková kapacita sítě / Skutečný stav všech přítomných svazků | řádek z `metrics_sit` |

## Rozšíření (návrh)

- Scénáře optimalizace realokace podle cílových kapacit
- Polars pro větší objemy
- Přímé napojení na databázi místo CSV exportů
