# Metriky dashboardu — podrobný popis pro tým

Tento dokument vysvětluje, **co jednotlivé čísla v dashboardu znamenají**, **jak se počítají** a **jak je interpretovat**.  
Slouží pro interní školení a pro odpovědi na otázky typu „proč se mi podíl piktogramu neshoduje s filtrem deskriptoru“.

> **Technická implementace:** `src/model/pipeline.py` (DuckDB views), `src/config.py` (whitelist piktogramů), `src/ui/dashboard.py` (zobrazení).  
> **Exporty:** `data_processed/exports/metrics_share_*.csv` po spuštění ETL.

---

## 1. Datové zdroje a „zrnitost“ řádku

Dashboard propojuje tři hlavní exporty MKP (typicky jako `*.parquet` v kořeni projektu):

| Zdroj | Soubor (typicky) | Co reprezentuje jeden řádek |
|--------|------------------|------------------------------|
| Přepočítané kapacity | `kapacity` / přepočet | Pobočka × lokace × OCH × označení × typ … |
| Skutečný stav (fond) | `skutečný stav` | Stav fondu na lokaci (agregace po OCH v detailu) |
| **Skutečný stav — realokace** | `skutečný stav - realokace` | **Pobočka × lokace × OCH × KAPACITA_DESKRIPTOR** s kapacitou plánovanou pro realokaci |

Pro **podíly OCH** a **podíly piktogramů** je rozhodující soubor **realokace**.  
Kapacita na řádku realokace = sloupec fyzické kapacity v exportu, v modelu pojmenovaný `kapacita_realokace` (počet svazků / míst na regálu dle exportu).

**Realokační lokace** (`je_realokace = Ano`) = lokace, která se vyskytuje v souboru realokace (shoda pobočka + kód lokace). Ostatní lokace jedou z přepočtu (fyzická kapacita).

---

## 2. Horní KPI (filtr vs. celá síť)

### 2.1 Horní řada — „aktuální výběr“

Po aplikaci filtrů v sidebaru (oblast, pobočka, lokace, OCH, realokace ano/ne, označení, typ, deskriptor …):

| Metrika v UI | Výpočet |
|--------------|---------|
| Přepočítaný kapacitní plán | Součet `kapacita_fyzicka_sum` po vybraných lokacích |
| Aktuální kapacitní plán | Součet `kapacita_realokace_sum` po vybraných lokacích |
| Skutečný stav | Součet `stav_fondu_celkem` |
| Naplněná kapacita (%) | `Skutečný stav / Přepočítaný plán × 100` (jen lokace s definovanou fyzickou kapacitou) |
| Zbývající kapacita | `Přepočítaný plán − Skutečný stav` |
| Počet lokací | Počet řádků v `metrics_lokace` po filtrech |

### 2.2 Blok „Celá síť — referenční KPI“

Stejné metriky, ale **bez UI filtrů** — referenční baseline pro porovnání s výběrem.

### 2.3 Přetížená / Riziková

Na úrovni **jedné lokace**:

- **Riziková** = naplnění > 90 %
- **Přetížená** = naplnění > 100 % (zároveň tedy i riziková)

---

## 3. Podílové pohledy — společná logika

V dashboardu jsou tři sekce: **OCH**, **Typ**, **Piktogramy**.  
Všechny používají stejnou strukturu tabulky odchylek poboček:

| Sloupec | Význam |
|---------|--------|
| **Kapacita … (svazky)** | Čitatel — součet kapacity dané kategorie na pobočce |
| **Podíl (%)** | Podíl na **pobočce** |
| **Podíl síť (%)** | Podíl v **celé síti** (stejná kategorie, stejný vzorec) |
| **Δ (pp)** | `Podíl (%) − Podíl síť (%)` v **procentních bodech** |

**Interpretace Δ:**  
- Δ > 0 → na pobočce je daná kategorie **silnější** než průměr sítě.  
- Δ < 0 → na pobočce je **slabší** než průměr sítě.

**Důležité:** Podílové tabulky **nereagují na filtry** v sidebaru. Filtry mění KPI a tabulky lokací, ne `metrics_share_*`.  
(Pokud v budoucnu potřebujete podíly „jen pro vybranou oblast“, muselo by se to doplnit zvlášť.)

---

## 4. Podíl OCH (kapacita realokace)

### Graf „Kapacitní plán vs. skutečný stav“ (nový)

Pro každé OCH ve **Skutečný stav - realokace** (síť, top 30 podle kapacity):

| Sloupec / série | Zdroj | Význam |
|-----------------|--------|--------|
| Kapacitní plán (realokace) | `SUM(kapacita_realokace)` po OCH | Plánovaná kapacita realokovatelných regálů |
| Skutečný stav (svazky) | `SUM(stav_na_regalu)` po OCH | Aktuální počet svazků na regálu (stejný export) |

**Přepínač zobrazení:**

- **Svazky** — absolutní součty (doporučeno pro porovnání plán vs. stav).
- **Podíl v síti (%)** — každá série jako podíl na součtu všech OCH v síti (paralelně k původnímu podílovému grafu).

**Naplněnost** u vybraného OCH: `SUM(stav) / SUM(kapacita realokace) × 100` — může být nad 100 % (bez koeficientu 60–70 % z přepočtu).  
Pohledy: `metrics_och_realok_sit`, `metrics_och_realok_pobocka`.

### Vzorec podílu (tabulka odchylek poboček)

```
Podíl OCH (%) = SUM(kapacita_realokace pro dané OCH) / SUM(kapacita_realokace všech OCH) × 100
```

- **Síť:** jmenovatel = celá realokace v síti.  
- **Pobočka:** jmenovatel = celá realokace dané pobočky.  
- Agregace podílů: `metrics_share_och_*` (z `metrics_lokace_och`).

### Příklad ze sítě (aktuální data)

| OCH | Kapacita realokace | Podíl v síti |
|-----|-------------------:|-------------:|
| A1 | 157 756 | 28,13 % |
| (prázdné) | 54 200 | 9,66 % |
| D | 33 555 | 5,98 % |

Součet podílů přes všechna OCH na pobočce / v síti by měl být **100 %** (každý řádek realokace má přiřazené OCH nebo prázdnou hodnotu, která se do součtu započítá).

### Co OCH podíl **neříká**

- Není to podíl na fyzickém přepočtu.  
- Není to počet regálů — váží se **kapacitou ve svazcích** z realokace.

---

## 5. Podíl Typ (fyzická kapacita z přepočtu)

### Vzorec

```
Podíl Typ (%) = SUM(kapacita_fyzicka pro daný Typ) / SUM(kapacita_fyzicka všech Typů) × 100
```

Zdroj: **přepočítané kapacity** (`kapacita_raw`), sloupec `Typ` — **ne** realokace, **ne** OCH písmena.

### Příklad

| Typ | Podíl v síti (orientačně) |
|-----|---------------------------|
| Doporučujeme (dospělí) | ~0,69 % fyzické kapacity v síti* |
| Doporučujeme dětem | ~0,21 % |

\*Konkrétní číslo závisí na dávce parquet; v exportu `metrics_share_typ_sit.csv`.

### Typ vs. OCH

V datech to **nejsou totéž**: `Typ` může být např. „Beletrie - dospělí“, zatímco OCH je písmeno (A1, D, …). Porovnávejte je odděleně.

---

## 6. Podíl piktogramů (kapacita realokace) — nejdůležitější pro interpretaci

### 6.1 Co je piktogram v modelu

Whitelist **16 kódů** v `src/config.py` (`PIKTOGRAMY`).  
Z řádku realokace se kód bere z **`KAPACITA_DESKRIPTOR`** funkcí `extract_piktogram_code()`:

- přesná shoda s kódem (např. `MÁG`), nebo  
- text obsahující „piktogram“, např. `E.T. (piktogram)` → kód **E.T.**

**Nerozpozná se** mimo jiné:

- prázdný deskriptor, „komiksy“, „ost.cizi“, …  
- kombinovaný zápis **`MÁG, E.T. (piktogram)`** — parser vezme prefix před `(`, který není jedním kódem z whitelistu.

### 6.2 Vzorec (aktuální verze)

```
Podíl piktogramu (%) =
    SUM(kapacita_realokace řádků s daným piktogramem)
    / SUM(kapacita_realokace VŠECH řádků realokace na pobočce nebo v síti)
    × 100
```

- **Čitatel:** jen řádky, kde šel deskriptor namapovat na jeden z 16 kódů.  
- **Jmenovatel:** **celá** kapacita realokace pobočky / sítě (`fact_realokace_pobocka_total`), stejná logika jako u OCH.

**Součet podílů všech 16 piktogramů obvykle nedává 100 %** — většina realokace nemá v deskriptoru rozpoznatelný piktogram.  
V aktuální dávce: součet podílů piktogramů v síti ≈ **11,2 %** (zbytek jsou jiné / neparsovatelné deskriptory).

### 6.3 Proč dříve vznikaly „divné“ procenta (piktogramový pool)

Dříve byl jmenovatel jen součet kapacit **rozpoznaných** piktogramů („piktogramový pool“).  
Pak např. E.T. v síti vycházelo **~10,7 %** (podíl mezi piktogramy), což vypadalo jako velký obor, ale ve skutečnosti šlo o podíl **uvnitř malé části** realokace.

**Nově** je E.T. v síti **~1,2 % celé realokace** — srovnatelné s tím, „kolik regálové kapacity v síti je označené jako sci-fi piktogram“.

### 6.4 Rozšířený příklad: E.T. na pobočce Hostivař

**Vstupní řádky (zjednodušeně):**

| Lokace | OCH | KAPACITA_DESKRIPTOR | kapacita_realokace |
|--------|-----|---------------------|-------------------:|
| HOS-BELDO | A1 | E.T. (piktogram) | **80** |
| … | … | MILENCI (piktogram) | 330 |
| … | … | PISTOLE (piktogram) | 290 |
| … | … | další piktogramy | … |
| … | … | komiksy / prázdné / jiné | … |

**Součty:**

| Položka | Hodnota |
|---------|--------:|
| Kapacita E.T. na Hostivaři (čitatel) | **80** |
| Celá realokace Hostivaře (jmenovatel) | **8 356** |
| Kapacita všech rozpoznaných piktogramů na Hostivaři (jen pro kontext) | 1 201 |

**Výpočet v tabulce:**

```
Podíl (%)     = 80 / 8 356 × 100  ≈ 0,96 %
Podíl síť (%) = 6 817 / 567 070 × 100 ≈ 1,20 %   (E.T. v celé síti)
Δ (pp)        ≈ 0,96 − 1,20 ≈ −0,24 pp
```

**Čtení:** Na Hostivaři je E.T. o něco **slabší** než průměr sítě, ale rozdíl je malý (−0,24 pp) — ne −4 pp jako při starém jmenovateli „jen piktogramy“.

### 6.5 Příklad: největší piktogramy v síti (podíl z celé realokace)

| Piktogram | Kapacita (síť) | Podíl z celé realokace |
|-----------|---------------:|-----------------------:|
| PISTOLE | 18 162 | 3,20 % |
| MÁG | 12 505 | 2,21 % |
| MILENCI | 9 875 | 1,74 % |
| E.T. | 6 817 | 1,20 % |

### 6.6 Časté nedorozumění: filtr KAPACITA_DESKRIPTOR

| Filtr deskriptoru v sidebaru | Tabulka podílů piktogramů |
|------------------------------|---------------------------|
| Vybere **lokace**, které mají někde řádek s daným deskriptorem | Počítá **součet kapacity** řádků s piktogramem / celou realokaci |
| Nezáleží na váze kapacity lokace | Váží každý řádek realokace kapacitou |
| Může ukázat 69 lokací s textem „E.T.“ v síti | Hostivař má jen **80** svazků E.T. z **8 356** celkové realokace pobočky |

Filtr tedy odpovídá na otázku **„kde to je“**, tabulka na **„kolik to zabírá kapacity“**.

---

## 7. Schéma toku dat (piktogramy)

```mermaid
flowchart TD
  A[Skutečný stav - realokace.parquet] --> B[Řádek: pobočka × lokace × OCH × deskriptor]
  B --> C{extract_piktogram_code deskriptor}
  C -->|ano| D[Čitatel: kapacita_realokace do piktogramu]
  C -->|ne| E[Do čitatele piktogramu nejde]
  B --> F[Jmenovatel: všechny řádky → sum po pobočce / síti]
  D --> G[podil_pct = čitatel / jmenovatel × 100]
  F --> G
```

---

## 8. Exporty a kontrola kvality

Po ETL (`run_etl` / build modelu) vzniknou mimo jiné:

| Soubor | Obsah |
|--------|--------|
| `metrics_share_och_sit.csv` | Podíly OCH — síť |
| `metrics_share_och_pobocka.csv` | Podíly OCH — pobočky + Δ |
| `metrics_share_typ_sit.csv` | Podíly Typ — síť |
| `metrics_share_typ_pobocka.csv` | Podíly Typ — pobočky + Δ |
| `metrics_share_piktogram_sit.csv` | Podíly piktogramů — síť |
| `metrics_share_piktogram_pobocka.csv` | Podíly piktogramů — pobočky + Δ |

Report **`data_processed/data_quality_report.md`** obsahuje součty kapacity podle OCH/Typ/piktogramu a u piktogramů poznámku k jmenovateli podílu.

---

## 9. FAQ pro diskuzi v týmu

**Proč se podíly piktogramů nesčítají na 100 %?**  
Protože jmenovatel je celá realokace, ale čitatel jen 16 rozpoznaných kódů. Zbytek jsou jiné deskriptory.

**Je podíl piktogramu stejný jako podíl OCH?**  
Ne. OCH člení **všechnu** realokaci; piktogram jen řádky s parsovatelním deskriptorem. Stejný regál může mít OCH `A1` a deskriptor `komiksy` (bez piktogramu).

**Můžu filtrovat podíly podle oblasti?**  
V aktuálním UI ne — podíly jsou vždy z celé sítě / všech poboček v dávce.

**Kde se bere kapacita u realokace?**  
Ze sloupce fyzické kapacity v parquet realokace, v modelu `kapacita_realokace`.

**Co když se změní parquet?**  
Streamlit cache se invaliduje podle otisku souborů; po změně dat obnovte aplikaci / redeploy.

---

## 10. Historie změny (piktogramy)

| Verze logiky | Jmenovatel podílu piktogramu |
|--------------|----------------------------|
| Původní (do úpravy) | Součet kapacit **jen rozpoznaných piktogramů** (mix piktogramů = 100 %) |
| **Aktuální** | Součet **celé realokace** pobočky / sítě (srovnatelné s OCH) |

Při vysvětlování starších screenshotů nebo exportů počítejte s tímto rozdílem.

---

*Poslední aktualizace dokumentu: v souladu s implementací v `pipeline.py` (jmenovatel `fact_realokace_pobocka_total`).*
