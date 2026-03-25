# Report datové kvality — kapacity fondu

Vygenerováno automaticky po ETL.

### Překryv zdrojů: Skutečný stav vs realokace (Parquet)
Klíč: **pobočka (číslo knihovny) + LOKACE_SHORT (normalizovaný)**.

| | Počet unikátních klíčů |
|---|--:|
| Pouze v „Skutečný stav.parquet“ (ostatní lokace) | **664** |
| Pouze v „Skutečný stav - realokace.parquet“ (bez řádku ve fondu) | **1** |
| V obou souborech (průnik) | **271** |

Celkem klíčů ve fondu: **935**, v realokaci: **272**.

### Integrace Sklady.parquet
Řádků po vyčištění ve zdroji: **20**.

- Nově přidané skladové klíče: **1**
- Ignorované klíče (už existovaly v Přepočítaných kapacitách): **19**

### Oblast z kapacitní tabulky (varování)
- pobocka_cislo=45: více různých oblastí v datech kapacity — ['Jih', 'Sklad'], použit modus.
- pobocka_cislo=56: více různých oblastí v datech kapacity — ['Jihovýchod', 'Sklad'], použit modus.

### Oblasti (mapa poboček)
Kanonické oblasti: Ústřední knihovna, Jih, Sklad, Jihozápad, Jihovýchod, Středozápad, Severovýchod.

Počet poboček s hodnotou mimo kanon (kromě „Neurčeno“): **0**.

### Duplicity lokace_id
Počet řádků s duplicitním lokace_id: **0**.

### Duplicity (pobocka_cislo + lokace_short)
Počet řádků: **0** (více záznamů se stejným klíčem — při joinu se použije min(lokace_id)).

### Lokace bez kapacitních řádků (ze souboru kapacity)
Lokací ve fondu bez shody v kapacitní tabulce: **398** (kapacita_celkem bude NULL, pokud neexistuje ani realokační řádek).

### Kapacitní řádky bez shody ve fondu (lokace master)
Počet: **76**.

### Lokace bez jakékoli efektivní kapacity (pohled ETL)
Počet: **392**.

### Lokace se stavem fondu 0 nebo NULL
Počet: **183** (informační — může být legální).

### Realokační lokace s odlišnou fyzickou a realokační kapacitou (součty podle OCH)
Počet lokací s rozdílem: **1** (očekáváno — rozhoduje realokační hodnota).

### Pokrytí kapacitními daty
Lokací celkem: **963**, s nenulovou / definovanou efektivní kapacitou: **571**.

### Lokace bez čísla pobočky (KNODDEL)
Počet: **28**.

---

## Poznámky

- Chybějící kapacita se **nepovažuje za nulu** — metriky `naplnenost_pct` jsou NULL.
- U realokačních lokací platí **kapacita_realokace**; fyzická kapacita slouží jen k porovnání v tomto reportu.
