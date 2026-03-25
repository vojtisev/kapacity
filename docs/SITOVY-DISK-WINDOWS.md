# Spuštění projektu ze síťového disku (Windows)

Projekt je připravený tak, aby šel mít na **OneDrive / síťové složce** i na **lokálním disku** a spouštět dvojklikem z Windows — stejný princip jako u souvisejících interních nástrojů (např. projekt **VYRAZOVANI**): celá složka se zkopíruje, uvnitř běží `setup_venv.bat` a pak `run_dashboard.bat` / `run_etl.bat`.

## Doporučený postup (zkopírování na lokální disk)

1. **Zkopírujte celý adresář** `KAPACITY` z OneDrive nebo ze síťové cesty na **lokální disk** (např. `C:\Projekty\KAPACITY`).
2. Otevřete tuto **lokální** složku v Průzkumníkovi a spusťte **`setup_venv.bat`** (jednou na stanici nebo po aktualizaci `requirements.txt`).
3. Pak **`run_dashboard.bat`** nebo **`run_etl.bat`**.

**Proč lokální kopie:** virtuální prostředí (`.venv`), `pip` a někdy i Streamlit na dlouhých cestách UNC nebo na synchronizovaném OneDrive selhávají nebo jsou velmi pomalé. Lokální kopie je nejspolehlivější varianta.

**Přímo ze sítě:** lze zkusit spouštění z mapovaného disku (`Z:\…`) nebo UNC (`\\server\…`); pokud něco selže, přejděte na lokální kopii podle bodů výše.

## Požadavky na stanici

- **Python 3.11+** z [python.org](https://www.python.org/downloads/) (při instalaci zaškrtnout **Add python.exe to PATH**).
- Na Windows je výhodný **Python Launcher** (`py -3`) — bývá u instalace z python.org.

## První jednorázová příprava (na každé stanici, nebo jednou na sdíleném místě)

1. Zkopírujte celou složku projektu na síťový disk (včetně `data_raw/`, Parquet v kořeni, pokud je používáte).
2. Otevřete složku v Průzkumníkovi a **dvojklikem** spusťte **`setup_venv.bat`**.
   - Vytvoří se lokální virtuální prostředí **`.venv`** ve složce projektu.
   - Nainstalují se závislosti z `requirements.txt`.
   - Na **pomalém síťovém úložišti** může první instalace trvat déle.

> **Poznámka:** Pokud antivirová politika brání spouštění skriptů z UNC cesty, použijte **mapované písmeno disku** (např. `Z:\KAPACITY`) místo `\\server\…`.

## Běžné použití

| Soubor | Účel |
|--------|------|
| **`run_dashboard.bat`** | Spustí Streamlit dashboard v prohlížeči. |
| **`run_etl.bat`** | Spustí ETL (`python -m src.model.pipeline`) a vygeneruje exporty. |

Všechny `.bat` soubory nejdřív přepnou adresář na složku, kde leží (funguje i ze síťového disku).

## Data

- **Parquet** v kořeni projektu a složka **`data_raw/`** musí na síťovém disku **fyzicky být** (git je neobsahuje).
- Po aktualizaci exportů znovu spusťte **`run_etl.bat`**, pokud chcete přepočítat `data_processed/`.

## Omezení a tipy

- **Více uživatelů** současně: jeden sdílený `.venv` obvykle funguje, ale při problémech může každý uživatel spustit **`setup_venv.bat`** do vlastní kopie složky, nebo mít venv jen na „své“ stanici s kopií projektu.
- **Délka cesty:** hluboké cesty na UNC někdy způsobí potíže — držte projekt v kratší cestě (mapovaný disk pomáhá).
- **Streamlit** otevře prohlížeč na `localhost`; dashboard běží jen na daném PC (není to centrální webový server pro celou síť bez dalšího nastavení).

## Když okno hned zmizí nebo „nic neudělá“

1. **Otevřete CMD ručně** (Win+R → `cmd` → Enter), přejděte do složky a spusťte příkaz znovu — uvidíte celý výpis:
   ```bat
   cd /d Z:\KAPACITY
   setup_venv.bat
   ```
   *(Cestu `Z:\KAPACITY` nahraďte svou — může být i `\\server\sdilene\KAPACITY`.)*

2. **Python v PATH** — v CMD zkuste `py -3 --version` nebo `python --version`. Když to hlásí „není rozpoznán příkaz“, nainstalujte Python a zaškrtněte **Add to PATH**.

3. **UNC vs. mapovaný disk** — vytvoření `venv` na síťové cestě `\\server\…` někdy selže (oprávnění, antivirus). Zkuste **zkopírovat projekt na `C:\Projekty\KAPACITY`** a spustit skripty odtud.

4. Staré skripty používaly `chcp 65001` a někdy **špatně vyhodnocený `%errorlevel%`** — aktuální `.bat` soubory to opravují a vždy na konci **nechají okno otevřené** (`pause`), dokud nestisknete klávesu.

5. **`Connection to pypi.org timed out`** při `pip` — firemní síť / proxy. Postup: [PIP-FIREWALL.md](PIP-FIREWALL.md).
