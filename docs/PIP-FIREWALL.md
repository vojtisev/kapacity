# Pip a firemní síť (timeout na pypi.org)

Chyba typu `Connection to pypi.org timed out` znamená, že z vašeho PC **nejde spolehlivě na veřejný Python Package Index** — časté u **firemních sítí**, kde je potřeba **proxy**, nebo kde **blokují HTTPS** ven z organizace.

V tomto repozitáři je pro síť MKP připraven soubor **`set_proxy_mlp.bat`** (natvrdo `HTTP_PROXY` / `HTTPS_PROXY` podle PAC). Volá ho **`setup_venv.bat`**, **`run_dashboard.bat`** a **`run_etl.bat`** — není potřeba nic nastavovat ručně. Mimo MKP síť upravte nebo dočasně vyjměte řádek `call ... set_proxy_mlp.bat` z těchto skriptů, případně obsah `set_proxy_mlp.bat`.

## 1. Zjistěte proxy od IT

Často existuje **HTTP/HTTPS proxy** (např. `http://proxy.firma.cz:8080`). Pak v **CMD** před `setup_venv.bat` nastavte (hodnoty dostanete od správců):

```bat
set HTTP_PROXY=http://uzivatel:heslo@proxy.firma.cz:8080
set HTTPS_PROXY=http://uzivatel:heslo@proxy.firma.cz:8080
setup_venv.bat
```

Nebo trvale v uživatelském souboru **`%APPDATA%\pip\pip.ini`**:

```ini
[global]
proxy = http://proxy.firma.cz:8080
```

*(Přihlašování do proxy záleží na politice firmy — někde stačí adresa bez hesla v souboru.)*

### WPAD / soubor `wpad.dat` (např. `http://wpad.mlp.cz/wpad.dat`)

Adresa **`http://wpad.mlp.cz/wpad.dat`** není sama o sobě **proxy server**, ale **PAC skript** (Proxy Auto-Configuration): malý „program“, který prohlížeči řekne, pro které adresy použít který `PROXY host:port`.

- **`pip` PAC neumí** — potřebuje konkrétní tvar `http://jmeno-serveru:port` (případně uživatelské jméno a heslo, pokud to vaše proxy vyžaduje).

**Postup:**

1. **Zjistěte skutečnou adresu proxy z PAC** (jedna z možností):
   - Otevřete v prohlížeči `http://wpad.mlp.cz/wpad.dat`, v textu hledejte řetězce typu `PROXY proxy.mlp.cz:8080` nebo `return "PROXY ..."`.
   - Nebo v **CMD** (na PC ve firemní síti):  
     `curl -s http://wpad.mlp.cz/wpad.dat`  
     a v výstupu najděte `PROXY` a port.
   - Nejiste-li si výběrem (v PAC bývá víc pravidel), **ověřte u IT** přesný **HTTP/HTTPS proxy** pro nástroje z příkazové řádky.

2. **Nastavte pip** (příklad, pokud v PAC je např. `PROXY proxy.mlp.cz:8080`):

   Soubor **`%APPDATA%\pip\pip.ini`** (složka vytvoříte, pokud chybí):

   ```ini
   [global]
   proxy = http://proxy.mlp.cz:8080
   ```

   Nebo jednorázově v CMD před `setup_venv.bat`:

   ```bat
   set HTTP_PROXY=http://proxy.mlp.cz:8080
   set HTTPS_PROXY=http://proxy.mlp.cz:8080
   setup_venv.bat
   ```

   *(Hodnotu hostu a portu **nahraďte** tím, co odpovídá vašemu PAC / pokynům IT.)*

3. **Windows může PAC používat pro celý systém** — *Nastavení → Síť a Internet → Proxy → Ruční nastavení proxy / Skript nastavení* a jako URL skriptu zadejte `http://wpad.mlp.cz/wpad.dat` (pokud to u vás správci doporučují). To pomůže **prohlížeči**, ale **pip často stejně potřebuje** explicitní `pip.ini` nebo proměnné výše.

4. **Ověření:** po nastavení spusťte `pip install --upgrade pip` v aktivovaném `.venv` — pokud projde bez timeoutu, je proxy pro PyPI v pořádku.

**Poznámka:** Pokud proxy vyžaduje **doménové přihlášení (NTLM)** a pip ho nevezme, řeší se to někdy nástrojem typu **Cntlm** nebo konfigurací od IT — opět se vyplatí krátká konzultace se správci.

## 2. Dočasně jiná síť

Jednorázově stáhnout balíky z **domácí WiFi** nebo **mobilního hotspotu** (po souhlasu bezpečnostní politiky), spustit `setup_venv.bat`, pak pracovat zase z kanceláře — už nainstalované balíky v `.venv` zůstanou.

## 3. Instalace z jiného počítače (bez přímého PyPI z pracovního PC)

Na počítači **s internetem** (stejná verze Pythonu):

```bat
mkdir wheels
pip download -r requirements.txt -d wheels
```

Složku **`wheels`** zkopírujte do projektu a na cílovém PC:

```bat
call .venv\Scripts\activate.bat
pip install --no-index --find-links=wheels -r requirements.txt
```

*(Nejdřív musíte mít vytvořené `.venv` — `python -m venv .venv` — i bez síťové instalace.)*

## 4. Trusted host (jen někdy pomůže u SSL)

Někdy IT doporučí:

```bat
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt
```

Timeout to **nevyřeší**, ale u některých SSL problémů ano.

---

**Shrnutí:** chyba není v projektu KAPACITY, ale v **cestě ven na internet** z vaší sítě. Nejčistší řešení je **proxy od správců** nebo **jednorázová instalace z jiné sítě / z balíků `wheels`**.
