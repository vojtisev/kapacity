"""
Spuštění dashboardu (samostatný příkaz po ETL):

    python3 -m streamlit run app.py

ETL a export (CSV + DQ report):

    python3 -m src.model.pipeline
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ui.dashboard import main

if __name__ == "__main__":
    main()
