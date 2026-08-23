"""
Configuración común para tests con pytest.
"""

import sys
import os
from pathlib import Path

# Añadimos el directorio raíz del proyecto al sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
