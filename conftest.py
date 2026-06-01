import sys
from pathlib import Path

# Ensure the repo root is importable so `import hivemind` works during tests
# even without an editable install.
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
