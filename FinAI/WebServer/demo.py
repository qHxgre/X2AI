import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from WebServer.app import create_app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True)