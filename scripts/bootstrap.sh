#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

mkdir -p data/raw data/schema data/dead_letter

if [[ ! -f .env ]]; then
  cp config/env.example .env
fi

echo "Bootstrap complete."
echo "Activate with: source .venv/bin/activate"
