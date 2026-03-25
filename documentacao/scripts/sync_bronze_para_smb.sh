#!/bin/zsh

set -euo pipefail

BASE_DIR="/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios"
SOURCE_DIR="$BASE_DIR/bronze/"
DEST_DIR="/Volumes/VRCIB_DS_000123/RECEITA_FEDERAL/GT_03_AVALIACAO_EM_MASSA/04_BRONZE/variaveis_municipios /"

EXCLUDES=(
  "--exclude=.DS_Store"
  "--exclude=__pycache__/"
)

if [[ "${1:-}" == "--dry-run" ]]; then
  echo "Executando rsync em modo simulacao..."
  rsync -avhn "${EXCLUDES[@]}" "$SOURCE_DIR" "$DEST_DIR"
else
  echo "Sincronizando bronze/ para o volume SMB..."
  rsync -avh "${EXCLUDES[@]}" "$SOURCE_DIR" "$DEST_DIR"
fi
