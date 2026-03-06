#!/bin/bash
# Podcast Digest - duplo-clique para abrir
cd "$(dirname "$0")"
source venv/bin/activate
echo ""
echo "  Abrindo Podcast Digest..."
echo "  (feche esta janela do Terminal para encerrar)"
echo ""
open "http://localhost:8501" &
streamlit run podcast_digest/app.py --server.headless true
