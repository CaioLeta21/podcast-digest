"""Streamlit GUI for podcast-digest."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import requests
import streamlit as st
import yaml

# Project root (one level up from this file)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.yaml"
ENV_PATH = PROJECT_ROOT / ".env"
PROFILE_PATH = PROJECT_ROOT / "profile.md"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config_raw() -> dict:
    """Load raw config.yaml (without env injection)."""
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def save_config_raw(config: dict):
    """Save config dict back to config.yaml."""
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def load_env() -> dict:
    """Load .env file as dict."""
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                env[key.strip()] = value.strip()
    return env


def save_env(env: dict):
    """Save env dict to .env file."""
    lines = []
    lines.append("# Podcast Digest - Environment Variables")
    lines.append("# (gerado automaticamente pela interface)")
    lines.append("")
    for key, value in env.items():
        if value:
            lines.append(f"{key}={value}")
    ENV_PATH.write_text("\n".join(lines) + "\n")


def load_profile() -> str:
    if PROFILE_PATH.exists():
        return PROFILE_PATH.read_text(encoding="utf-8")
    return ""


def save_profile(text: str):
    PROFILE_PATH.write_text(text, encoding="utf-8")


def resolve_youtube_channel(url_or_input: str) -> Tuple[Optional[str], Optional[str]]:
    """Given a YouTube channel URL, return (channel_id, channel_name).

    Accepts:
      - https://www.youtube.com/@handle
      - https://youtube.com/@handle
      - https://www.youtube.com/channel/UCxxxxx
      - https://www.youtube.com/c/ChannelName
      - A raw channel ID starting with UC
    """
    text = url_or_input.strip().rstrip("/")

    # Already a raw channel ID
    if re.match(r"^UC[\w-]{22}$", text):
        return text, None

    # Direct /channel/ URL - ID is right there
    m = re.search(r"youtube\.com/channel/(UC[\w-]{22})", text)
    if m:
        return m.group(1), None

    # For @handle, /c/, or any other URL, fetch the page and extract
    if "youtube.com" not in text:
        text = f"https://www.youtube.com/{text}" if text.startswith("@") else text

    try:
        resp = requests.get(text, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }, cookies={"CONSENT": "YES+1"})
        resp.raise_for_status()
        html = resp.text

        # Extract channel ID (try multiple patterns YouTube uses)
        channel_id = None
        for pattern in [
            r'"channelId"\s*:\s*"(UC[\w-]{22})"',
            r'"externalId"\s*:\s*"(UC[\w-]{22})"',
            r'"browse_id"\s*:\s*"(UC[\w-]{22})"',
            r'youtube\.com/channel/(UC[\w-]{22})',
            r'(UC[\w-]{22})',
        ]:
            m = re.search(pattern, html)
            if m:
                channel_id = m.group(1)
                break

        if not channel_id:
            return None, None

        # Extract channel name (try multiple patterns)
        channel_name = None
        for pattern in [
            r'"ownerChannelName"\s*:\s*"([^"]+)"',
            r'"author"\s*:\s*"([^"]+)"',
            r'<title>([^<]+)</title>',
        ]:
            m = re.search(pattern, html)
            if m:
                name = m.group(1).strip()
                # Clean up <title> format: "ChannelName - YouTube"
                name = re.sub(r'\s*-\s*YouTube$', '', name)
                if name and name.lower() != "home":
                    channel_name = name
                    break

        return channel_id, channel_name

    except Exception:
        return None, None


def get_db():
    """Get database instance."""
    try:
        from .config import load_config
        from .database import Database
    except ImportError:
        from podcast_digest.config import load_config
        from podcast_digest.database import Database
    # Load env vars from .env for config
    env = load_env()
    for k, v in env.items():
        os.environ.setdefault(k, v)
    config = load_config(str(CONFIG_PATH))
    return Database(config["database"]["path"]), config


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def page_dashboard():
    st.header("Dashboard")

    try:
        db, config = get_db()
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return

    digests = db.get_recent_digests(days=7)

    if not digests:
        st.info(
            "Nenhum digest ainda. Vá na aba **Rodar Digest** para executar "
            "o primeiro, ou aguarde a execução automática das 7h."
        )
        return

    # Stats
    col1, col2, col3 = st.columns(3)
    high = [d for d in digests if d["relevance_score"] >= 7]
    med = [d for d in digests if 4 <= d["relevance_score"] < 7]
    low = [d for d in digests if d["relevance_score"] < 4]

    col1.metric("Alta relevância", len(high))
    col2.metric("Moderada", len(med))
    col3.metric("Baixa", len(low))

    st.divider()

    # Episode cards
    for d in digests:
        score = d["relevance_score"]
        if score >= 7:
            icon = "🟢"
        elif score >= 4:
            icon = "🟡"
        else:
            icon = "⚪"

        with st.expander(f"{icon} [{score}/10] {d['title']}", expanded=(score >= 7)):
            st.write(d["summary"].replace("$", "\\$"))
            st.caption(f"Canal: {d['channel_id']} | Data: {d['digest_date']}")
            st.markdown(f"[Assistir no YouTube]({d['url']})")


def page_channels():
    st.header("Canais")

    config = load_config_raw()
    channels = config.get("channels", [])

    # Add new channel (first, so it's the main action)
    st.subheader("Adicionar canal")

    url_input = st.text_input(
        "Cole o link do canal do YouTube",
        placeholder="Ex: https://www.youtube.com/@Kurzgesagt",
    )

    if st.button("Adicionar", type="primary"):
        if not url_input:
            st.error("Cole o link do canal.")
        else:
            with st.spinner("Buscando canal..."):
                channel_id, channel_name = resolve_youtube_channel(url_input)

            if not channel_id:
                st.error(
                    "Não consegui encontrar esse canal. "
                    "Verifique se o link está correto."
                )
            elif any(ch["id"] == channel_id for ch in channels):
                st.warning(f"O canal '{channel_name or channel_id}' já está na lista.")
            else:
                name = channel_name or "Canal sem nome"
                channels.append({"id": channel_id, "name": name})
                config["channels"] = channels
                save_config_raw(config)
                st.success(f"Canal '{name}' adicionado!")
                st.rerun()

    # List current channels
    st.divider()
    if channels:
        st.subheader(f"{len(channels)} canais monitorados")
        for i, ch in enumerate(channels):
            col1, col2 = st.columns([6, 1])
            col1.markdown(f"**{ch.get('name', 'Sem nome')}**")
            if col2.button("Remover", key=f"remove_{i}"):
                channels.pop(i)
                config["channels"] = channels
                save_config_raw(config)
                st.rerun()
    else:
        st.info("Nenhum canal adicionado ainda. Cole um link acima para começar.")


def page_profile():
    st.header("Perfil de Interesses")

    st.caption(
        "Descreva seus interesses aqui. A IA usa esse texto para "
        "pontuar a relevância de cada episódio para você."
    )

    current = load_profile()

    new_text = st.text_area(
        "Seu perfil",
        value=current,
        height=400,
        label_visibility="collapsed",
    )

    if st.button("Salvar perfil"):
        save_profile(new_text)
        st.success("Perfil salvo!")


def page_feedback():
    st.header("Feedback")

    st.caption(
        "Dê notas aos episódios para calibrar a IA. "
        "Quanto mais feedback, melhores as recomendações."
    )

    try:
        db, config = get_db()
    except Exception:
        st.info("Configure suas API keys primeiro na aba Configurações.")
        return

    digests = db.get_recent_digests(days=14)

    if not digests:
        st.info("Nenhum episódio para avaliar ainda. Rode o digest primeiro.")
        return

    for d in digests:
        with st.container(border=True):
            col1, col2 = st.columns([5, 1])

            with col1:
                st.markdown(f"**{d['title']}**")
                st.caption(f"AI score: {d['relevance_score']}/10 | {d['digest_date']}")
                with st.expander("Ver resumo"):
                    st.write(d["summary"])

            with col2:
                score = st.selectbox(
                    "Nota",
                    options=[None, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    format_func=lambda x: "---" if x is None else str(x),
                    key=f"score_{d['video_id']}",
                    label_visibility="collapsed",
                )

            if score is not None:
                comment = st.text_input(
                    "Comentário (opcional)",
                    key=f"comment_{d['video_id']}",
                )
                if st.button("Salvar", key=f"save_{d['video_id']}"):
                    try:
                        from .models import FeedbackEntry
                    except ImportError:
                        from podcast_digest.models import FeedbackEntry
                    entry = FeedbackEntry(
                        video_id=d["video_id"],
                        user_score=score,
                        comment=comment,
                    )
                    db.save_feedback(entry)
                    st.success(f"Feedback salvo! (sua nota: {score}/10)")


def page_people():
    st.header("Pessoas Rastreadas")

    st.caption(
        "Rastreie pessoas específicas e encontre automaticamente "
        "entrevistas e podcasts em que elas participam no YouTube."
    )

    config = load_config_raw()
    tracked = config.get("tracked_people", [])

    # Add new person
    st.subheader("Adicionar pessoa")

    col_name, col_queries = st.columns([2, 3])
    with col_name:
        new_name = st.text_input(
            "Nome da pessoa",
            placeholder="Ex: Ray Dalio",
        )
    with col_queries:
        custom_queries = st.text_input(
            "Queries customizadas (opcional, separadas por vírgula)",
            placeholder='Ex: "Ray Dalio" hedge fund, "Ray Dalio" economy',
        )

    if st.button("Adicionar pessoa", type="primary"):
        if not new_name:
            st.error("Digite o nome da pessoa.")
        elif any(p["name"].lower() == new_name.strip().lower() for p in tracked):
            st.warning(f"'{new_name.strip()}' já está na lista.")
        else:
            entry = {"name": new_name.strip()}
            if custom_queries.strip():
                entry["queries"] = [q.strip() for q in custom_queries.split(",") if q.strip()]
            tracked.append(entry)
            config["tracked_people"] = tracked
            save_config_raw(config)

            # Also add to database
            try:
                db, _ = get_db()
                db.add_person(new_name.strip())
            except Exception:
                pass

            st.success(f"'{new_name.strip()}' adicionada!")
            st.rerun()

    # List tracked people
    st.divider()
    if tracked:
        st.subheader(f"{len(tracked)} pessoas rastreadas")

        for i, person in enumerate(tracked):
            col1, col2, col3 = st.columns([4, 4, 1])
            col1.markdown(f"**{person['name']}**")
            if person.get("queries"):
                col2.caption(", ".join(person["queries"]))
            else:
                col2.caption("Queries padrão (interview, podcast)")
            if col3.button("Remover", key=f"remove_person_{i}"):
                try:
                    db, _ = get_db()
                    db.remove_person(person["name"])
                except Exception:
                    pass
                tracked.pop(i)
                config["tracked_people"] = tracked
                save_config_raw(config)
                st.rerun()

        # Recent results
        st.divider()
        st.subheader("Resultados recentes")

        try:
            db, _ = get_db()
            people = db.get_active_people()

            if people:
                for person in people:
                    videos = db.get_person_recent_videos(person["person_id"], limit=5)
                    if videos:
                        st.markdown(f"**{person['name']}** ({len(videos)} vídeos recentes)")
                        for v in videos:
                            duration = f"{v['duration_seconds'] // 60}min" if v.get('duration_seconds') else ""
                            st.markdown(
                                f"- [{v['title']}]({v['url']}) {duration}"
                            )
                    else:
                        st.caption(f"{person['name']}: nenhum resultado ainda")
            else:
                st.info("Nenhum resultado ainda. Clique em 'Buscar agora' abaixo.")
        except Exception:
            st.info("Rode uma busca para ver os resultados.")

        # Search now button
        st.divider()
        if st.button("Buscar agora"):
            st.session_state.person_search_status = "running"

            status_placeholder = st.empty()
            status_placeholder.info("Buscando... isso pode levar alguns segundos.")

            env_vars = load_env()
            run_env = os.environ.copy()
            run_env.update(env_vars)

            try:
                result = subprocess.run(
                    [sys.executable, "-m", "podcast_digest", "scan-people", "--dry-run"],
                    capture_output=True,
                    text=True,
                    cwd=str(PROJECT_ROOT),
                    env=run_env,
                    timeout=120,
                )

                output = result.stdout + result.stderr

                if result.returncode == 0:
                    status_placeholder.success("Busca concluída!")
                else:
                    status_placeholder.error("Erro na busca.")

                with st.expander("Log da busca", expanded=True):
                    st.code(output, language="text")

            except subprocess.TimeoutExpired:
                status_placeholder.error("Timeout: busca levou mais de 2 minutos.")
            except Exception as e:
                status_placeholder.error(f"Erro: {e}")
    else:
        st.info("Nenhuma pessoa rastreada. Adicione alguém acima para começar.")

    # Settings
    st.divider()
    st.subheader("Configurações de busca")

    search_cfg = config.get("person_search", {})
    enabled = st.checkbox("Busca automática habilitada", value=search_cfg.get("enabled", True))
    days_options = {"Segunda": 0, "Terça": 1, "Quarta": 2, "Quinta": 3, "Sexta": 4, "Sábado": 5, "Domingo": 6}
    current_day = search_cfg.get("day_of_week", 0)
    day_name = [k for k, v in days_options.items() if v == current_day]
    day_name = day_name[0] if day_name else "Segunda"

    col1, col2 = st.columns(2)
    with col1:
        selected_day = st.selectbox("Dia da busca automática", options=list(days_options.keys()), index=current_day)
    with col2:
        lookback = st.number_input("Janela de busca (dias)", min_value=1, max_value=30, value=search_cfg.get("lookback_days", 7))

    if st.button("Salvar configurações de busca"):
        if "person_search" not in config:
            config["person_search"] = {}
        config["person_search"]["enabled"] = enabled
        config["person_search"]["day_of_week"] = days_options[selected_day]
        config["person_search"]["lookback_days"] = lookback
        save_config_raw(config)
        st.success("Configurações salvas!")


OAUTH_CACHE = PROJECT_ROOT / "__cache__"
TOKEN_FILE = str(OAUTH_CACHE / "tokens.json")


def _is_youtube_authed() -> bool:
    return Path(TOKEN_FILE).exists()


def page_settings():
    st.header("Configurações")

    env = load_env()
    config = load_config_raw()

    # YouTube Auth
    st.subheader("Conta do YouTube")

    if _is_youtube_authed():
        st.success("Conta do YouTube conectada. As transcricoes vao funcionar automaticamente.")
    else:
        st.warning("Conta do YouTube nao conectada. Conecte abaixo para que as transcricoes funcionem.")

    st.caption(
        "O programa precisa da sua conta do YouTube para acessar as transcricoes dos videos. "
        "Clique no botao abaixo, abra o link no navegador e digite o codigo mostrado. "
        "Isso so precisa ser feito 1 vez."
    )

    if not _is_youtube_authed():
        if st.button("Conectar conta do YouTube", type="primary"):
            st.session_state.oauth_started = True

        if st.session_state.get("oauth_started"):
            with st.spinner("Gerando codigo de autorizacao..."):
                try:
                    from pytubefix import YouTube
                    import threading
                    import queue

                    auth_info = queue.Queue()

                    def custom_verifier(verification_url, user_code):
                        auth_info.put((verification_url, user_code))
                        # Wait for user to complete
                        for _ in range(120):
                            time.sleep(1)

                    # Start auth in background thread
                    def do_auth():
                        try:
                            yt = YouTube(
                                "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                                use_oauth=True,
                                allow_oauth_cache=True,
                                token_file=TOKEN_FILE,
                                oauth_verifier=custom_verifier,
                            )
                            _ = yt.title  # triggers auth
                        except Exception:
                            pass

                    t = threading.Thread(target=do_auth, daemon=True)
                    t.start()

                    # Wait for auth info
                    try:
                        url, code = auth_info.get(timeout=15)
                        st.info(f"1. Abra no navegador: **{url}**")
                        st.info(f"2. Digite o codigo: **{code}**")
                        st.caption("Depois de autorizar no navegador, aguarde alguns segundos e recarregue esta pagina.")

                        t.join(timeout=120)

                        if _is_youtube_authed():
                            st.success("Conta conectada com sucesso!")
                            st.session_state.oauth_started = False
                            st.rerun()
                    except Exception:
                        st.error("Timeout ao gerar codigo. Tente novamente.")
                        st.session_state.oauth_started = False
                except Exception as e:
                    st.error(f"Erro: {e}")
                    st.session_state.oauth_started = False

    st.divider()

    # Supadata API (backup)
    st.subheader("API de backup (opcional)")

    supadata_key = env.get("SUPADATA_API_KEY", "")
    st.caption(
        "Se a conta do YouTube nao estiver conectada, "
        "a Supadata API serve como backup para transcricoes. "
        "Gratis: 100/mes em supadata.ai"
    )
    new_supadata_key = st.text_input(
        "Supadata API Key (opcional)",
        value=supadata_key,
        type="password",
        placeholder="sd_...",
    )

    st.divider()

    # AI Provider
    st.subheader("Provedor de IA")
    provider = config.get("ai_provider", "gemini")
    new_provider = st.radio(
        "Qual IA usar para os resumos?",
        options=["gemini", "claude"],
        format_func=lambda x: "Gemini (Google, gratuito)" if x == "gemini" else "Claude (Anthropic, pago)",
        index=0 if provider == "gemini" else 1,
        horizontal=True,
    )

    # API Keys
    st.subheader("API Keys")

    if new_provider == "gemini":
        st.caption("Obtenha sua key em: https://aistudio.google.com/apikey")
        gemini_key = st.text_input(
            "Gemini API Key",
            value=env.get("GEMINI_API_KEY", ""),
            type="password",
        )
    else:
        st.caption("Obtenha sua key em: https://console.anthropic.com/")
        claude_key = st.text_input(
            "Anthropic API Key",
            value=env.get("ANTHROPIC_API_KEY", ""),
            type="password",
        )

    # Email
    st.subheader("Email")
    st.caption(
        "Para Gmail, use um App Password: https://myaccount.google.com/apppasswords"
    )

    email_addr = st.text_input(
        "Seu email",
        value=env.get("EMAIL_ADDRESS", ""),
        placeholder="seu@gmail.com",
    )
    email_pass = st.text_input(
        "Senha do email (App Password)",
        value=env.get("EMAIL_PASSWORD", ""),
        type="password",
    )
    to_addr = st.text_input(
        "Email de destino (deixe vazio para enviar para si mesmo)",
        value=config.get("email", {}).get("to_address", ""),
    )

    # Schedule
    st.subheader("Horário do digest diário")
    st.caption("O digest roda automaticamente todo dia neste horário.")
    hour = st.slider(
        "Hora",
        min_value=0, max_value=23,
        value=7,
    )

    # Save
    st.divider()
    if st.button("Salvar configurações", type="primary"):
        # Save env
        new_env = {}
        if new_provider == "gemini":
            new_env["GEMINI_API_KEY"] = gemini_key
            new_env["ANTHROPIC_API_KEY"] = env.get("ANTHROPIC_API_KEY", "")
        else:
            new_env["ANTHROPIC_API_KEY"] = claude_key
            new_env["GEMINI_API_KEY"] = env.get("GEMINI_API_KEY", "")
        new_env["EMAIL_ADDRESS"] = email_addr
        new_env["EMAIL_PASSWORD"] = email_pass
        new_env["YOUTUBE_API_KEY"] = env.get("YOUTUBE_API_KEY", "")
        new_env["SUPADATA_API_KEY"] = new_supadata_key
        save_env(new_env)

        # Save config
        config["ai_provider"] = new_provider
        config["email"]["to_address"] = to_addr
        save_config_raw(config)

        st.success("Configurações salvas!")



def _parse_pipeline_line(line: str) -> dict:
    """Parse a log line from the pipeline to extract progress info."""
    info = {}

    # Checking N channels
    m = re.search(r"Checking (\d+) channels", line)
    if m:
        info["stage"] = "channels"
        info["total_channels"] = int(m.group(1))
        return info

    # RSS: N new videos from ChannelName
    m = re.search(r"RSS: (\d+) new videos from (.+)", line)
    if m:
        info["stage"] = "channels"
        info["channel_done"] = m.group(2).strip()
        info["new_count"] = int(m.group(1))
        return info

    # Found N new videos
    m = re.search(r"Found (\d+) new videos", line)
    if m:
        info["stage"] = "channels_done"
        info["total_videos"] = int(m.group(1))
        return info

    # No new videos to process
    if "No new videos to process" in line:
        info["stage"] = "no_videos"
        return info

    # Fetching transcripts
    if "Fetching transcripts" in line:
        info["stage"] = "transcripts"
        return info

    # Transcript via ... : Title
    m = re.search(r"Transcript via \w+ \((\w+), (\d+) chars\): (.+)", line)
    if m:
        info["stage"] = "transcripts"
        info["transcript_title"] = m.group(3).strip()
        return info

    # Transcripts: N OK
    m = re.search(r"Transcripts: (\d+) OK", line)
    if m:
        info["stage"] = "transcripts_done"
        info["transcripts_ok"] = int(m.group(1))
        return info

    # Pass 1: synthesizing N episodes
    m = re.search(r"Pass 1: synthesizing (\d+) episodes", line)
    if m:
        info["stage"] = "synthesis"
        info["total_synthesis"] = int(m.group(1))
        return info

    # [N/M] OK: Title  or  [N/M] FAIL: Title
    m = re.search(r"\[(\d+)/(\d+)\] (OK|FAIL): (.+)", line)
    if m:
        info["stage"] = "synthesis"
        info["synth_current"] = int(m.group(1))
        info["synth_total"] = int(m.group(2))
        info["synth_status"] = m.group(3)
        info["synth_title"] = m.group(4).strip()
        return info

    # Rate limited ... waiting Ns
    m = re.search(r"Rate limited for .+, waiting (\d+)s", line)
    if m:
        info["stage"] = "synthesis"
        info["rate_limited"] = True
        info["wait_seconds"] = int(m.group(1))
        return info

    # Pass 1 complete
    if "Pass 1 complete" in line:
        info["stage"] = "synthesis_done"
        return info

    # Pass 2: cross-episode
    if "Pass 2: cross-episode" in line:
        info["stage"] = "cross_synthesis"
        return info

    # DOCX salvo
    if "DOCX salvo em" in line:
        info["stage"] = "report"
        return info

    # Pipeline complete
    if "Pipeline complete" in line:
        info["stage"] = "done"
        return info

    return info


def page_run():
    st.header("Rodar Digest")

    st.caption(
        "Escolha a janela temporal e clique em Rodar. "
        "O resultado sera salvo como .docx para impressao."
    )

    if not _is_youtube_authed():
        st.warning(
            "Conta do YouTube nao conectada. "
            "Va em Configuracoes e conecte sua conta para que as transcricoes funcionem."
        )

    # Lookback selector
    lookback_options = {
        "1 dia": 1,
        "3 dias": 3,
        "5 dias": 5,
        "7 dias": 7,
    }
    selected_lookback = st.radio(
        "Janela temporal da analise",
        options=list(lookback_options.keys()),
        index=0,
        horizontal=True,
    )
    lookback_days = lookback_options[selected_lookback]

    # Show run status
    if "run_status" not in st.session_state:
        st.session_state.run_status = None
    if "run_log" not in st.session_state:
        st.session_state.run_log = ""

    if st.button("Rodar agora", type="primary"):
        st.session_state.run_status = "running"
        st.session_state.run_log = ""

        # Progress UI elements
        progress_bar = st.progress(0)
        stage_text = st.empty()
        detail_text = st.empty()
        log_expander = st.expander("Log de execucao", expanded=False)
        log_area = log_expander.empty()

        # Stage definitions for progress calculation
        stage_progress = {
            "start": 0.0,
            "channels": 0.05,
            "channels_done": 0.15,
            "transcripts": 0.20,
            "transcripts_done": 0.35,
            "synthesis": 0.40,
            "synthesis_done": 0.85,
            "cross_synthesis": 0.88,
            "report": 0.95,
            "done": 1.0,
        }

        stage_labels = {
            "start": "Iniciando pipeline...",
            "channels": "Buscando novos videos nos canais...",
            "channels_done": "Canais verificados",
            "transcripts": "Extraindo transcricoes...",
            "transcripts_done": "Transcricoes extraidas",
            "synthesis": "Resumindo episodios com IA...",
            "synthesis_done": "Resumos gerados",
            "cross_synthesis": "Analise cruzada entre episodios...",
            "report": "Gerando relatorio .docx...",
            "done": "Pipeline concluido!",
            "no_videos": "Nenhum video novo encontrado.",
        }

        stage_text.info("Iniciando pipeline...")
        accumulated_log = []

        # Load .env into environment
        env_vars = load_env()
        run_env = os.environ.copy()
        run_env.update(env_vars)

        cmd = [
            sys.executable, "-u", "-m", "podcast_digest", "run",
            "--lookback-days", str(lookback_days),
        ]

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(PROJECT_ROOT),
                env=run_env,
                bufsize=1,
            )

            current_stage = "start"
            synth_total = 0
            channels_checked = 0
            total_channels = 0

            for line in process.stdout:
                line = line.rstrip("\n")
                accumulated_log.append(line)
                log_area.code("\n".join(accumulated_log[-100:]), language="text")

                info = _parse_pipeline_line(line)
                if not info:
                    continue

                new_stage = info.get("stage", current_stage)

                # Update progress based on stage
                if new_stage == "channels":
                    current_stage = "channels"
                    if "total_channels" in info:
                        total_channels = info["total_channels"]
                        stage_text.info(f"Buscando novos videos em {total_channels} canais...")
                    elif "channel_done" in info:
                        channels_checked += 1
                        pct = stage_progress["channels"] + (
                            stage_progress["channels_done"] - stage_progress["channels"]
                        ) * (channels_checked / max(total_channels, 1))
                        progress_bar.progress(min(pct, 0.99))
                        detail_text.caption(
                            f"Canal {channels_checked}/{total_channels}: {info['channel_done']} "
                            f"({info.get('new_count', 0)} novos)"
                        )

                elif new_stage == "channels_done":
                    current_stage = "channels_done"
                    progress_bar.progress(stage_progress["channels_done"])
                    n = info.get("total_videos", 0)
                    stage_text.info(f"{n} videos novos encontrados")
                    detail_text.empty()

                elif new_stage == "no_videos":
                    progress_bar.progress(1.0)
                    stage_text.warning("Nenhum video novo para processar.")
                    detail_text.empty()

                elif new_stage == "transcripts":
                    current_stage = "transcripts"
                    base = stage_progress["transcripts"]
                    progress_bar.progress(base)
                    if "transcript_title" in info:
                        stage_text.info("Extraindo transcricoes...")
                        detail_text.caption(f"Transcrevendo: {info['transcript_title'][:70]}")
                    else:
                        stage_text.info("Extraindo transcricoes...")

                elif new_stage == "transcripts_done":
                    current_stage = "transcripts_done"
                    progress_bar.progress(stage_progress["transcripts_done"])
                    stage_text.info(f"{info.get('transcripts_ok', 0)} transcricoes extraidas")
                    detail_text.empty()

                elif new_stage == "synthesis":
                    current_stage = "synthesis"
                    if "total_synthesis" in info:
                        synth_total = info["total_synthesis"]
                        stage_text.info(f"Resumindo {synth_total} episodios com IA...")
                        progress_bar.progress(stage_progress["synthesis"])
                    elif "synth_current" in info:
                        cur = info["synth_current"]
                        tot = info.get("synth_total", synth_total) or synth_total
                        synth_range = stage_progress["synthesis_done"] - stage_progress["synthesis"]
                        pct = stage_progress["synthesis"] + synth_range * (cur / max(tot, 1))
                        progress_bar.progress(min(pct, 0.99))
                        status_icon = "OK" if info["synth_status"] == "OK" else "FALHOU"
                        stage_text.info(f"Resumindo episodios ({cur}/{tot})...")
                        detail_text.caption(
                            f"[{status_icon}] {info['synth_title'][:70]}"
                        )
                    elif info.get("rate_limited"):
                        wait = info.get("wait_seconds", 60)
                        detail_text.caption(
                            f"Rate limit atingido, aguardando {wait}s..."
                        )

                elif new_stage == "synthesis_done":
                    current_stage = "synthesis_done"
                    progress_bar.progress(stage_progress["synthesis_done"])
                    stage_text.info("Resumos gerados!")
                    detail_text.empty()

                elif new_stage == "cross_synthesis":
                    current_stage = "cross_synthesis"
                    progress_bar.progress(stage_progress["cross_synthesis"])
                    stage_text.info("Analise cruzada entre episodios...")
                    detail_text.empty()

                elif new_stage == "report":
                    current_stage = "report"
                    progress_bar.progress(stage_progress["report"])
                    stage_text.info("Gerando relatorio .docx...")
                    detail_text.empty()

                elif new_stage == "done":
                    current_stage = "done"
                    progress_bar.progress(1.0)
                    stage_text.success("Pipeline concluido!")
                    detail_text.empty()

            process.wait(timeout=600)

            full_log = "\n".join(accumulated_log)
            st.session_state.run_log = full_log

            if process.returncode == 0:
                if current_stage != "done":
                    progress_bar.progress(1.0)
                    stage_text.success("Pipeline concluido!")
                    detail_text.empty()
                st.session_state.run_status = "done"
            else:
                stage_text.error("Erro na execucao.")
                st.session_state.run_status = "error"

        except subprocess.TimeoutExpired:
            process.kill()
            stage_text.error("Timeout: a execucao levou mais de 10 minutos.")
            st.session_state.run_status = "error"
        except Exception as e:
            stage_text.error(f"Erro: {e}")
            st.session_state.run_status = "error"

    if st.session_state.run_log and st.session_state.run_status != "running":
        with st.expander("Log de execucao", expanded=False):
            st.code(st.session_state.run_log, language="text")

        if st.session_state.run_status == "done":
            # Show .docx download
            data_dir = PROJECT_ROOT / "data"
            docx_files = sorted(data_dir.glob("podcast_digest_*.docx"), reverse=True)
            if docx_files:
                latest = docx_files[0]
                st.success(f"Arquivo salvo: {latest.name}")
                with open(latest, "rb") as f:
                    st.download_button(
                        "Baixar .docx",
                        data=f.read(),
                        file_name=latest.name,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )

            # Also show HTML preview
            preview_path = PROJECT_ROOT / "data" / "preview.html"
            if preview_path.exists():
                with st.expander("Preview visual"):
                    html = preview_path.read_text(encoding="utf-8")
                    st.components.v1.html(html, height=800, scrolling=True)


def page_history():
    st.header("Historico de Digests")

    st.caption("Consulte resumos de episodios processados anteriormente.")

    db_path = load_config_raw().get("database", {}).get("path", "data/podcast_digest.db")
    if not os.path.isabs(db_path):
        db_path = str(PROJECT_ROOT / db_path)

    import sqlite3
    if not Path(db_path).exists():
        st.info("Nenhum digest encontrado ainda.")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get available dates
    dates = [r["digest_date"] for r in conn.execute(
        "SELECT DISTINCT digest_date FROM digests ORDER BY digest_date DESC"
    ).fetchall()]

    if not dates:
        st.info("Nenhum digest encontrado ainda.")
        conn.close()
        return

    selected_date = st.selectbox("Data", dates)

    # Get digests for selected date
    rows = conn.execute(
        """SELECT d.video_id, v.title, v.channel_id, v.url,
                  d.summary, d.key_topics, d.relevance_score,
                  d.relevance_reason, v.duration_seconds
           FROM digests d
           JOIN videos v ON d.video_id = v.video_id
           WHERE d.digest_date = ?
           ORDER BY d.relevance_score DESC""",
        (selected_date,)
    ).fetchall()
    conn.close()

    # Get channel names from config
    config = load_config_raw()
    ch_names = {ch["id"]: ch["name"] for ch in config.get("channels", [])}

    st.write(f"**{len(rows)} episodios** processados em {selected_date}")

    # Score filter
    min_score = st.slider("Filtrar por score minimo", 1, 10, 1, key="history_filter")

    for r in rows:
        score = r["relevance_score"]
        if score < min_score:
            continue

        channel_name = ch_names.get(r["channel_id"], r["channel_id"])
        dur = r["duration_seconds"] or 0
        dur_str = f" | {dur // 60}min" if dur else ""

        with st.expander(f"[{score}/10] {r['title']} ({channel_name}{dur_str})"):
            st.write(r["summary"])
            topics = r["key_topics"]
            if topics:
                st.caption(f"Topicos: {topics}")
            if r["relevance_reason"]:
                st.caption(f"Relevancia: {r['relevance_reason']}")
            st.caption(r["url"])


def _analyze_single_video(video_url: str, video_id: str, deep: bool = False, user_api_key: str = "", user_provider: str = ""):
    """Fetch info, transcript, and synthesize a single YouTube video."""
    import asyncio
    import json as json_mod

    # Load env and config
    env_vars = load_env()
    os.environ.update(env_vars)

    sys.path.insert(0, str(PROJECT_ROOT))
    from podcast_digest.config import load_config
    from podcast_digest.database import Database
    from podcast_digest.models import Video
    from podcast_digest.synthesis import synthesize_single_video
    from podcast_digest.transcripts import fetch_transcripts_batch

    config = load_config()
    db = Database(config["database"]["path"])

    # Get video info via yt-dlp
    title = ""
    channel_id = "unknown"
    duration = 0
    try:
        yt_dlp_path = str(Path(sys.executable).parent / "yt-dlp")
        r = subprocess.run(
            [yt_dlp_path, "--dump-json", "--no-download", "--no-warnings", video_url],
            capture_output=True, text=True, timeout=30,
            cwd=str(PROJECT_ROOT),
        )
        if r.returncode == 0:
            data = json_mod.loads(r.stdout)
            title = data.get("title", "")
            channel_id = data.get("channel_id", "unknown")
            duration = data.get("duration", 0) or 0
    except Exception:
        pass

    video = Video(
        video_id=video_id,
        channel_id=channel_id,
        title=title or video_id,
        published_at=__import__("datetime").datetime.now(),
        duration_seconds=duration,
        url=video_url,
    )

    yield {"stage": "info", "title": title, "duration": duration}

    # Fetch transcript
    videos = fetch_transcripts_batch([video], config)
    video = videos[0]

    if not video.transcript:
        yield {"stage": "error", "message": "Transcricao nao disponivel para este video."}
        return

    yield {"stage": "transcript", "language": video.transcript_language}

    # Save to DB
    db.save_video(
        video.video_id, video.channel_id, video.title, video.published_at,
        video.duration_seconds, video.url, video.transcript, video.transcript_language,
    )

    # Synthesize
    channel_name = db.get_channel_name(video.channel_id)

    # Override API key and provider if user provided their own
    if user_api_key:
        provider = user_provider or config.get("_ai_provider", config.get("ai_provider", "gemini"))
        config["_ai_provider"] = provider
        config.setdefault(provider, {})["api_key"] = user_api_key

    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            result = pool.submit(
                asyncio.run,
                synthesize_single_video(video, config, channel_name, deep=deep)
            ).result(timeout=120)
    except Exception as e:
        yield {"stage": "error", "message": f"Erro na sintese com IA: {e}"}
        return

    if not result:
        yield {"stage": "error", "message": "Erro na sintese com IA (resultado vazio)."}
        return

    yield {
        "stage": "done",
        "title": result["title"],
        "channel_name": result["channel_name"],
        "summary": result["summary"],
        "key_topics": result["key_topics"],
        "duration_seconds": result["duration_seconds"],
    }


def page_single_video():
    st.header("Analisar Video")

    st.caption(
        "Cole o link de um video do YouTube para gerar uma sintese com IA."
    )

    FREE_LIMIT = 3

    # Get or create persistent user ID via localStorage
    from streamlit_js_eval import streamlit_js_eval
    user_uid = streamlit_js_eval(
        js_expressions="""
        (function() {
            var uid = localStorage.getItem('podcast_digest_uid');
            if (!uid) {
                uid = crypto.randomUUID();
                localStorage.setItem('podcast_digest_uid', uid);
            }
            return uid;
        })()
        """,
        key="get_user_uid",
    )

    # Wait for JS to return the UID
    if not user_uid:
        st.spinner("Carregando...")
        return

    # Check usage from DB
    env_vars = load_env()
    os.environ.update(env_vars)
    sys.path.insert(0, str(PROJECT_ROOT))
    from podcast_digest.config import load_config as _load_config
    from podcast_digest.database import Database

    config = _load_config()
    db = Database(config["database"]["path"])
    usage_count = db.get_usage_count(user_uid)
    remaining = max(0, FREE_LIMIT - usage_count)
    needs_key = remaining == 0

    # Show usage status
    if not needs_key:
        st.info(f"Voce tem {remaining} analise(s) gratuita(s) restante(s).")
    else:
        st.warning(
            "Suas analises gratuitas acabaram. "
            "Insira sua propria API key para continuar usando."
        )
        with st.expander("Como conseguir sua API key (gratuito ou pago)"):
            st.markdown("""
**Gemini (Google) — gratuito**
1. Acesse [ai.google.dev](https://ai.google.dev/)
2. Clique em **Get API key in Google AI Studio**
3. Faca login com sua conta Google
4. Clique em **Create API Key**, copie e cole abaixo
5. O plano gratuito ja inclui uso generoso

**Claude (Anthropic)**
1. Acesse [console.anthropic.com](https://console.anthropic.com/)
2. Crie uma conta (e-mail ou Google)
3. Va em **API Keys** no menu lateral
4. Clique em **Create Key**, copie e cole abaixo

**OpenAI (GPT)**
1. Acesse [platform.openai.com/api-keys](https://platform.openai.com/api-keys)
2. Crie uma conta ou faca login
3. Clique em **Create new secret key**, copie e cole abaixo

**DeepSeek**
1. Acesse [platform.deepseek.com](https://platform.deepseek.com/)
2. Crie uma conta e va em **API Keys**
3. Gere uma nova key, copie e cole abaixo

**Grok (xAI)**
1. Acesse [console.x.ai](https://console.x.ai/)
2. Crie uma conta, va em **API Keys**
3. Gere uma nova key, copie e cole abaixo

A key e detectada automaticamente. Basta colar no campo abaixo e clicar em Analisar.
""")

    url = st.text_input(
        "Link do video",
        placeholder="https://www.youtube.com/watch?v=...",
    )

    depth = st.radio(
        "Profundidade da analise",
        options=["Simples", "Aprofundada"],
        horizontal=True,
        help="Simples: 3-5 paragrafos. Aprofundada: 6-10 paragrafos com mais detalhes.",
    )
    deep = depth == "Aprofundada"

    # API key input (always visible for transparency, required after free uses)
    user_api_key = ""
    user_provider = None
    if needs_key:
        user_api_key = st.text_input(
            "Sua API key",
            type="password",
            help="Cole sua API key de qualquer provider: Gemini, Claude, OpenAI, DeepSeek ou Grok. A IA e detectada automaticamente.",
        )
        if user_api_key:
            from podcast_digest.synthesis import detect_provider
            detected, ambiguous = detect_provider(user_api_key)
            if ambiguous:
                provider_choice = st.radio(
                    "Qual provider?",
                    ["OpenAI", "DeepSeek"],
                    horizontal=True,
                    key="ambiguous_provider",
                )
                user_provider = provider_choice.lower()
            else:
                labels = {"claude": "Claude", "gemini": "Gemini", "grok": "Grok"}
                st.caption(f"Detectado: {labels.get(detected, detected)}")
                user_provider = detected

    if "single_video_result" not in st.session_state:
        st.session_state.single_video_result = None

    if st.button("Analisar", type="primary") and url:
        # Validate API key if required
        if needs_key and not user_api_key:
            st.error("Insira sua API key para continuar.")
            return

        # Extract video ID
        m = re.search(r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})", url)
        if not m:
            st.error("URL invalida. Cole um link do YouTube valido.")
            return

        video_id = m.group(1)
        st.session_state.single_video_result = None

        progress_bar = st.progress(0)
        stage_text = st.empty()
        detail_text = st.empty()

        stage_text.info("Buscando informacoes do video...")
        progress_bar.progress(0.1)

        try:
            for update in _analyze_single_video(url, video_id, deep=deep, user_api_key=user_api_key, user_provider=user_provider or ""):
                stage = update["stage"]

                if stage == "info":
                    title = update.get("title", video_id)
                    dur = update.get("duration", 0)
                    dur_str = f" ({dur // 60}min)" if dur else ""
                    progress_bar.progress(0.2)
                    stage_text.info("Extraindo transcricao...")
                    detail_text.caption(f"{title}{dur_str}")

                elif stage == "transcript":
                    progress_bar.progress(0.5)
                    stage_text.info("Gerando sintese com IA...")

                elif stage == "error":
                    progress_bar.progress(1.0)
                    stage_text.error(update["message"])
                    detail_text.empty()
                    return

                elif stage == "done":
                    progress_bar.progress(1.0)
                    stage_text.success("Analise concluida!")
                    detail_text.empty()
                    st.session_state.single_video_result = update
                    # Increment usage only on success (and only for free users)
                    if not user_api_key:
                        db.increment_usage(user_uid)

        except Exception as e:
            stage_text.error(f"Erro: {e}")
            return

    # Display result
    r = st.session_state.single_video_result
    if r:
        st.divider()

        st.subheader(r["title"])
        dur = r.get("duration_seconds", 0)
        dur_str = f" | {dur // 60}min" if dur else ""
        st.caption(f"{r['channel_name']}{dur_str}")

        st.write(r["summary"])

        if r.get("key_topics"):
            st.write("**Topicos:** " + ", ".join(r["key_topics"]))


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Video Digest" if os.getenv("PUBLIC_MODE", "").lower() in ("true", "1", "yes") else "Podcast Digest",
        page_icon="🎬" if os.getenv("PUBLIC_MODE", "").lower() in ("true", "1", "yes") else "🎧",
        layout="wide",
    )

    # Sidebar navigation
    public_mode = os.getenv("PUBLIC_MODE", "").lower() in ("true", "1", "yes")

    if public_mode:
        st.markdown("""
        <style>
            .back-to-home {
                position: fixed; top: 12px; left: 12px; z-index: 999999;
                background: #1a1610; border: 1px solid #2a2218; border-radius: 8px;
                padding: 6px 14px; text-decoration: none; display: inline-flex;
                align-items: center; gap: 6px; transition: all 0.2s;
            }
            .back-to-home:hover { border-color: #e08a3a; background: #241e16; }
            .back-to-home span { color: #d0b898; font-size: 0.85rem; font-weight: 500; }
            .back-to-home:hover span { color: #e08a3a; }
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
        </style>
        <a href="https://letabuild.com" class="back-to-home" target="_self">
            <span>← letabuild.com</span>
        </a>
        """, unsafe_allow_html=True)
        st.sidebar.title("Video Digest")
        st.sidebar.caption("Analise videos do YouTube via IA")
        page_single_video()
    else:
        st.sidebar.title("Podcast Digest")
        st.sidebar.caption("Curadoria diária de podcasts via IA")
        page = st.sidebar.radio(
            "Navegação",
            options=[
                "Dashboard",
                "Canais",
                "Pessoas",
                "Perfil de Interesses",
                "Feedback",
                "Configurações",
                "Rodar Digest",
                "Analisar Video",
                "Historico",
            ],
            label_visibility="collapsed",
        )

        # Route to page
        if page == "Dashboard":
            page_dashboard()
        elif page == "Canais":
            page_channels()
        elif page == "Pessoas":
            page_people()
        elif page == "Perfil de Interesses":
            page_profile()
        elif page == "Feedback":
            page_feedback()
        elif page == "Configurações":
            page_settings()
        elif page == "Rodar Digest":
            page_run()
        elif page == "Analisar Video":
            page_single_video()
        elif page == "Historico":
            page_history()


if __name__ == "__main__":
    main()
