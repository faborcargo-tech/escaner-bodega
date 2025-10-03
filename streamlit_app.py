# streamlit_app.py
import time
from datetime import datetime
import pandas as pd
import pytz
import streamlit as st
from supabase import create_client, Client

st.set_page_config(page_title="Scanner de Bodega", layout="wide")

# =========================
# Conexión con Supabase
# =========================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"

# =========================
# Estado de sesión
# =========================
if "rows_ingresar" not in st.session_state:
    st.session_state.rows_ingresar = []
if "rows_imprimir" not in st.session_state:
    st.session_state.rows_imprimir = []
if "last_scan" not in st.session_state:
    st.session_state.last_scan = ""
if "last_scan_time" not in st.session_state:
    st.session_state.last_scan_time = time.time()
if "auto_search" not in st.session_state:
    st.session_state.auto_search = True
if "page" not in st.session_state:
    st.session_state.page = "ingresar"

# =========================
# Columnas visibles
# =========================
VISIBLE_COLUMNS = [
    "asignacion",
    "guia",
    "fecha_ingreso",
    "estado_escaneo",
    "estado_orden",
    "estado_envio",
    "archivo_adjunto",
    "comentario",
    "titulo",
    "asin",
]

# =========================
# Estilos
# =========================
def set_page_style():
    if st.session_state.page == "ingresar":
        bg_color = "#71A9D9"  # azul
    else:
        bg_color = "#71D999"  # verde

    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {bg_color};
        }}
        .nav-container {{
            display: flex;
            justify-content: center;
            gap: 20px;
            margin-bottom: 25px;
        }}
        .nav-button {{
            padding: 0.7em 1.5em;
            border-radius: 10px;
            font-weight: bold;
            font-size: 15px;
            cursor: pointer;
            text-align: center;
            border: 2px solid black;
            background-color: white;
            color: black;
        }}
        .nav-button-active {{
            background-color: black !important;
            color: white !important;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

set_page_style()

# =========================
# Navegación
# =========================
col1, col2 = st.columns([1, 1])
with col1:
    if st.button("INGRESAR PAQUETES", key="btn_ingresar"):
        st.session_state.page = "ingresar"
        set_page_style()
        st.rerun()

with col2:
    if st.button("IMPRIMIR GUIAS", key="btn_imprimir"):
        st.session_state.page = "imprimir"
        set_page_style()
        st.rerun()

# =========================
# Funciones de Supabase
# =========================
def lookup_by_guia(guia: str) -> dict | None:
    try:
        response = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
        return response.data[0] if response.data else None
    except Exception as e:
        st.error(f"❌ Error al consultar guía {guia}: {e}")
        return None

def insert_no_coincidente(guia: str):
    now_str = datetime.now(pytz.timezone("America/Santiago")).isoformat()
    try:
        supabase.table(TABLE_NAME).insert({
            "guia": guia,
            "fecha_ingreso": now_str,
            "estado_escaneo": "NO COINCIDENTE",
        }).execute()
    except Exception as e:
        st.error(f"❌ Error al insertar NO COINCIDENTE ({guia}): {e}")

def update_ingreso(guia: str):
    try:
        supabase.table(TABLE_NAME).update({
            "fecha_ingreso": datetime.now(pytz.timezone("America/Santiago")).isoformat(),
            "estado_escaneo": "INGRESADO CORRECTAMENTE!"
        }).eq("guia", guia).execute()
    except Exception as e:
        st.error(f"❌ Error al actualizar ingreso ({guia}): {e}")

def update_impresion(guia: str):
    try:
        supabase.table(TABLE_NAME).update({
            "fecha_impresion": datetime.now(pytz.timezone("America/Santiago")).isoformat()
        }).eq("guia", guia).execute()
    except Exception as e:
        st.error(f"❌ Error al actualizar impresión ({guia}): {e}")

# =========================
# Procesar escaneo
# =========================
def process_scan(guia: str):
    guia = guia.strip()
    if not guia:
        return

    match = lookup_by_guia(guia)

    if match:
        if st.session_state.page == "ingresar":
            update_ingreso(guia)
            match["fecha_ingreso"] = datetime.now(pytz.timezone("America/Santiago")).strftime("%d/%m/%Y %I:%M%p").lower()
            match["estado_escaneo"] = "INGRESADO CORRECTAMENTE!"
        elif st.session_state.page == "imprimir":
            update_impresion(guia)
            match["fecha_impresion"] = datetime.now(pytz.timezone("America/Santiago")).strftime("%d/%m/%Y %I:%M%p").lower()
    else:
        insert_no_coincidente(guia)
        match = {
            "asignacion": "",
            "guia": guia,
            "fecha_ingreso": datetime.now(pytz.timezone("America/Santiago")).strftime("%d/%m/%Y %I:%M%p").lower(),
            "estado_escaneo": "NO COINCIDENTE!",
            "estado_orden": "",
            "estado_envio": "",
            "archivo_adjunto": "",
            "comentario": "",
            "titulo": "",
            "asin": "",
        }

    if st.session_state.page == "ingresar":
        st.session_state.rows_ingresar.insert(0, match)
    else:
        st.session_state.rows_imprimir.insert(0, match)

    st.session_state.last_scan = guia
    st.session_state.last_scan_time = time.time()

# =========================
# Contenido páginas
# =========================
if st.session_state.page == "ingresar":
    st.header("📦 INGRESAR PAQUETES")
else:
    st.header("🖨️ IMPRIMIR GUIAS")

st.checkbox(
    "Escaneo automático",
    key="auto_search",
    help="Procesa automáticamente al detectar más de 8 caracteres después de 1s.",
    value=st.session_state.auto_search,
)

scan_val = st.text_area(
    "Escanea aquí (o pega el número de guía)",
    height=80,
    placeholder="Apunta el lector aquí y escanea…",
    key="scan_input",
)

submit = st.button("Procesar escaneo", type="primary")

# Lógica de auto escaneo
if st.session_state.auto_search and scan_val and scan_val != st.session_state.last_scan:
    now = time.time()
    if len(scan_val.strip()) > 8 and (now - st.session_state.last_scan_time) > 1:
        process_scan(scan_val)
        st.session_state.pop("scan_input", None)
        st.rerun()

if submit:
    process_scan(scan_val)
    st.session_state.pop("scan_input", None)
    st.rerun()

st.divider()

# =========================
# Tabla
# =========================
rows = st.session_state.rows_ingresar if st.session_state.page == "ingresar" else st.session_state.rows_imprimir

df = (
    pd.DataFrame(rows, columns=VISIBLE_COLUMNS)
    if rows else pd.DataFrame(columns=VISIBLE_COLUMNS)
)

st.dataframe(df, use_container_width=True, hide_index=True)

# Descarga CSV
csv_bytes = df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download Filtered CSV",
    data=csv_bytes,
    file_name=f"paquetes_{st.session_state.page}.csv",
    mime="text/csv",
)
