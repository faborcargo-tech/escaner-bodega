# streamlit_app.py
import time
from datetime import datetime
import pandas as pd
import streamlit as st
from supabase import create_client, Client

st.set_page_config(page_title="Scanner de Bodega", layout="wide")

# =========================
# Conexión con Supabase
# =========================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"  # 👈 nombre de tu tabla

# =========================
# Estado de sesión
# =========================
if "rows" not in st.session_state:
    st.session_state.rows = []
if "last_scan" not in st.session_state:
    st.session_state.last_scan = ""
if "auto_search" not in st.session_state:
    st.session_state.auto_search = True
if "page" not in st.session_state:
    st.session_state.page = "ingresar"

# =========================
# Columnas visibles en la tabla
# =========================
COLUMNS = [
    "asignacion",
    "guia",
    "fecha_ingreso",
    "fecha_impresion",
    "estado_escaneo",
    "asin",
    "cantidad",
    "estado_orden",
    "estado_envio",
    "archivo_adjunto",
    "url_imagen",
    "comentario",
    "descripcion",
    "titulo",
]

# =========================
# Estilos personalizados
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
# Funciones de Supabase con manejo de errores
# =========================
def lookup_by_guia(guia: str) -> dict | None:
    """Busca la guía en Supabase y devuelve un diccionario."""
    try:
        response = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
        return response.data[0] if response.data else None
    except Exception as e:
        st.error(f"❌ Error al consultar guía {guia}: {e}")
        return None

def insert_no_coincidente(guia: str):
    """Inserta un paquete no coincidente en Supabase."""
    now_str = datetime.now().isoformat()
    try:
        supabase.table(TABLE_NAME).insert({
            "guia": guia,
            "fecha_ingreso": now_str,
            "estado_escaneo": "NO COINCIDENTE",
        }).execute()
    except Exception as e:
        st.error(f"❌ Error al insertar NO COINCIDENTE ({guia}): {e}")

def update_ingreso(guia: str):
    """Actualiza fecha_ingreso y estado_escaneo en Supabase."""
    try:
        supabase.table(TABLE_NAME).update({
            "fecha_ingreso": datetime.now().isoformat(),
            "estado_escaneo": "INGRESADO CORRECTAMENTE!"
        }).eq("guia", guia).execute()
    except Exception as e:
        st.error(f"❌ Error al actualizar ingreso ({guia}): {e}")

def update_impresion(guia: str):
    """Actualiza fecha_impresion en Supabase."""
    try:
        supabase.table(TABLE_NAME).update({
            "fecha_impresion": datetime.now().isoformat()
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
            match["fecha_ingreso"] = datetime.now().strftime("%d/%m/%Y %I:%M%p").lower()
            match["estado_escaneo"] = "INGRESADO CORRECTAMENTE!"
        elif st.session_state.page == "imprimir":
            update_impresion(guia)
            match["fecha_impresion"] = datetime.now().strftime("%d/%m/%Y %I:%M%p").lower()
    else:
        insert_no_coincidente(guia)
        match = {
            "asignacion": "",
            "guia": guia,
            "fecha_ingreso": datetime.now().strftime("%d/%m/%Y %I:%M%p").lower(),
            "fecha_impresion": "",
            "estado_escaneo": "NO COINCIDENTE!",
            "asin": "",
            "cantidad": 0,
            "estado_orden": "",
            "estado_envio": "",
            "archivo_adjunto": "",
            "url_imagen": "",
            "comentario": "",
            "descripcion": "",
            "titulo": "",
        }

    if not st.session_state.rows or st.session_state.rows[-1] != match:
        st.session_state.rows.append(match)

    st.session_state.last_scan = guia

# =========================
# Contenido de páginas
# =========================
if st.session_state.page == "ingresar":
    st.header("📦 INGRESAR PAQUETES")
else:
    st.header("🖨️ IMPRIMIR GUIAS")

# Caja de escaneo
st.checkbox(
    "Escaneo automático",
    key="auto_search",
    help="Limpia el campo y vuelve a enfocar tras cada lectura.",
    value=st.session_state.auto_search,
)

scan_val = st.text_area(
    "Escanea aquí (o pega el número de guía)",
    height=80,
    placeholder="Apunta el lector aquí y escanea…",
    key="scan_input",
)

submit = st.button("Procesar escaneo", type="primary")

if submit or (st.session_state.auto_search and scan_val and scan_val != st.session_state.last_scan):
    process_scan(scan_val)
    if st.session_state.auto_search:
        time.sleep(0.05)
        st.session_state.pop("scan_input", None)
        st.rerun()

st.divider()

# Filtros
c1, c2, c3 = st.columns([1.2, 1, 1])
with c1:
    date_filter = st.text_input("FECHA DE INGRESO FILTER…")
with c2:
    estado_orden_filter = st.text_input("ESTADO DE ORDEN FILTER…")
with c3:
    envio_filter = st.text_input("Estado de Envio Filter…")

# Tabla
df = (
    pd.DataFrame(st.session_state.rows, columns=COLUMNS)
    if st.session_state.rows
    else pd.DataFrame(columns=COLUMNS)
)

if date_filter:
    df = df[df["fecha_ingreso"].astype(str).str.contains(date_filter, case=False, na=False)]
if estado_orden_filter:
    df = df[df["estado_orden"].astype(str).str.contains(estado_orden_filter, case=False, na=False)]
if envio_filter:
    df = df[df["estado_envio"].astype(str).str.contains(envio_filter, case=False, na=False)]

st.dataframe(df, use_container_width=True, hide_index=True)

# Descarga CSV
csv_bytes = df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download Filtered CSV",
    data=csv_bytes,
    file_name="paquetes_filtrados.csv",
    mime="text/csv",
)
