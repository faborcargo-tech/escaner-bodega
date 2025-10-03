# streamlit_app.py
import time
from datetime import datetime
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Scanner de Bodega", layout="wide")

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
    st.session_state.page = "ingresar"  # página inicial

# =========================
# Base de datos simulada (MOCK)
# =========================
MOCK_DB = {
    "712946833130": {
        "ASIGNACION": "FBC7654",
        "GUIA": "712946833130",
        "FECHA DE INGRESO": "26/09/2025 10:24am",
        "ESTADO ESCANEO": "PENDIENTE",
        "ASIN": "B08BNS3D8G",
        "QUANTITY": 1,
        "ESTADO ORDEN": "approved",
        "Estado de Envio": "pending",
    },
    "712946950192": {
        "ASIGNACION": "FBC7655",
        "GUIA": "712946950192",
        "FECHA DE INGRESO": "26/09/2025 10:24am",
        "ESTADO ESCANEO": "PENDIENTE",
        "ASIN": "B0CDWS1NL6",
        "QUANTITY": 1,
        "ESTADO ORDEN": "approved",
        "Estado de Envio": "pending",
    },
    "712946830680": {
        "ASIGNACION": "FBC7650",
        "GUIA": "712946830680",
        "FECHA DE INGRESO": "26/09/2025 10:24am",
        "ESTADO ESCANEO": "PENDIENTE",
        "ASIN": "B000GVFLIO",
        "QUANTITY": 1,
        "ESTADO ORDEN": "approved",
        "Estado de Envio": "pending",
    },
}

COLUMNS = [
    "ASIGNACION",
    "GUIA",
    "FECHA DE INGRESO",
    "ESTADO ESCANEO",
    "ASIN",
    "QUANTITY",
    "ESTADO ORDEN",
    "Estado de Envio",
]

# =========================
# Estilos personalizados
# =========================
def set_page_style():
    if st.session_state.page == "ingresar":
        bg_color = "#71A9D9"
    else:
        bg_color = "#71D999"

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
st.markdown('<div class="nav-container">', unsafe_allow_html=True)

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

st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Funciones comunes
# =========================
def lookup_by_guia(guia: str) -> dict | None:
    return MOCK_DB.get(guia)

def process_scan(guia: str):
    guia = guia.strip()
    if not guia:
        return

    match = lookup_by_guia(guia)
    now_str = datetime.now().strftime("%d/%m/%Y %I:%M%p").lower()

    if match:
        row = match.copy()
        row["FECHA DE INGRESO"] = now_str
        row["ESTADO ESCANEO"] = "INGRESADO CORRECTAMENTE!"
    else:
        row = {
            "ASIGNACION": "",
            "GUIA": guia,
            "FECHA DE INGRESO": now_str,
            "ESTADO ESCANEO": "NO COINCIDENTE!",
            "ASIN": "",
            "QUANTITY": 0,
            "ESTADO ORDEN": "",
            "Estado de Envio": "",
        }

    if not st.session_state.rows or st.session_state.rows[-1] != row:
        st.session_state.rows.append(row)

    st.session_state.last_scan = guia

# =========================
# Contenido de páginas (idénticas en estructura)
# =========================
if st.session_state.page == "ingresar":
    st.header("📦 INGRESAR PAQUETES")

elif st.session_state.page == "imprimir":
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
    df = df[df["FECHA DE INGRESO"].str.contains(date_filter, case=False, na=False)]
if estado_orden_filter:
    df = df[df["ESTADO ORDEN"].str.contains(estado_orden_filter, case=False, na=False)]
if envio_filter:
    df = df[df["Estado de Envio"].str.contains(envio_filter, case=False, na=False)]

st.dataframe(df, use_container_width=True, hide_index=True)

# Descarga CSV
csv_bytes = df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download Filtered CSV",
    data=csv_bytes,
    file_name="paquetes_filtrados.csv",
    mime="text/csv",
)

