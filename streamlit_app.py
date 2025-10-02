# streamlit_app.py
import time
from datetime import datetime
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Scanner de Bodega", layout="wide")

# =========================
# "Base de datos" de ejemplo (MOCK)
# Luego la reemplazaremos por Supabase.
# Clave: GUIA
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
# Estado de sesión
# =========================
if "rows" not in st.session_state:
    st.session_state.rows = []       # filas ya escaneadas que se muestran
if "last_scan" not in st.session_state:
    st.session_state.last_scan = ""  # para evitar doble disparo
if "auto_search" not in st.session_state:
    st.session_state.auto_search = True

# =========================
# Encabezado estilo botones
# =========================
colA, colB, colC, colD = st.columns([1.2, 1, 1, 1])
with colA:
    st.button("INGRESAR PAQUETES", use_container_width=True)
with colB:
    st.button("IMPRIMIR GUIAS", use_container_width=True)
with colC:
    st.button("COMPROBANTES DE ENVIOS", use_container_width=True)
with colD:
    st.button("ESTADO DE PAQUETES", use_container_width=True)

st.markdown("### INGRESAR PAQUETES")

# Toggle de escaneo automático
st.checkbox(
    "Escaneo automático",
    key="auto_search",
    help="Limpia el campo y vuelve a enfocar tras cada lectura.",
    value=st.session_state.auto_search,
)

# =========================
# Lógica de búsqueda (mock)
# =========================
def lookup_by_guia(guia: str) -> dict | None:
    """Simula consulta a BD por número de GUIA.
    Luego cambiaremos esta función para que consulte Supabase."""
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
        # Si no existe en BD, deja registro como NO COINCIDENTE
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

    # evita duplicado inmediato por doble lectura del escáner
    if not st.session_state.rows or st.session_state.rows[-1] != row:
        st.session_state.rows.append(row)

    st.session_state.last_scan = guia

# =========================
# Caja de escaneo
# =========================
scan_val = st.text_area(
    "Escanea aquí (o pega el número de guía)",
    height=80,
    placeholder="Apunta el lector aquí y escanea…",
    key="scan_input",
)

submit = st.button("Procesar escaneo", type="primary")

# Disparo: botón o auto (cuando cambia el valor y está activo el toggle)
if submit or (st.session_state.auto_search and scan_val and scan_val != st.session_state.last_scan):
    process_scan(scan_val)
    if st.session_state.auto_search:
        time.sleep(0.05)
        st.session_state.scan_input = ""  # limpia el campo
        st.experimental_rerun()

st.divider()

# =========================
# Filtros rápidos
# =========================
c1, c2, c3 = st.columns([1.2, 1, 1])
with c1:
    date_filter = st.text_input("FECHA DE INGRESO FILTER…")
with c2:
    estado_orden_filter = st.text_input("ESTADO DE ORDEN FILTER…")
with c3:
    envio_filter = st.text_input("Estado de Envio Filter…")

# =========================
# Tabla principal
# =========================
df = (
    pd.DataFrame(st.session_state.rows, columns=COLUMNS)
    if st.session_state.rows
    else pd.DataFrame(columns=COLUMNS)
)

def apply_filters(_df: pd.DataFrame) -> pd.DataFrame:
    if date_filter:
        _df = _df[_df["FECHA DE INGRESO"].str.contains(date_filter, case=False, na=False)]
    if estado_orden_filter:
        _df = _df[_df["ESTADO ORDEN"].str.contains(estado_orden_filter, case=False, na=False)]
    if envio_filter:
        _df = _df[_df["Estado de Envio"].str.contains(envio_filter, case=False, na=False)]
    return _df

filtered = apply_filters(df)

st.dataframe(
    filtered,
    use_container_width=True,
    hide_index=True,
)

# =========================
# Descarga CSV
# =========================
csv_bytes = filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download Filtered CSV",
    data=csv_bytes,
    file_name="paquetes_filtrados.csv",
    mime="text/csv",
)
