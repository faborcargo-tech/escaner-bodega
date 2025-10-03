import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import pytz
import time

# ==============================
# CONFIG
# ==============================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
TZ = pytz.timezone("America/Santiago")

# ==============================
# FUNCIONES BASE DE DATOS
# ==============================
def get_all_registros():
    response = supabase.table(TABLE_NAME).select("*").execute()
    return response.data if response.data else []

def lookup_by_guia(guia: str):
    response = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
    return response.data[0] if response.data else None

def update_ingreso(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update({
        "fecha_ingreso": now.isoformat(),
        "estado_escaneo": "INGRESADO CORRECTAMENTE!"
    }).eq("guia", guia).execute()

def update_impresion(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update({
        "fecha_impresion": now.isoformat()
    }).eq("guia", guia).execute()

def insert_no_coincidente(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).insert({
        "guia": guia,
        "fecha_ingreso": now.isoformat(),
        "estado_escaneo": "NO COINCIDENTE!",
        "cantidad": 0
    }).execute()

# ==============================
# FUNCION PRINCIPAL DE ESCANEO
# ==============================
def process_scan(guia: str):
    match = lookup_by_guia(guia)

    if match:
        if st.session_state.page == "ingresar":
            update_ingreso(guia)
            match["fecha_ingreso"] = datetime.now(TZ).strftime("%d/%m/%Y %I:%M%p").lower()
            match["estado_escaneo"] = "INGRESADO CORRECTAMENTE!"

        elif st.session_state.page == "imprimir":
            update_impresion(guia)
            match["fecha_impresion"] = datetime.now(TZ).strftime("%d/%m/%Y %I:%M%p").lower()
            archivo = match.get("archivo_adjunto", "")
            if archivo:
                st.success("Etiqueta disponible, iniciando descarga...")
                st.markdown(
                    f"""<meta http-equiv="refresh" content="0; url={archivo}" />""",
                    unsafe_allow_html=True
                )
            else:
                st.warning("⚠️ Etiqueta no disponible")

        st.session_state.logs[st.session_state.page].insert(0, match)

    else:
        insert_no_coincidente(guia)
        st.session_state.logs[st.session_state.page].insert(0, {
            "guia": guia,
            "fecha_ingreso": datetime.now(TZ).strftime("%d/%m/%Y %I:%M%p").lower(),
            "estado_escaneo": "NO COINCIDENTE!",
            "cantidad": 0
        })

# ==============================
# UI
# ==============================
st.set_page_config(page_title="Escáner Bodega", layout="wide")

if "page" not in st.session_state:
    st.session_state.page = "ingresar"
if "logs" not in st.session_state:
    st.session_state.logs = {"ingresar": [], "imprimir": []}
if "last_input" not in st.session_state:
    st.session_state.last_input = ""
if "last_time" not in st.session_state:
    st.session_state.last_time = time.time()

# Barra de navegación
col1, col2 = st.columns([1,1])
with col1:
    if st.button("INGRESAR PAQUETES"):
        st.session_state.page = "ingresar"
with col2:
    if st.button("IMPRIMIR GUIAS"):
        st.session_state.page = "imprimir"

# Cambiar color de fondo según la página
if st.session_state.page == "ingresar":
    st.markdown("<style>body {background-color: #71A9D9;}</style>", unsafe_allow_html=True)
elif st.session_state.page == "imprimir":
    st.markdown("<style>body {background-color: #71D999;}</style>", unsafe_allow_html=True)

# Título
st.header("📦 " + ("INGRESAR PAQUETES" if st.session_state.page == "ingresar" else "IMPRIMIR GUIAS"))

# Checkbox automático
auto_scan = st.checkbox("Escaneo automático", value=True)

# Input de escaneo
scan_val = st.text_area("Escanea aquí (o pega el número de guía)", key="scan_input")

# Procesar manual
if st.button("Procesar escaneo"):
    process_scan(scan_val.strip())

# Escaneo automático
if auto_scan:
    if scan_val != st.session_state.last_input and len(scan_val.strip()) > 8:
        now = time.time()
        if now - st.session_state.last_time > 1:
            process_scan(scan_val.strip())
            st.session_state.last_input = scan_val
            st.session_state.last_time = now

# ==============================
# TABLA LOG
# ==============================
st.subheader("Registro de escaneos")
df = pd.DataFrame(st.session_state.logs[st.session_state.page])

# Columnas visibles
visible_cols = ["asignacion", "guia", "fecha_ingreso", "estado_escaneo",
                "estado_orden", "estado_envio", "archivo_adjunto", "comentario", "titulo", "asin"]

df = df[[c for c in visible_cols if c in df.columns]]

# Reemplazar archivo_adjunto por botones de descarga
if "archivo_adjunto" in df.columns:
    def make_button(url):
        if url:
            return f'<a href="{url}" target="_blank"><button>Descargar</button></a>'
        return "No disponible"
    df["archivo_adjunto"] = df["archivo_adjunto"].apply(make_button)

# Mostrar tabla ordenada
if not df.empty:
    st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
else:
    st.info("No hay registros aún.")

# Export CSV
csv = df.to_csv(index=False).encode("utf-8")
st.download_button("Download Filtered CSV", csv, "log.csv", "text/csv")
