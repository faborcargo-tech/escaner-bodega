import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
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
    try:
        supabase.table(TABLE_NAME).insert({
            "asignacion": "",
            "guia": guia,
            "fecha_ingreso": now.isoformat(),
            "estado_escaneo": "NO COINCIDENTE!",
            "asin": "",
            "cantidad": 0,
            "estado_orden": "",
            "estado_envio": "",
            "archivo_adjunto": "",
            "url_imagen": "",
            "comentario": "",
            "descripcion": "",
            "titulo": ""
        }).execute()
    except Exception as e:
        st.error(f"❌ Error al insertar NO COINCIDENTE ({guia}): {e}")

def get_logs(page: str):
    """Obtiene logs de Supabase según la página (ingreso o impresión)."""
    cutoff = (datetime.now(TZ) - timedelta(days=60)).isoformat()

    if page == "ingresar":
        response = supabase.table(TABLE_NAME).select("*").gte("fecha_ingreso", cutoff).order("fecha_ingreso", desc=True).execute()
    else:
        response = supabase.table(TABLE_NAME).select("*").gte("fecha_impresion", cutoff).order("fecha_impresion", desc=True).execute()

    return response.data if response.data else []

# ==============================
# FUNCION PRINCIPAL DE ESCANEO
# ==============================
def process_scan(guia: str):
    match = lookup_by_guia(guia)

    if match:
        if st.session_state.page == "ingresar":
            update_ingreso(guia)
            st.success(f"📦 Guía {guia} ingresada correctamente")

        elif st.session_state.page == "imprimir":
            update_impresion(guia)
            archivo = match.get("archivo_adjunto", "")
            if archivo:
                st.success("Etiqueta disponible, abriendo en nueva pestaña...")
                st.markdown(f"""<script>window.open("{archivo}", "_blank");</script>""", unsafe_allow_html=True)
            else:
                st.warning("⚠️ Etiqueta no disponible")

    else:
        insert_no_coincidente(guia)
        st.error(f"⚠️ Guía {guia} no encontrada. Se registró como NO COINCIDENTE.")

# ==============================
# UI
# ==============================
st.set_page_config(page_title="Escáner Bodega", layout="wide")

if "page" not in st.session_state:
    st.session_state.page = "ingresar"
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

# Fondo según página
if st.session_state.page == "ingresar":
    st.markdown("<style>.stApp{background-color: #71A9D9;}</style>", unsafe_allow_html=True)
else:
    st.markdown("<style>.stApp{background-color: #71D999;}</style>", unsafe_allow_html=True)

# Título
st.header("📦 " + ("INGRESAR PAQUETES" if st.session_state.page == "ingresar" else "IMPRIMIR GUIAS"))

# Checkbox automático (desactivado por defecto)
auto_scan = st.checkbox("Escaneo automático", value=False)

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
# TABLA LOG DIRECTO DE SUPABASE
# ==============================
st.subheader("Registro de escaneos (últimos 60 días)")
rows = get_logs(st.session_state.page)
df = pd.DataFrame(rows)

# Columnas visibles
visible_cols = ["asignacion", "guia", "fecha_ingreso", "estado_escaneo",
                "estado_orden", "estado_envio", "archivo_adjunto", "comentario", "titulo", "asin"]

df = df[[c for c in visible_cols if c in df.columns]]

# Botón en columna archivo_adjunto
if "archivo_adjunto" in df.columns:
    def make_button(url):
        if url:
            return f'<a href="{url}" target="_blank"><button>Descargar</button></a>'
        return "No disponible"
    df["archivo_adjunto"] = df["archivo_adjunto"].apply(make_button)

# Mostrar tabla
if not df.empty:
    st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
else:
    st.info("No hay registros aún.")

# Export CSV
csv = df.to_csv(index=False).encode("utf-8")
st.download_button("Download Filtered CSV", csv, f"log_{st.session_state.page}.csv", "text/csv")

