import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import time
from io import BytesIO
import requests

# ==============================
# CONFIG
# ==============================
st.set_page_config(page_title="Escáner Bodega", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
STORAGE_BUCKET = "etiquetas"
TZ = pytz.timezone("America/Santiago")

# ==============================
# HELPERS
# ==============================
def lookup_by_guia(guia: str):
    r = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
    return r.data[0] if r.data else None

def update_ingreso(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update({
        "fecha_ingreso": now.isoformat(),
        "estado_escaneo": "INGRESADO CORRECTAMENTE!"
    }).eq("guia", guia).execute()

def update_impresion(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update({
        "fecha_impresion": now.isoformat(),
        "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!"
    }).eq("guia", guia).execute()

def insert_no_coincidente(guia: str):
    now = datetime.now(TZ)
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

def url_disponible(url: str) -> bool:
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

def descargar_pdf(url: str) -> bytes | None:
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return r.content
        return None
    except Exception:
        return None

# ==============================
# PROCESAR ESCANEO
# ==============================
def process_scan(guia: str):
    match = lookup_by_guia(guia)
    if not match:
        insert_no_coincidente(guia)
        st.error(f"⚠️ Guía {guia} no encontrada. Se registró como NO COINCIDENTE.")
        return

    if st.session_state.page == "ingresar":
        update_ingreso(guia)
        st.success(f"📦 Guía {guia} ingresada correctamente")
        return

    if st.session_state.page == "imprimir":
        update_impresion(guia)
        archivo = match.get("archivo_adjunto", "")
        asignacion = match.get("asignacion", "etiqueta")

        if archivo and url_disponible(archivo):
            st.success(f"🖨️ Etiqueta {asignacion} disponible, descargando...")
            
            pdf_bytes = descargar_pdf(archivo)
            if pdf_bytes:
                st.download_button(
                    label=f"📄 Descargar nuevamente {asignacion}.pdf",
                    data=pdf_bytes,
                    file_name=f"{asignacion}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
            
            # registrar impresión local (simula log)
            now = datetime.now(TZ).isoformat()
            if "logs_impresion" not in st.session_state:
                st.session_state.logs_impresion = []
            st.session_state.logs_impresion.append({
                "asignacion": asignacion,
                "guia": guia,
                "fecha_impresion": now,
                "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!",
                "estado_orden": match.get("estado_orden", ""),
                "estado_envio": match.get("estado_envio", ""),
                "archivo_adjunto": archivo,
                "comentario": match.get("comentario", ""),
                "titulo": match.get("titulo", ""),
                "asin": match.get("asin", "")
            })
        else:
            st.warning("⚠️ Etiqueta no disponible o dañada.")

# ==============================
# UI
# ==============================
if "page" not in st.session_state:
    st.session_state.page = "ingresar"

col1, col2, col3 = st.columns([1,1,1])
with col1:
    if st.button("INGRESAR PAQUETES"):
        st.session_state.page = "ingresar"
with col2:
    if st.button("IMPRIMIR GUIAS"):
        st.session_state.page = "imprimir"
with col3:
    if st.button("🗃️ DATOS"):
        st.session_state.page = "datos"

if st.session_state.page == "imprimir":
    st.markdown("<style>.stApp{background-color:#71D999;}</style>", unsafe_allow_html=True)
else:
    st.markdown("<style>.stApp{background-color:#71A9D9;}</style>", unsafe_allow_html=True)

st.header("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else "📦 INGRESAR PAQUETES")

# ==============================
# ESCANEO
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)", key="scan_input")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

# ==============================
# LOG DE ESCANEOS
# ==============================
if st.session_state.page == "imprimir":
    st.subheader("Registro de escaneos (últimos 60 días)")
    logs = st.session_state.get("logs_impresion", [])
    df = pd.DataFrame(logs)
    if not df.empty:
        visible_cols = [
            "asignacion", "guia", "fecha_impresion", "estado_escaneo",
            "estado_orden", "estado_envio", "archivo_adjunto",
            "comentario", "titulo", "asin"
        ]
        df = df[[c for c in visible_cols if c in df.columns]]

        # Botones de descarga funcionales
        def make_button(url, asign):
            pdf_bytes = descargar_pdf(url)
            if pdf_bytes:
                return st.download_button(
                    label="Descargar",
                    data=pdf_bytes,
                    file_name=f"{asign}.pdf",
                    mime="application/pdf",
                    key=f"dl_{asign}_{time.time()}"
                )
            else:
                return "No disponible"

        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No hay registros aún.")
