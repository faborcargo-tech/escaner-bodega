import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import time
import requests
from io import BytesIO

# ==============================
# CONFIGURACIÓN
# ==============================
st.set_page_config(page_title="Escáner Bodega", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
STORAGE_BUCKET = "etiquetas"
TZ = pytz.timezone("America/Santiago")

# ==============================
# HELPERS DE STORAGE
# ==============================
def _get_public_url(path: str) -> str:
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            url = url.get("publicUrl") or url.get("public_url")
        return url
    except Exception:
        return None

def url_disponible(url: str) -> bool:
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

# ==============================
# DB HELPERS
# ==============================
def lookup_by_guia(guia: str):
    res = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
    return res.data[0] if res.data else None

def update_impresion(guia: str):
    now = datetime.now(TZ).isoformat()
    supabase.table(TABLE_NAME).update({
        "fecha_impresion": now,
        "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!"
    }).eq("guia", guia).execute()

def get_logs(page: str):
    cutoff = (datetime.now(TZ) - timedelta(days=60)).isoformat()
    if page == "ingresar":
        q = supabase.table(TABLE_NAME).select("*").gte("fecha_ingreso", cutoff)
    else:
        q = supabase.table(TABLE_NAME).select("*").gte("fecha_impresion", cutoff)
    return q.order("fecha_impresion" if page == "imprimir" else "fecha_ingreso", desc=True).execute().data or []

# ==============================
# ESCANEO
# ==============================
def process_scan(guia: str):
    match = lookup_by_guia(guia)
    if not match:
        st.error(f"⚠️ Guía {guia} no encontrada en la base de datos.")
        return

    if st.session_state.page == "imprimir":
        update_impresion(guia)
        archivo = match.get("archivo_adjunto", "")
        asignacion = match.get("asignacion", "etiqueta")

        if archivo and url_disponible(archivo):
            st.success(f"🖨️ Etiqueta {asignacion} disponible, descargando...")

            # Descarga directa real del PDF (sin abrir código)
            try:
                file_bytes = requests.get(archivo).content
                st.download_button(
                    label=f"📄 Descargar nuevamente {asignacion}.pdf",
                    data=file_bytes,
                    file_name=f"{asignacion}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
            except Exception as e:
                st.error(f"❌ Error al descargar PDF: {e}")
        else:
            st.warning("⚠️ Etiqueta no disponible o URL inválida.")

    else:
        now = datetime.now(TZ).isoformat()
        supabase.table(TABLE_NAME).update({
            "fecha_ingreso": now,
            "estado_escaneo": "INGRESADO CORRECTAMENTE!"
        }).eq("guia", guia).execute()
        st.success(f"📦 Guía {guia} ingresada correctamente.")

# ==============================
# RECORDAR SECCIÓN ACTIVA
# ==============================
query_params = st.experimental_get_query_params()
default_page = query_params.get("page", ["ingresar"])[0]
if "page" not in st.session_state:
    st.session_state.page = default_page

def set_page(page_name: str):
    st.session_state.page = page_name
    st.experimental_set_query_params(page=page_name)

# ==============================
# UI PRINCIPAL
# ==============================
col1, col2, col3 = st.columns([1,1,1])
with col1:
    if st.button("INGRESAR PAQUETES"):
        set_page("ingresar")
with col2:
    if st.button("IMPRIMIR GUIAS"):
        set_page("imprimir")
with col3:
    if st.button("🗃️ DATOS"):
        set_page("datos")

if st.session_state.page == "ingresar":
    st.markdown("<style>.stApp{background-color:#71A9D9;}</style>", unsafe_allow_html=True)
elif st.session_state.page == "imprimir":
    st.markdown("<style>.stApp{background-color:#71D999;}</style>", unsafe_allow_html=True)
else:
    st.markdown("<style>.stApp{background-color:#F2F4F4;}</style>", unsafe_allow_html=True)

st.header("📦 INGRESAR PAQUETES" if st.session_state.page=="ingresar" else "🖨️ IMPRIMIR GUIAS")

# ==============================
# ESCANEO
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    auto_scan = st.checkbox("Escaneo automático", value=False)
    guia_input = st.text_area("Escanea aquí (o pega el número de guía)", key="scan_input")
    if st.button("Procesar escaneo"):
        process_scan(guia_input.strip())

# ==============================
# LOG (persistente en Supabase)
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    df = pd.DataFrame(rows)

    visible_cols = [
        "asignacion", "guia", 
        "fecha_impresion" if st.session_state.page=="imprimir" else "fecha_ingreso",
        "estado_escaneo", "estado_orden", "estado_envio",
        "archivo_adjunto", "comentario", "titulo", "asin"
    ]
    df = df[[c for c in visible_cols if c in df.columns]]

    def make_button(url, asign):
        if url and url_disponible(url):
            file_bytes = requests.get(url).content
            btn = st.download_button(
                label="Descargar",
                data=file_bytes,
                file_name=f"{asign}.pdf",
                mime="application/pdf",
                key=f"btn_{asign}_{time.time()}"
            )
            return "✅"
        return "No disponible"

    if not df.empty:
        for i, row in df.iterrows():
            st.markdown("---")
            st.write(f"**Asignación:** {row['asignacion']}  |  **Guía:** {row['guia']}")
            st.write(f"**Estado:** {row['estado_escaneo']}  |  **Fecha:** {row.get('fecha_impresion') or row.get('fecha_ingreso')}")
            make_button(row.get("archivo_adjunto"), row["asignacion"])
    else:
        st.info("Sin registros para mostrar.")
