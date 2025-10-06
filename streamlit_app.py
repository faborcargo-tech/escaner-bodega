import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import requests
import time

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
# FUNCIONES DE STORAGE
# ==============================
def _get_public_or_signed_url(path: str) -> str | None:
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            url = url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        return url
    except Exception:
        return None


def upload_pdf_to_storage(asignacion: str, uploaded_file) -> str | None:
    """Sube un PDF y devuelve su URL pública, asegurando MIME application/pdf."""
    if not asignacion or uploaded_file is None:
        return None
    key_path = f"{asignacion}.pdf"
    file_bytes = uploaded_file.read()
    try:
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, file_bytes, {"upsert": "true"})
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None
    # Corrige MIME a application/pdf
    try:
        headers = {
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "apikey": SUPABASE_KEY,
            "Content-Type": "application/json",
        }
        requests.patch(
            f"{SUPABASE_URL}/storage/v1/object/info/{STORAGE_BUCKET}/{key_path}",
            headers=headers,
            json={"contentType": "application/pdf"},
            timeout=5
        )
    except Exception:
        pass
    return _get_public_or_signed_url(key_path)


def build_download_url(public_url: str, asignacion: str | None = None) -> str:
    """Convierte /object/public/ → /object/download/ para forzar descarga."""
    if not public_url:
        return ""
    if "/object/public/" in public_url:
        tail = public_url.split("/object/public/", 1)[1]
        return f"{SUPABASE_URL}/storage/v1/object/download/{tail}"
    if asignacion:
        return f"{SUPABASE_URL}/storage/v1/object/download/{STORAGE_BUCKET}/{asignacion}.pdf"
    return public_url


def url_disponible(url: str) -> bool:
    """Verifica si una URL está accesible (200 OK)."""
    if not url:
        return False
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

# ==============================
# BASE DE DATOS
# ==============================
def lookup_by_guia(guia: str):
    res = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
    return res.data[0] if res.data else None


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


def get_logs(page: str):
    cutoff = (datetime.now(TZ) - timedelta(days=60)).isoformat()
    field = "fecha_ingreso" if page == "ingresar" else "fecha_impresion"
    res = supabase.table(TABLE_NAME).select("*").gte(field, cutoff).order(field, desc=True).execute()
    return res.data or []

# ==============================
# ESCANEO
# ==============================
def process_scan(guia: str):
    match = lookup_by_guia(guia)
    if not match:
        insert_no_coincidente(guia)
        st.error(f"⚠️ Guía {guia} no encontrada. Se registró como NO COINCIDENTE.")
        return

    # ---- MODO INGRESAR ----
    if st.session_state.page == "ingresar":
        update_ingreso(guia)
        st.success(f"📦 Guía {guia} ingresada correctamente")
        return

    # ---- MODO IMPRIMIR ----
    if st.session_state.page == "imprimir":
        update_impresion(guia)
        archivo_public = match.get("archivo_adjunto") or ""
        asignacion = (match.get("asignacion") or "etiqueta").strip()

        if archivo_public:
            # URL que fuerza descarga (no muestra “código raro” del PDF)
            download_url = build_download_url(archivo_public, asignacion)
            st.success(f"🖨️ Etiqueta {asignacion} disponible, descargando...")

            # Botón de respaldo (descarga binaria correcta)
            st.download_button(
                label=f"📄 Descargar nuevamente {asignacion}.pdf",
                data=requests.get(download_url).content,
                file_name=f"{asignacion}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )
        else:
            st.warning("⚠️ Etiqueta no disponible para esta guía.")

# ==============================
# UI PRINCIPAL (con persistencia de sección)
# ==============================
query_params = st.query_params
if "page" not in st.session_state:
    st.session_state.page = query_params.get("page", ["ingresar"])[0]

def set_page(p):
    st.session_state.page = p
    st.query_params["page"] = p

col1, col2, col3 = st.columns(3)
with col1:
    if st.button("INGRESAR PAQUETES"):
        set_page("ingresar")
with col2:
    if st.button("IMPRIMIR GUIAS"):
        set_page("imprimir")
with col3:
    if st.button("🗃️ DATOS"):
        set_page("datos")

bg = {"ingresar": "#71A9D9", "imprimir": "#71D999", "datos": "#F2F4F4"}[st.session_state.page]
st.markdown(f"<style>.stApp{{background-color:{bg};}}</style>", unsafe_allow_html=True)
st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else "🗃️ DATOS")
)

# ==============================
# SECCIONES
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    auto_scan = st.checkbox("Escaneo automático", value=False)
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

    # ----- LOG -----
    st.subheader("Registro de escaneos (últimos 60 días)")
    df = pd.DataFrame(get_logs(st.session_state.page))
    if not df.empty:
        visible_cols = [
            "asignacion", "guia",
            "fecha_ingreso", "fecha_impresion",
            "estado_escaneo", "estado_orden", "estado_envio",
            "archivo_adjunto", "comentario", "titulo", "asin"
        ]
        df = df[[c for c in visible_cols if c in df.columns]]

        def make_button(url, asignacion):
            if url_disponible(url):
                link = build_download_url(url, asignacion)
                return f'<a href="{link}" download><button>Descargar</button></a>'
            return "No disponible"

        if "archivo_adjunto" in df.columns:
            df["archivo_adjunto"] = df.apply(
                lambda r: make_button(r["archivo_adjunto"], r.get("asignacion", "")),
                axis=1
            )

        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No hay registros aún.")

# ==============================
# PÁGINA DATOS (solo lectura estable)
# ==============================
if st.session_state.page == "datos":
    st.markdown("### Base de datos completa")
    search = st.text_input("Buscar (asignacion / guia / orden_meli / pack_id / titulo)")
    page_size = st.selectbox("Filas por página", [25, 50, 100, 200], index=1)

    q = supabase.table(TABLE_NAME).select("*").order("id", desc=True)
    if search:
        q = q.or_(
            f"asignacion.ilike.%{search}%,guia.ilike.%{search}%,orden_meli.ilike.%{search}%,pack_id.ilike.%{search}%,titulo.ilike.%{search}%"
        )
    data = q.limit(page_size).execute().data or []

    if not data:
        st.info("Sin registros para mostrar.")
    else:
        st.dataframe(pd.DataFrame(data), use_container_width=True)
