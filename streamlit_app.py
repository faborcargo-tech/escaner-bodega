import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import time
import requests
from io import BytesIO

# ==============================
# CONFIG
# ==============================
st.set_page_config(page_title="Escáner Bodega", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
STORAGE_BUCKET = "etiquetas"  # PDFs de etiquetas
TZ = pytz.timezone("America/Santiago")

# ==============================
# STORAGE HELPERS
# ==============================
def _get_public_or_signed_url(path: str) -> str | None:
    """Devuelve URL pública o firmada si el bucket es privado."""
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            url = url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        if url:
            return url
    except Exception:
        pass
    try:
        signed = supabase.storage.from_(STORAGE_BUCKET).create_signed_url(path, 60 * 60 * 24 * 30)
        if isinstance(signed, dict):
            return signed.get("signedUrl") or signed.get("signedURL") or signed.get("signed_url")
        return signed
    except Exception:
        return None


def upload_pdf_to_storage(asignacion: str, uploaded_file) -> str | None:
    """
    Sube PDF <asignacion>.pdf al bucket y devuelve URL pública o firmada.
    """
    if not asignacion or uploaded_file is None:
        return None

    key_path = f"{asignacion}.pdf"
    file_bytes = uploaded_file.read()

    try:
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, file_bytes)
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None

    # Corrige MIME
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


def build_download_url(public_url: str | None = None, asignacion: str | None = None) -> str | None:
    """Convierte una URL pública /object/public/... en /object/download/..."""
    if public_url and "/object/public/" in public_url:
        tail = public_url.split("/object/public/", 1)[1]
        return f"{SUPABASE_URL}/storage/v1/object/download/{tail}"
    if asignacion:
        return f"{SUPABASE_URL}/storage/v1/object/download/{STORAGE_BUCKET}/{asignacion}.pdf"
    return public_url


def url_disponible(url: str) -> bool:
    if not url:
        return False
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

    if st.session_state.page == "ingresar":
        update_ingreso(guia)
        st.success(f"📦 Guía {guia} ingresada correctamente")
        return

    if st.session_state.page == "imprimir":
        update_impresion(guia)
        archivo_public = match.get("archivo_adjunto") or ""
        asignacion = (match.get("asignacion") or "etiqueta").strip()

        if archivo_public:
            download_url = build_download_url(archivo_public, asignacion)
            st.success("Etiqueta disponible, descargando…")

            # ✅ Descarga automática directa (sin abrir ventana)
            html = f'<iframe src="{download_url}" style="display:none;width:0;height:0;border:0;"></iframe>'
            st.components.v1.html(html, height=0)
        else:
            st.warning("⚠️ Etiqueta no disponible para esta guía.")


# ==============================
# DATOS (CRUD)
# ==============================
ALL_COLUMNS = [
    "id", "asignacion", "guia", "fecha_ingreso", "estado_escaneo",
    "asin", "cantidad", "estado_orden", "estado_envio",
    "archivo_adjunto", "url_imagen", "comentario", "descripcion",
    "fecha_impresion", "titulo", "orden_meli", "pack_id"
]
REQUIRED_FIELDS = ["asignacion", "orden_meli"]
LOCKED_FIELDS_EDIT = ["asignacion", "orden_meli"]

def datos_defaults():
    return dict(
        id=None, asignacion="", guia="", fecha_ingreso=None,
        estado_escaneo="", asin="", cantidad=1, estado_orden="",
        estado_envio="", archivo_adjunto="", url_imagen="", comentario="",
        descripcion="", fecha_impresion=None, titulo="", orden_meli="", pack_id=""
    )


# ==============================
# UI (NAV Y COLOR)
# ==============================
if "page" not in st.session_state:
    st.session_state.page = "ingresar"

col1, col2, col3 = st.columns(3)
with col1:
    if st.button("INGRESAR PAQUETES"):
        st.session_state.page = "ingresar"
with col2:
    if st.button("IMPRIMIR GUIAS"):
        st.session_state.page = "imprimir"
with col3:
    if st.button("🗃️ DATOS"):
        st.session_state.page = "datos"

bg = {"ingresar": "#71A9D9", "imprimir": "#71D999", "datos": "#F2F4F4"}[st.session_state.page]
st.markdown(f"<style>.stApp{{background-color:{bg};}}</style>", unsafe_allow_html=True)

st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else "🗃️ DATOS")
)

# ==============================
# ESCANEO
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    auto_scan = st.checkbox("Escaneo automático", value=False)
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)", key="scan_input")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

# ==============================
# LOG ESCANEOS
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    df = pd.DataFrame(rows)

    visible_cols = [
        "asignacion", "guia", "fecha_ingreso", "estado_escaneo",
        "estado_orden", "estado_envio", "archivo_adjunto",
        "comentario", "titulo", "asin"
    ]
    df = df[[c for c in visible_cols if c in df.columns]]

    # 🔗 Reemplazar por botón de descarga real
    if "archivo_adjunto" in df.columns:
        def make_button(url, asign):
            dl = build_download_url(url, asign)
            if url_disponible(dl):
                return f'<a href="{dl}" download><button>Descargar</button></a>'
            return "No disponible"

        df["archivo_adjunto"] = df.apply(lambda r: make_button(r.get("archivo_adjunto"), r.get("asignacion")), axis=1)

    if not df.empty:
        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No hay registros aún.")

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Filtered CSV", csv, f"log_{st.session_state.page}.csv", "text/csv"
    )

# ==============================
# LIMPIAR ADJUNTOS INVÁLIDOS
# ==============================
def limpiar_adjuntos_invalidos():
    st.info("🔍 Verificando enlaces de PDFs...")
    try:
        res = supabase.table(TABLE_NAME).select("id, archivo_adjunto").execute()
        rows = [r for r in (res.data or []) if r.get("archivo_adjunto")]
    except Exception as e:
        st.error(f"❌ Error leyendo datos: {e}")
        return

    total = 0
    for r in rows:
        url = r.get("archivo_adjunto")
        if not url:
            continue
        try:
            resp = requests.head(url, timeout=5)
            if resp.status_code == 404:
                supabase.table(TABLE_NAME).update({"archivo_adjunto": None}).eq("id", r["id"]).execute()
                total += 1
        except Exception:
            continue

    st.success(f"✅ Se limpiaron {total} enlaces inválidos.")

if st.button("🧹 Limpiar adjuntos inválidos"):
    limpiar_adjuntos_invalidos()
