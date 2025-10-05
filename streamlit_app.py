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
STORAGE_BUCKET = "etiquetas"
TZ = pytz.timezone("America/Santiago")

# ==============================
# STORAGE HELPERS
# ==============================
def ensure_storage_bucket() -> bool:
    """Evita verificar bucket con anon key (no tiene permisos)."""
    return True


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


def url_disponible(url: str) -> bool:
    """Verifica si un enlace público realmente existe."""
    if not url:
        return False
    try:
        r = requests.head(url, timeout=5)
        if r.status_code == 200:
            return True
        if r.status_code == 405:
            r = requests.get(url, stream=True, timeout=5)
            return r.status_code == 200
        return False
    except Exception:
        return False


def upload_pdf_to_storage(asignacion: str, uploaded_file) -> str | None:
    """
    Sube el PDF como <asignacion>.pdf al bucket 'etiquetas' (reemplaza si existe)
    y corrige el tipo MIME después de subirlo.
    """
    if not asignacion:
        st.error("La asignación es requerida para subir el PDF.")
        return None
    if uploaded_file is None:
        return None

    key_path = f"{asignacion}.pdf"
    file_bytes = uploaded_file.read()

    try:
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, file_bytes)
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None

    # Ajustar MIME application/pdf
    try:
        headers = {
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "apikey": SUPABASE_KEY,
            "Content-Type": "application/json",
        }
        json_body = {"contentType": "application/pdf"}
        url = f"{SUPABASE_URL}/storage/v1/object/info/{STORAGE_BUCKET}/{key_path}"
        requests.patch(url, headers=headers, json=json_body, timeout=5)
    except Exception:
        pass

    return _get_public_or_signed_url(key_path)


# ==============================
# DB HELPERS
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
    if page == "ingresar":
        response = supabase.table(TABLE_NAME).select(
            "asignacion, guia, fecha_ingreso, estado_escaneo, estado_orden, estado_envio, archivo_adjunto, comentario, titulo, asin"
        ).gte("fecha_ingreso", cutoff).order("fecha_ingreso", desc=True).execute()
    else:
        response = supabase.table(TABLE_NAME).select(
            "asignacion, guia, fecha_impresion, estado_escaneo, estado_orden, estado_envio, archivo_adjunto, comentario, titulo, asin"
        ).gte("fecha_impresion", cutoff).order("fecha_impresion", desc=True).execute()
    return response.data if response.data else []


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
        archivo_public = match.get("archivo_adjunto") or ""
        asignacion = (match.get("asignacion") or "etiqueta").strip()

        if archivo_public:
            st.success(f"🖨️ Etiqueta {asignacion} disponible, descargando...")

            # Mostrar botón de descarga inmediata
            pdf_bytes = None
            try:
                resp = requests.get(archivo_public)
                if resp.status_code == 200:
                    pdf_bytes = resp.content
            except Exception:
                pass

            if pdf_bytes:
                st.download_button(
                    label=f"📄 Descargar nuevamente {asignacion}.pdf",
                    data=pdf_bytes,
                    file_name=f"{asignacion}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
        else:
            st.warning("⚠️ Etiqueta no disponible para esta guía.")


# ==============================
# DATOS CRUD Y UI
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
    return dict(id=None, asignacion="", guia="", fecha_ingreso=None,
                estado_escaneo="", asin="", cantidad=1, estado_orden="",
                estado_envio="", archivo_adjunto="", url_imagen="", comentario="",
                descripcion="", fecha_impresion=None, titulo="", orden_meli="", pack_id="")

def datos_fetch(limit=200, offset=0, search:str=""):
    q = supabase.table(TABLE_NAME).select("*").order("id", desc=True)
    if search:
        res = q.ilike("asignacion", f"%{search}%").range(offset, offset + limit - 1).execute()
        data = res.data or []
        if not data:
            for col in ["guia", "orden_meli", "pack_id", "titulo"]:
                res = supabase.table(TABLE_NAME).select("*").ilike(col, f"%{search}%").order("id", desc=True).range(offset, offset + limit - 1).execute()
                if res.data:
                    data = res.data
                    break
        return data
    else:
        return q.range(offset, offset + limit - 1).execute().data or []


# ==============================
# UI GENERAL
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

if st.session_state.page == "ingresar":
    st.markdown("<style>.stApp{background-color:#71A9D9;}</style>", unsafe_allow_html=True)
elif st.session_state.page == "imprimir":
    st.markdown("<style>.stApp{background-color:#71D999;}</style>", unsafe_allow_html=True)
else:
    st.markdown("<style>.stApp{background-color:#F2F4F4;}</style>", unsafe_allow_html=True)

st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else "🗃️ DATOS")
)

# ==============================
# ESCANEO Y LOG
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)", key="scan_input")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    df = pd.DataFrame(rows)

    if st.session_state.page == "imprimir":
        visible_cols = [
            "asignacion", "guia", "fecha_impresion", "estado_escaneo",
            "estado_orden", "estado_envio", "archivo_adjunto",
            "comentario", "titulo", "asin"
        ]
    else:
        visible_cols = [
            "asignacion", "guia", "fecha_ingreso", "estado_escaneo",
            "estado_orden", "estado_envio", "archivo_adjunto",
            "comentario", "titulo", "asin"
        ]

    df = df[[c for c in visible_cols if c in df.columns]]

    if "archivo_adjunto" in df.columns:
        def make_button(url, asignacion="Etiqueta"):
            if url_disponible(url):
                nombre = asignacion if isinstance(asignacion, str) else "Etiqueta"
                return f'<a href="{url}" target="_blank" download="{nombre}.pdf"><button>Descargar</button></a>'
            return "No disponible"
        df["archivo_adjunto"] = [make_button(row.get("archivo_adjunto"), row.get("asignacion")) for _, row in df.iterrows()]

    if not df.empty:
        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No hay registros aún.")

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download Filtered CSV", csv, f"log_{st.session_state.page}.csv", "text/csv")


# ==============================
# LIMPIAR ADJUNTOS INVÁLIDOS
# ==============================
def limpiar_adjuntos_invalidos():
    st.info("🔍 Verificando enlaces de PDFs... esto puede tardar unos segundos.")
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
