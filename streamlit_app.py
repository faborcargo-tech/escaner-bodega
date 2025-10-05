import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import time
import requests
from io import BytesIO

# ==============================
# CONFIGURACIÓN GENERAL
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
def _get_public_or_signed_url(path: str) -> str | None:
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            url = url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        return url
    except Exception:
        return None


def upload_pdf_to_storage(asignacion: str, uploaded_file) -> str | None:
    """
    Sube el PDF como <asignacion>.pdf al bucket 'etiquetas' (reemplaza si existe)
    y se asegura de que el tipo MIME sea application/pdf.
    """
    if not asignacion:
        st.error("La asignación es requerida para subir el PDF.")
        return None
    if uploaded_file is None:
        return None

    key_path = f"{asignacion}.pdf"
    file_bytes = uploaded_file.read()

    try:
        # Subida directa
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, file_bytes)
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None

    # ✅ Reajustar tipo MIME usando la API REST de Supabase
    try:
        headers = {
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "apikey": SUPABASE_KEY,
            "Content-Type": "application/json",
        }
        payload = {"contentType": "application/pdf"}
        url = f"{SUPABASE_URL}/storage/v1/object/info/{STORAGE_BUCKET}/{key_path}"
        requests.patch(url, headers=headers, json=payload, timeout=5)
    except Exception:
        pass

    return supabase.storage.from_(STORAGE_BUCKET).get_public_url(key_path)



def build_download_url(public_url: str, asignacion: str | None = None) -> str:
    """Convierte /object/public/ → /object/download/ para forzar descarga"""
    if not public_url:
        return ""
    if "/object/public/" in public_url:
        tail = public_url.split("/object/public/", 1)[1]
        return f"{SUPABASE_URL}/storage/v1/object/download/{tail}"
    if asignacion:
        return f"{SUPABASE_URL}/storage/v1/object/download/{STORAGE_BUCKET}/{asignacion}.pdf"
    return public_url


def url_disponible(url: str) -> bool:
    """Verifica si una URL devuelve 200"""
    if not url:
        return False
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False


# ==============================
# HELPERS DE BASE DE DATOS
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
# PROCESAR ESCANEO (VERSIÓN ESTABLE FINAL)
# ==============================
def process_scan(guia: str):
    match = lookup_by_guia(guia)
    now = datetime.now(TZ)

    if not match:
        insert_no_coincidente(guia)
        st.error(f"⚠️ Guía {guia} no encontrada. Se registró como NO COINCIDENTE.")
        st.rerun()
        return

    asignacion = (match.get("asignacion") or "etiqueta").strip()
    archivo_public = match.get("archivo_adjunto") or ""

    # --- INGRESAR ---
    if st.session_state.page == "ingresar":
        update_ingreso(guia)
        st.success(f"📦 Guía {guia} ingresada correctamente")
        st.rerun()
        return

    # --- IMPRIMIR ---
    if st.session_state.page == "imprimir":
        update_impresion(guia)

        if archivo_public:
            # 🔄 Reemplazar espacios o parámetros extra
            archivo_public = archivo_public.strip()

            # ✅ Descargar automáticamente abriendo en nueva pestaña (más seguro)
            st.markdown(
                f"""
                <script>
                window.open("{archivo_public}?download={asignacion}.pdf", "_blank");
                </script>
                """,
                unsafe_allow_html=True,
            )

            st.success(f"🖨️ Etiqueta {asignacion}.pdf disponible, descarga iniciada.")

            # Actualizar timestamp en BD
            supabase.table(TABLE_NAME).update({
                "fecha_impresion": now.isoformat()
            }).eq("guia", guia).execute()
        else:
            st.warning("⚠️ Etiqueta no disponible para esta guía.")

        # Refrescar log
        st.rerun()



# ==============================
# INTERFAZ GENERAL
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
# SECCIONES PRINCIPALES
# ==============================
# ==============================
# LOG DE ESCANEOS (ingresar / imprimir)
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    df = pd.DataFrame(rows)

    # columnas visibles dinámicas según página
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

    # filtra solo las columnas que existan
    df = df[[c for c in visible_cols if c in df.columns]]

    # convierte URLs en botones de descarga
    if "archivo_adjunto" in df.columns:
        def make_button(url, asignacion="Etiqueta"):
            if url:
                nombre = asignacion if isinstance(asignacion, str) else "Etiqueta"
                return f'<a href="{url}" target="_blank" download="{nombre}.pdf"><button>Descargar</button></a>'
            return "No disponible"

        df["archivo_adjunto"] = [
            make_button(row.get("archivo_adjunto"), row.get("asignacion"))
            for _, row in df.iterrows()
        ]

    # render tabla
    if not df.empty:
        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No hay registros aún.")

    # botón CSV
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Filtered CSV",
        csv,
        f"log_{st.session_state.page}.csv",
        "text/csv"
    )

# ==============================
# CRUD DATOS (simplificado para estabilidad)
# ==============================
if st.session_state.page == "datos":
    st.markdown("### Base de datos completa")

    search = st.text_input("Buscar (asignacion / guia / orden_meli / pack_id / titulo)")
    page_size = st.selectbox("Filas por página", [25,50,100,200], index=1)

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



# ==============================
# LIMPIAR ADJUNTOS INVÁLIDOS
# ==============================
def limpiar_adjuntos_invalidos():
    st.info("🔍 Verificando enlaces de PDFs...")
    res = supabase.table(TABLE_NAME).select("id, archivo_adjunto").neq("archivo_adjunto", None).execute()
    rows = res.data or []
    total = 0
    for r in rows:
        url = r.get("archivo_adjunto")
        if not url_disponible(url):
            supabase.table(TABLE_NAME).update({"archivo_adjunto": None}).eq("id", r["id"]).execute()
            total += 1
    st.success(f"✅ Se limpiaron {total} enlaces inválidos.")

if st.button("🧹 Limpiar adjuntos inválidos"):
    limpiar_adjuntos_invalidos()
