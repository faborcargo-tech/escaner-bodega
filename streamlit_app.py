# streamlit_app.py
# -------------------------------------------------------------
# Escáner Bodega — versión limpia y corregida
# Con token manual para pruebas y sin errores de indentación
# -------------------------------------------------------------

import io
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus
import pandas as pd
import pytz
import requests
import streamlit as st
from supabase import Client, create_client

# ==============================
# ✅ CONFIGURACIÓN GENERAL
# ==============================

st.set_page_config(page_title="Escáner Bodega", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
STORAGE_BUCKET = "etiquetas"
TZ = pytz.timezone("America/Santiago")

# ==============================
# ✅ STORAGE HELPERS
# ==============================

def _get_public_or_signed_url(path: str) -> str | None:
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            return url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        return url
    except Exception:
        return None

def upload_pdf_to_storage(asignacion: str, file_like) -> str | None:
    """Sube/reemplaza PDF como etiquetas/<asignacion>.pdf y retorna su URL pública."""
    if not asignacion or file_like is None:
        return None
    key_path = f"{asignacion}.pdf"
    try:
        data = file_like.read() if hasattr(file_like, "read") else file_like
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, data, {"upsert": "true"})
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None

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
            timeout=5,
        )
    except Exception:
        pass

    return _get_public_or_signed_url(key_path)

def url_disponible(url: str) -> bool:
    if not url:
        return False
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

# ==============================
# ✅ DB HELPERS
# ==============================

def lookup_by_guia(guia: str):
    res = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
    return res.data[0] if res.data else None

def update_ingreso(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update(
        {"fecha_ingreso": now.isoformat(), "estado_escaneo": "INGRESADO CORRECTAMENTE!"}
    ).eq("guia", guia).execute()

def update_impresion(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update(
        {"fecha_impresion": now.isoformat(), "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!"}
    ).eq("guia", guia).execute()

# ==============================
# ✅ ESCANEO
# ==============================

def process_scan(guia: str):
    match = lookup_by_guia(guia)
    if not match:
        st.error(f"⚠️ Guía {guia} no encontrada.")
        return

    if st.session_state.page == "ingresar":
        update_ingreso(guia)
        st.success(f"📦 Guía {guia} ingresada correctamente.")
        return

    if st.session_state.page == "imprimir":
        update_impresion(guia)
        archivo_public = match.get("archivo_adjunto") or ""
        asignacion = (match.get("asignacion") or "etiqueta").strip()

        if archivo_public and url_disponible(archivo_public):
            pdf_bytes = requests.get(archivo_public, timeout=10).content
            if pdf_bytes[:4] == b"%PDF":
                st.success(f"🖨️ Etiqueta {asignacion} lista.")
                st.download_button(
                    f"📄 Descargar {asignacion}.pdf",
                    data=pdf_bytes,
                    file_name=f"{asignacion}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
            else:
                st.warning("⚠️ El archivo no parece un PDF válido.")
        else:
            st.warning("⚠️ No hay etiqueta PDF disponible para esta guía.")

# ==============================
# ✅ PERSISTENCIA DE SECCIÓN
# ==============================

def _get_page_param_default() -> str:
    try:
        qp = st.query_params
        return qp.get("page", ["ingresar"])[0]
    except Exception:
        qp = st.experimental_get_query_params()
        return qp.get("page", ["ingresar"])[0]

def _set_page_param(p: str):
    try:
        st.query_params["page"] = p
    except Exception:
        st.experimental_set_query_params(page=p)

if "page" not in st.session_state:
    st.session_state.page = _get_page_param_default()

def set_page(p: str):
    st.session_state.page = p
    _set_page_param(p)

# ==============================
# ✅ NAV
# ==============================

col1, col2, col3, col4 = st.columns(4)
with col1:
    if st.button("INGRESAR PAQUETES"):
        set_page("ingresar")
with col2:
    if st.button("IMPRIMIR GUIAS"):
        set_page("imprimir")
with col3:
    if st.button("🗃️ DATOS"):
        set_page("datos")
with col4:
    if st.button("🔧 PRUEBAS"):
        set_page("pruebas")

bg = {
    "ingresar": "#71A9D9",
    "imprimir": "#71D999",
    "datos": "#F2F4F4",
    "pruebas": "#F7E1A1",
}.get(st.session_state.page, "#F2F4F4")
st.markdown(f"<style>.stApp{{background-color:{bg};}}</style>", unsafe_allow_html=True)

st.header(
    "📦 INGRESAR PAQUETES"
    if st.session_state.page == "ingresar"
    else (
        "🖨️ IMPRIMIR GUIAS"
        if st.session_state.page == "imprimir"
        else ("🗃️ DATOS" if st.session_state.page == "datos" else "🔧 PRUEBAS")
    )
)

# ============================================================
# 🔧 PRUEBAS — Token manual + impresión etiqueta
# ============================================================

if "meli" not in st.session_state:
    st.session_state.meli = {"access_token": "", "refresh_token": "", "expires_at": 0}

if st.session_state.page == "pruebas":
    st.subheader("Prueba de impresión de etiqueta (no toca la base)")

    # 🧩 CAMPO TEMPORAL PARA PEGAR ACCESS TOKEN MANUAL
    st.markdown("#### Token manual (solo para pruebas)")
    token_manual = st.text_input(
        "Access Token generado externamente (Postman / OAuth manual)",
        value=st.session_state.meli.get("access_token", ""),
        type="password",
        help="Pega aquí un access_token válido de Mercado Libre Chile (ME2).",
    )

    if token_manual.strip():
        st.session_state.meli["access_token"] = token_manual.strip()
        st.success("✅ Token manual cargado. Ya puedes probar impresión.")
    else:
        st.info("Pega tu token manual arriba o conecta con Mercado Libre más abajo.")

    st.markdown("---")

    colI, colO, colP = st.columns([1, 2, 1])
    shipment_id_input = colI.text_input("shipment_id (opcional)")
    order_id_input = colO.text_input("order_id (opcional)")
    pack_id_input = colP.text_input("pack_id (opcional)")

    asignacion_input = st.text_input(
        "asignación (para nombrar PDF si subes a Storage)", placeholder="asignacion-optional"
    )
    subir_storage = st.checkbox("Subir a Storage (reemplaza si existe)")

    if st.button("🔍 Probar", use_container_width=True):
        access_token = st.session_state.meli.get("access_token", "")
        if not access_token:
            st.error("⚠️ No hay Access Token cargado.")
            st.stop()

        shipment_id = shipment_id_input.strip()
        if not shipment_id:
            if order_id_input:
                r = requests.get(
                    f"https://api.mercadolibre.com/orders/{order_id_input}",
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=20,
                )
                if r.status_code == 200:
                    shipment_id = str((r.json().get("shipping") or {}).get("id", ""))
            elif pack_id_input:
                r = requests.get(
                    f"https://api.mercadolibre.com/packs/{pack_id_input}",
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=20,
                )
                if r.status_code == 200:
                    shipment_id = str((r.json().get("shipment") or {}).get("id", ""))

        if not shipment_id:
            st.error("❌ No se pudo determinar shipment_id.")
            st.stop()

        r = requests.get(
            f"https://api.mercadolibre.com/shipment_labels?shipment_ids={shipment_id}&response_type=pdf",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=25,
        )

        if r.status_code == 200 and r.content[:4] == b"%PDF":
            st.success(f"Etiqueta OK — shipment_id={shipment_id}")
            st.download_button(
                "📄 Descargar etiqueta (PDF)",
                data=r.content,
                file_name=f"{(asignacion_input or f'etiqueta_{shipment_id}')}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )
            if subir_storage and asignacion_input.strip():
                url_pdf = upload_pdf_to_storage(asignacion_input.strip(), io.BytesIO(r.content))
                if url_pdf:
                    st.info(f"Subido a Storage: {url_pdf}")
        else:
            st.error(f"❌ Error: {r.status_code} {r.text}")
