# streamlit_app.py

import io
import time
from datetime import datetime, timedelta
import pandas as pd
import pytz
import requests
import streamlit as st
from supabase import Client, create_client

# ==============================
# ✅ CONFIG GENERAL
# ==============================

st.set_page_config(page_title="Escáner Bodega", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
STORAGE_BUCKET = "etiquetas"
TZ = pytz.timezone("America/Santiago")

# ==============================
# ✅ HELPERS STORAGE / DB
# ==============================

def _get_public_url(path: str) -> str | None:
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            return url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        return url
    except Exception:
        return None

def upload_pdf_to_storage(asignacion: str, file_like) -> str | None:
    if not asignacion or file_like is None:
        return None
    key_path = f"{asignacion}.pdf"
    try:
        data = file_like.read() if hasattr(file_like, "read") else file_like
        supabase.storage.from_(STORAGE_BUCKET).upload(
            key_path, data, {"upsert": "true"}
        )
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None
    return _get_public_url(key_path)

def url_disponible(url: str) -> bool:
    if not url:
        return False
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

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

def insert_no_coincidente(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).insert(
        {
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
            "titulo": "",
        }
    ).execute()

def get_logs(page: str):
    cutoff = (datetime.now(TZ) - timedelta(days=60)).isoformat()
    field = "fecha_ingreso" if page == "ingresar" else "fecha_impresion"
    res = (
        supabase.table(TABLE_NAME)
        .select("*")
        .gte(field, cutoff)
        .order(field, desc=True)
        .execute()
    )
    return res.data or []

# ==============================
# ✅ ESCANEO
# ==============================

def process_scan(guia: str):
    match = lookup_by_guia(guia)
    if not match:
        insert_no_coincidente(guia)
        st.error(f"⚠️ Guía {guia} no encontrada. Se registró como NO COINCIDENTE.")
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
            try:
                pdf_bytes = requests.get(archivo_public, timeout=10).content
                if pdf_bytes[:4] == b"%PDF":
                    st.success(f"🖨️ Etiqueta {asignacion} lista.")
                    st.download_button(
                        f"📄 Descargar nuevamente {asignacion}.pdf",
                        data=pdf_bytes,
                        file_name=f"{asignacion}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                else:
                    st.warning("⚠️ El archivo no parece un PDF válido.")
            except Exception:
                st.warning("⚠️ No se pudo descargar el PDF desde Supabase.")
        else:
            st.warning("⚠️ No hay etiqueta PDF disponible para esta guía.")

        try:
            now = datetime.now(TZ).isoformat()
            supabase.table(TABLE_NAME).insert(
                {
                    "asignacion": asignacion,
                    "guia": guia,
                    "fecha_impresion": now,
                    "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!",
                    "archivo_adjunto": archivo_public,
                }
            ).execute()
        except Exception:
            pass

# ==============================
# ✅ PERSISTENCIA / NAV
# ==============================

if "page" not in st.session_state:
    st.session_state.page = "ingresar"

def set_page(p: str):
    st.session_state.page = p

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

# ==============================
# ✅ LOGS DE ESCANEO
# ==============================

def render_log_with_download_buttons(rows: list, page: str):
    if not rows:
        st.info("No hay registros aún.")
        return
    cols = (
        ["Asignación", "Guía", "Fecha impresión", "Estado", "Descargar"]
        if page == "imprimir"
        else ["Asignación", "Guía", "Fecha ingreso", "Estado", "Descargar"]
    )
    hc = st.columns([2, 2, 2, 2, 1])
    for i, h in enumerate(cols):
        hc[i].markdown(f"**{h}**")
    for r in rows:
        asign = r.get("asignacion", "")
        guia = r.get("guia", "")
        fecha = r.get("fecha_impresion") if page == "imprimir" else r.get("fecha_ingreso")
        estado = r.get("estado_escaneo", "")
        url = r.get("archivo_adjunto", "")
        c = st.columns([2, 2, 2, 2, 1])
        c[0].write(asign or "-")
        c[1].write(guia or "-")
        c[2].write((str(fecha)[:19]) if fecha else "-")
        c[3].write(estado or "-")
        if url and url_disponible(url):
            try:
                pdf_bytes = requests.get(url, timeout=8).content
                if pdf_bytes[:4] == b"%PDF":
                    c[4].download_button(
                        "⇩",
                        data=pdf_bytes,
                        file_name=f"{(asign or 'etiqueta')}.pdf",
                        mime="application/pdf",
                        key=f"dl_{asign}_{guia}_{time.time()}",
                    )
                else:
                    c[4].write("No válido")
            except Exception:
                c[4].write("No disponible")
        else:
            c[4].write("No disponible")

if st.session_state.page in ("ingresar", "imprimir"):
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    render_log_with_download_buttons(rows, st.session_state.page)

# =========================================================
# 🔧 PRUEBAS — Token manual + impresión de guía PDF
# =========================================================

if st.session_state.page == "pruebas":
    st.subheader("Probar descarga de etiqueta con token manual")

    access_token = st.text_area(
        "Access Token (pégalo aquí, generado desde Postman)",
        value=st.session_state.get("meli_manual_token", ""),
        height=100,
    )
    if access_token:
        st.session_state.meli_manual_token = access_token.strip()

    col1, col2, col3 = st.columns(3)
    shipment_id = col1.text_input("Shipment ID (opcional)")
    order_id = col2.text_input("Order ID (opcional)")
    pack_id = col3.text_input("Pack ID (opcional)")
    asignacion = st.text_input("Nombre de asignación (para guardar PDF, opcional)")

    if st.button("🔍 Probar impresión de guía", use_container_width=True):
        if not access_token:
            st.error("Debes ingresar un Access Token válido.")
        else:
            token = access_token.strip()
            headers = {"Authorization": f"Bearer {token}"}
            shipment = shipment_id.strip()

            # Si no se ingresó shipment, intentar derivar desde order/pack
            if not shipment and order_id:
                r = requests.get(
                    f"https://api.mercadolibre.com/orders/{order_id}", headers=headers, timeout=20
                )
                if r.status_code == 200:
                    shipment = (r.json().get("shipping") or {}).get("id")
            if not shipment and pack_id:
                r = requests.get(
                    f"https://api.mercadolibre.com/packs/{pack_id}", headers=headers, timeout=20
                )
                if r.status_code == 200:
                    shipment = (r.json().get("shipment") or {}).get("id")

            if not shipment:
                st.error("No se pudo derivar shipment_id desde order/pack.")
            else:
                r = requests.get(
                    "https://api.mercadolibre.com/shipment_labels",
                    params={"shipment_ids": shipment, "response_type": "pdf"},
                    headers=headers,
                    timeout=25,
                )
                if r.status_code == 200 and r.content[:4] == b"%PDF":
                    st.success(f"Etiqueta lista (shipment_id={shipment}).")
                    st.download_button(
                        "📄 Descargar etiqueta (PDF)",
                        data=r.content,
                        file_name=f"{(asignacion or f'etiqueta_{shipment}')}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                else:
                    st.error(f"Error {r.status_code}: {r.text[:300]}")
