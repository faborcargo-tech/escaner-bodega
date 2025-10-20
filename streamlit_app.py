# streamlit_app.py

import io
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
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

def _get_public_url(path: str) -> Optional[str]:
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            return url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        return url  # SDKs antiguos retornan string
    except Exception:
        return None

def upload_pdf_to_storage(asignacion: str, file_like) -> Optional[str]:
    if not asignacion or file_like is None:
        return None
    key_path = f"{asignacion}.pdf"
    try:
        data = file_like.read() if hasattr(file_like, "read") else file_like
        supabase.storage.from_(STORAGE_BUCKET).upload(
            key_path, data, {"upsert": "true", "content-type": "application/pdf"}
        )
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None
    return _get_public_url(key_path)

def url_disponible(url: str) -> bool:
    if not url:
        return False
    try:
        r = requests.head(url, allow_redirects=True, timeout=5)
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
            "orden_meli": "",
            "pack_id": "",
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

        # Log adicional (no romper si falla por RLS)
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
# ✅ LOGS DE ESCANEO (solo en INGRESAR/IMPRIMIR)
# ==============================

if st.session_state.page in ("ingresar", "imprimir"):
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)")
    if st.button("Procesar escaneo"):
        process_scan((scan_val or "").strip())

    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    # Render solo aquí (no en PRUEBAS ni en DATOS)
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

    render_log_with_download_buttons(rows, st.session_state.page)

# =========================================================
# 🔄 SINCRONIZAR VENTAS – pestaña DATOS (tabla única)
# =========================================================

def _meli_headers(token: str, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    h = {"Authorization": f"Bearer {token}"}
    if extra:
        h.update(extra)
    return h

def _meli_get_seller_id(token: str) -> Optional[int]:
    try:
        r = requests.get(
            "https://api.mercadolibre.com/users/me",
            headers=_meli_headers(token),
            timeout=20,
        )
        if r.status_code == 200:
            return (r.json() or {}).get("id")
    except Exception:
        pass
    return None

def _meli_get_order_notes(order_id: str, token: str) -> str:
    """Devuelve la primera nota (asignación FBCXXXX) si existe."""
    try:
        r = requests.get(
            f"https://api.mercadolibre.com/orders/{order_id}/notes",
            headers=_meli_headers(token),
            timeout=20,
        )
        if r.status_code == 200:
            notes = r.json()
            if isinstance(notes, list) and notes:
                return (notes[0].get("note") or "").strip()
    except Exception:
        pass
    return ""

def _meli_get_envio_status(shipment_id: Any, token: str) -> str:
    if not shipment_id:
        return ""
    try:
        r = requests.get(
            f"https://api.mercadolibre.com/shipments/{shipment_id}",
            headers=_meli_headers(token, {"x-format-new": "true"}),
            timeout=20,
        )
        if r.status_code == 200:
            return (r.json() or {}).get("status", "")
    except Exception:
        pass
    return ""

def _meli_get_item_picture(item_id: str, token: str) -> str:
    if not item_id:
        return ""
    try:
        r = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}",
            headers=_meli_headers(token),
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json() or {}
            pics = data.get("pictures") or []
            if pics:
                return pics[0].get("secure_url") or pics[0].get("url") or ""
            return data.get("thumbnail") or ""
    except Exception:
        pass
    return ""

def _map_order_to_row(order: Dict[str, Any], token: str) -> Dict[str, Any]:
    """Mapea una orden de Meli a la fila de Supabase según tu requerimiento."""
    order_id = str(order.get("id", ""))
    pack_id = str(order.get("pack_id") or (order.get("pack") or {}).get("id") or "")
    status = order.get("status", "")

    items = order.get("order_items") or []
    first = items[0] if items else {}
    item_info = first.get("item") or {}

    asin = item_info.get("seller_sku") or item_info.get("seller_custom_field") or ""
    cantidad = first.get("quantity") or 0
    titulo = item_info.get("title") or ""

    shipping_id = (order.get("shipping") or {}).get("id")
    estado_envio = _meli_get_envio_status(shipping_id, token)

    asignacion = _meli_get_order_notes(order_id, token)
    url_imagen = _meli_get_item_picture(item_info.get("id") or "", token)

    return {
        "asignacion": asignacion,   # FBCXXXX (nota)
        "guia": "",                 # manual
        "estado_orden": status,
        "estado_envio": estado_envio,
        "asin": asin,
        "cantidad": cantidad,
        "titulo": titulo,
        "orden_meli": order_id,
        "pack_id": pack_id,
        "url_imagen": url_imagen,
        "archivo_adjunto": "",      # manual o por impresión
    }

def _get_existing_ids_by_order(order_id: str) -> List[int]:
    """Devuelve todos los IDs existentes para un orden_meli (para detectar duplicados previos)."""
    try:
        rows = supabase.table(TABLE_NAME).select("id").eq("orden_meli", order_id).execute().data or []
        return [r["id"] for r in rows if "id" in r]
    except Exception:
        return []

def sync_meli_orders(days: int = 60):
    """Sincroniza órdenes de los últimos `days` días. Evita duplicados por `orden_meli`."""
    token = (st.session_state.get("meli_manual_token") or "").strip()
    if not token:
        st.error("❌ No hay Access Token guardado. Ve a la pestaña PRUEBAS y guarda el token.")
        return

    seller_id = _meli_get_seller_id(token)
    if not seller_id:
        st.error("❌ No se pudo obtener el seller_id con el token indicado.")
        return

    st.info(f"⏳ Sincronizando ventas de los últimos {days} días…")

    now = datetime.utcnow()
    date_from = (now - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00.000-00:00")
    date_to = now.strftime("%Y-%m-%dT23:59:59.999-00:00")

    base_url = "https://api.mercadolibre.com/orders/search"
    limit = 50
    offset = 0
    inserted, updated = 0, 0

    while True:
        params = {
            "seller": seller_id,
            "order.date_created.from": date_from,
            "order.date_created.to": date_to,
            "sort": "date_desc",
            "limit": limit,
            "offset": offset,
        }
        try:
            r = requests.get(base_url, headers=_meli_headers(token), params=params, timeout=40)
        except Exception as e:
            st.error(f"Error de red al consultar órdenes: {e}")
            break

        if r.status_code != 200:
            st.error(f"Error {r.status_code} al consultar órdenes: {r.text[:300]}")
            break

        payload = r.json() or {}
        orders = payload.get("results") or []
        if not orders:
            break

        for order in orders:
            row = _map_order_to_row(order, token)
            order_id = row["orden_meli"]

            try:
                existing_ids = _get_existing_ids_by_order(order_id)
                if existing_ids:
                    # Actualizamos el primero que exista (evitamos insertar duplicados)
                    supabase.table(TABLE_NAME).update({
                        "asignacion": row["asignacion"],
                        "estado_orden": row["estado_orden"],
                        "estado_envio": row["estado_envio"],
                        "asin": row["asin"],
                        "cantidad": row["cantidad"],
                        "titulo": row["titulo"],
                        "pack_id": row["pack_id"],
                        "url_imagen": row["url_imagen"],
                    }).eq("id", existing_ids[0]).execute()
                    updated += 1
                else:
                    supabase.table(TABLE_NAME).insert(row).execute()
                    inserted += 1
            except Exception as e:
                st.warning(f"⚠️ Error guardando orden {order_id}: {e}")

        paging = payload.get("paging") or {}
        total = paging.get("total") or 0
        offset += limit
        if offset >= total:
            break

    st.success(f"✅ Sincronización completa: {inserted} nuevas · {updated} actualizadas.")
    st.session_state.last_sync = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Bloque UI para pestaña DATOS (tabla única)
if st.session_state.page == "datos":
    st.subheader("📦 Sincronización con Mercado Libre")
    colA, colB, colC = st.columns([2, 2, 6])
    with colA:
        if st.button("🔄 Sincronizar ventas (últimos 60 días)", use_container_width=True):
            sync_meli_orders(days=60)
    with colB:
        st.caption(f"Última sincronización: {st.session_state.get('last_sync', '—')}")

    st.markdown("---")
    st.subheader("Tabla de órdenes (últimos 60 días en DB)")
    try:
        # Mostrar lo que haya en la base (descendente por id para ver lo nuevo arriba)
        data = (
            supabase.table(TABLE_NAME)
            .select("*")
            .order("id", desc=True)
            .execute()
            .data
            or []
        )
        df = pd.DataFrame(data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"No se pudo cargar la tabla: {e}")

# =========================================================
# 🔧 PRUEBAS — Token manual + impresión de guía PDF (sin tabla)
# =========================================================

if st.session_state.page == "pruebas":
    st.subheader("Access Token manual")

    access_token = st.text_area(
        "Access Token (pégalo aquí, generado desde Postman)",
        value=st.session_state.get("meli_manual_token", ""),
        height=100,
    )

    cols_tok = st.columns(2)
    with cols_tok[0]:
        if st.button("💾 Guardar token manual", use_container_width=True):
            st.session_state.meli_manual_token = (access_token or "").strip()
            st.success("Token guardado en sesión.")
    with cols_tok[1]:
        if st.button("🗑️ Limpiar token", use_container_width=True):
            st.session_state.meli_manual_token = ""
            st.info("Token limpiado de la sesión.")

    st.markdown("---")
    st.subheader("Probar descarga de etiqueta con token manual")

    col1, col2, col3 = st.columns(3)
    shipment_id = col1.text_input("Shipment ID (opcional)")
    order_id = col2.text_input("Order ID (opcional)")
    pack_id = col3.text_input("Pack ID (opcional)")
    asignacion = st.text_input("Nombre de asignación (para guardar PDF, opcional)")

    if st.button("🔍 Probar impresión de guía", use_container_width=True):
        token = (st.session_state.get("meli_manual_token") or "").strip()
        if not token:
            st.error("Debes guardar un Access Token válido.")
        else:
            headers = {"Authorization": f"Bearer {token}"}
            shipment = (shipment_id or "").strip()

            # Derivar shipment si hace falta
            if not shipment and order_id:
                r = requests.get(
                    f"https://api.mercadolibre.com/orders/{order_id}",
                    headers=headers,
                    timeout=20,
                )
                if r.status_code == 200:
                    shipment = (r.json().get("shipping") or {}).get("id")
            if not shipment and pack_id:
                r = requests.get(
                    f"https://api.mercadolibre.com/packs/{pack_id}",
                    headers=headers,
                    timeout=20,
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
