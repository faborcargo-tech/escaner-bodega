# streamlit_app.py

import io
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import pandas as pd
import pytz
import requests
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
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

MANUAL_FIELDS = ["guia", "archivo_adjunto", "comentario", "descripcion", "orden_amazon"]

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
            "fecha_venta": None,
            "fecha_sincronizacion": None,
            "orden_amazon": "",
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
# 🔄 SINCRONIZAR VENTAS – pestaña DATOS (única tabla)
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

def _parse_ts(s: Optional[str]) -> Optional[str]:
    """Convierte fecha de Meli a ISO local (Santiago) para guardar como timestamp."""
    if not s:
        return None
    try:
        # Meli formato: 2025-10-05T13:45:10.000-04:00
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(TZ).replace(tzinfo=None).isoformat()
    except Exception:
        return None

def _get_order_note(order_id: str, token: str) -> str:
    # Nota de la orden (FBCXXXX)
    try:
        r = requests.get(
            f"https://api.mercadolibre.com/orders/{order_id}/notes",
            headers=_meli_headers(token),
            timeout=15,
        )
        if r.status_code == 200:
            notes = r.json()
            if isinstance(notes, list) and notes:
                note = (notes[0].get("note") or "").strip()
                return note
    except Exception:
        pass
    # fallback (si existiese)
    try:
        r2 = requests.get(
            f"https://api.mercadolibre.com/orders/{order_id}",
            headers=_meli_headers(token),
            timeout=15,
        )
        if r2.status_code == 200:
            data = r2.json() or {}
            # algunos sellers dejan comentario en address.comment
            comment = (((data.get("shipping") or {}).get("receiver_address") or {}).get("comment") or "").strip()
            return comment
    except Exception:
        pass
    return ""

def _get_ship_status(shipment_id: Any, token: str) -> str:
    if not shipment_id:
        return ""
    try:
        r = requests.get(
            f"https://api.mercadolibre.com/shipments/{shipment_id}",
            headers=_meli_headers(token, {"x-format-new": "true"}),
            timeout=15,
        )
        if r.status_code == 200:
            return (r.json() or {}).get("status", "")
    except Exception:
        pass
    return ""

def _get_item_picture(item_id: str, token: str) -> str:
    if not item_id:
        return ""
    try:
        r = requests.get(
            f"https://api.mercadolibre.com/items/{item_id}",
            headers=_meli_headers(token),
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json() or {}
            pics = data.get("pictures") or []
            if pics:
                return pics[0].get("secure_url") or pics[0].get("url") or data.get("thumbnail") or ""
            return data.get("thumbnail") or ""
    except Exception:
        pass
    return ""

def _map_order(order: Dict[str, Any]) -> Tuple[str, str, str, int, str, str, str, str, str]:
    """Extrae datos básicos del payload de /orders/search (sin llamadas extra)."""
    oid = str(order.get("id", ""))
    status = order.get("status", "")
    created = _parse_ts(order.get("date_created"))
    order_items = order.get("order_items") or []
    item = order_items[0] if order_items else {}
    item_info = item.get("item") or {}
    asin = item_info.get("seller_sku") or item_info.get("seller_custom_field") or ""
    qty = int(item.get("quantity") or 0)
    title = item_info.get("title") or ""
    item_id = item_info.get("id") or ""
    pack_id = str(order.get("pack_id") or (order.get("pack") or {}).get("id") or "") or oid
    shipment_id = (order.get("shipping") or {}).get("id")
    return oid, status, created, qty, asin, title, item_id, pack_id, shipment_id

def sync_meli_orders(days: int = 60):
    """Sincroniza órdenes de los últimos `days` días. Evita duplicados por `orden_meli` y
    no toca campos manuales (guia, archivo_adjunto, comentario, descripcion, orden_amazon).
    """
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

        # Pre-procesar mínimo sin llamadas extra
        basics = [_map_order(o) for o in orders]

        # Concurrencia: para notas, estados de envío e imagen
        with ThreadPoolExecutor(max_workers=12) as exe:
            futures = {}
            for (oid, _, _, _, _, _, item_id, _, shipment_id) in basics:
                futures[exe.submit(_get_order_note, oid, token)] = ("note", oid)
                futures[exe.submit(_get_item_picture, item_id, token)] = ("pic", oid)
                if shipment_id:
                    futures[exe.submit(_get_ship_status, shipment_id, token)] = ("ship", oid)

            notes: Dict[str, str] = {}
            pics: Dict[str, str] = {}
            ships: Dict[str, str] = {}
            for fut in as_completed(futures):
                kind, oid = futures[fut]
                try:
                    val = fut.result()
                except Exception:
                    val = ""
                if kind == "note":
                    notes[oid] = val
                elif kind == "pic":
                    pics[oid] = val
                elif kind == "ship":
                    ships[oid] = val

        now_sync_iso = datetime.now(TZ).replace(tzinfo=None).isoformat()

        for (oid, status, created, qty, asin, title, _item_id, pack_id, _shipment_id) in basics:
            asignacion = (notes.get(oid) or "").strip()
            url_imagen = pics.get(oid, "")
            estado_envio = ships.get(oid, "")

            # si no trae pack_id, usar orden_meli
            pack_final = pack_id or oid

            row_sync = {
                "asignacion": asignacion,
                "guia": "",  # manual
                "estado_orden": status,
                "estado_envio": estado_envio,
                "asin": asin,
                "cantidad": qty,
                "titulo": title,
                "orden_meli": oid,
                "pack_id": pack_final,
                "url_imagen": url_imagen,
                "archivo_adjunto": "",  # manual o por impresión
                "fecha_venta": created,
                "fecha_sincronizacion": now_sync_iso,
            }

            try:
                # Comprobar duplicados por orden_meli
                existing = supabase.table(TABLE_NAME).select("id").eq("orden_meli", oid).execute().data or []
                if existing:
                    supabase.table(TABLE_NAME).update({
                        "asignacion": row_sync["asignacion"],
                        "estado_orden": row_sync["estado_orden"],
                        "estado_envio": row_sync["estado_envio"],
                        "asin": row_sync["asin"],
                        "cantidad": row_sync["cantidad"],
                        "titulo": row_sync["titulo"],
                        "pack_id": row_sync["pack_id"],
                        "url_imagen": row_sync["url_imagen"],
                        "fecha_venta": row_sync["fecha_venta"],
                        "fecha_sincronizacion": row_sync["fecha_sincronizacion"],
                    }).eq("id", existing[0]["id"]).execute()
                    updated += 1
                else:
                    supabase.table(TABLE_NAME).insert(row_sync).execute()
                    inserted += 1
            except Exception as e:
                st.warning(f"⚠️ Error guardando orden {oid}: {e}")

        paging = payload.get("paging") or {}
        total = paging.get("total") or 0
        offset += limit
        if offset >= total:
            break

    st.success(f"✅ Sincronización: {inserted} nuevas · {updated} actualizadas.")
    st.session_state.last_sync = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Bloque UI para pestaña DATOS (única tabla + edición + borrar/resincronizar)
if st.session_state.page == "datos":
    st.subheader("📦 Sincronización con Mercado Libre")

    colA, colB, colC = st.columns([2, 2, 6])
    with colA:
        if st.button("🔄 Sincronizar ventas (últimos 60 días)", use_container_width=True):
            sync_meli_orders(days=60)
    with colB:
        if st.button("🗑️ Vaciar y resincronizar 60 días", use_container_width=True):
            try:
                supabase.table(TABLE_NAME).delete().neq("id", -1).execute()
                st.warning("Tabla vaciada. Iniciando resincronización…")
                sync_meli_orders(days=60)
            except Exception as e:
                st.error(f"No se pudo vaciar la tabla: {e}")
    with colC:
        st.caption(f"Última sincronización: {st.session_state.get('last_sync', '—')}")

    st.markdown("---")
    st.subheader("Tabla de órdenes (DB)")

    try:
        # Traer todo y ordenar por fecha_venta desc (más recientes arriba)
        data = (
            supabase.table(TABLE_NAME)
            .select("*")
            .order("fecha_venta", desc=True)
            .order("id", desc=True)  # fallback
            .execute()
            .data
            or []
        )
        df = pd.DataFrame(data)

        # Columna imagen (usando la URL)
        # Para mostrar 100x100 en data_editor, usamos ImageColumn
        from streamlit import column_config as cc

        # Orden de columnas solicitado
        desired_order = [
            "id", "url_imagen", "asignacion", "orden_meli", "pack_id",
            "estado_orden", "estado_envio", "asin", "cantidad", "titulo",
            "guia", "archivo_adjunto", "comentario", "descripcion",
            "orden_amazon", "fecha_venta", "fecha_sincronizacion",
            "fecha_ingreso", "fecha_impresion",
        ]
        # Asegurar columnas faltantes en el DF
        for col in desired_order:
            if col not in df.columns:
                df[col] = None

        df = df[desired_order]

        st.caption("👉 Haz clic en una celda editable, cambia y luego pulsa **Guardar cambios**.")
        edited = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "url_imagen": cc.ImageColumn("Imagen", help="Vista 100x100", width=100),
                "asignacion": cc.TextColumn("Asignación"),
                "orden_meli": cc.TextColumn("Orden ML"),
                "pack_id": cc.TextColumn("Pack ID"),
                "estado_orden": cc.TextColumn("Estado orden"),
                "estado_envio": cc.TextColumn("Estado envío"),
                "asin": cc.TextColumn("ASIN / SKU"),
                "cantidad": cc.NumberColumn("Cant.", step=1),
                "titulo": cc.TextColumn("Título"),
                "guia": cc.TextColumn("Guía (manual)"),
                "archivo_adjunto": cc.TextColumn("Archivo adjunto (URL PDF)"),
                "comentario": cc.TextColumn("Comentario"),
                "descripcion": cc.TextColumn("Descripción"),
                "orden_amazon": cc.TextColumn("Orden Amazon"),
                "fecha_venta": cc.DatetimeColumn("Fecha venta"),
                "fecha_sincronizacion": cc.DatetimeColumn("Fecha sincronización"),
                "fecha_ingreso": cc.DatetimeColumn("Fecha ingreso"),
                "fecha_impresion": cc.DatetimeColumn("Fecha impresión"),
            },
            disabled=[
                "id", "url_imagen", "asignacion", "orden_meli", "pack_id",
                "estado_orden", "estado_envio", "asin", "cantidad", "titulo",
                "fecha_venta", "fecha_sincronizacion", "fecha_ingreso", "fecha_impresion",
            ],  # solo manual_fields se editan
            key="datos_table_editor",
        )

        if st.button("💾 Guardar cambios (campos manuales)", use_container_width=True):
            try:
                # Detectar cambios en campos manuales y actualizar por id
                original = df.set_index("id")
                changed = edited.set_index("id")
                to_update = []
                for idx in changed.index:
                    row_o = original.loc[idx]
                    row_n = changed.loc[idx]
                    updates = {}
                    for f in MANUAL_FIELDS:
                        if str(row_o.get(f)) != str(row_n.get(f)):
                            updates[f] = (row_n.get(f) or "").strip() if isinstance(row_n.get(f), str) else row_n.get(f)
                    if updates:
                        to_update.append((idx, updates))
                if not to_update:
                    st.info("No hay cambios para guardar.")
                else:
                    for (row_id, payload) in to_update:
                        supabase.table(TABLE_NAME).update(payload).eq("id", row_id).execute()
                    st.success(f"✅ Guardado: {len(to_update)} filas actualizadas.")
            except Exception as e:
                st.error(f"No se pudo guardar: {e}")

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
