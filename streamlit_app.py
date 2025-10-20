# streamlit_app.py

import io
import time
import re
import json
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

# Solo estos campos son editables manualmente en DATOS
MANUAL_FIELDS = ["guia", "archivo_adjunto", "descripcion", "orden_amazon"]

# ==============================
# ✅ HELPERS STORAGE / DB
# ==============================

def _get_public_url(path: str) -> Optional[str]:
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            return url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        return url
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
# 🔄 SINCRONIZACIÓN (pestaña DATOS)
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
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(TZ).replace(tzinfo=None).isoformat()
    except Exception:
        return None

def _extract_notes_list(payload: Any) -> List[str]:
    """
    Normaliza la respuesta de /orders/{id}/notes:
    - En tu captura viene como: [ {"results":[{"note":"FBCXXXX", ...}], "order_id": ...} ]
    - También soporta dict simple o lista simple.
    Devuelve lista de strings (priorizando 'note'), sin limpiar.
    """
    texts: List[str] = []

    def pick_from_result(d: Dict[str, Any]):
        # Prioridad exacta a 'note' como pediste:
        if "note" in d and d["note"]:
            texts.append(str(d["note"]))
        else:
            for k in ("text", "plain_text", "description", "message"):
                if d.get(k):
                    texts.append(str(d[k]))

    if isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, dict):
                results = entry.get("results")
                if isinstance(results, list):
                    for res in results:
                        if isinstance(res, dict):
                            pick_from_result(res)
                else:
                    pick_from_result(entry)
            else:
                # elemento lista no dict
                if entry:
                    texts.append(str(entry))
    elif isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            for res in results:
                if isinstance(res, dict):
                    pick_from_result(res)
        else:
            pick_from_result(payload)

    return texts

def _get_order_note(order_id: str, token: str) -> str:
    """
    Devuelve la asignación EXACTA igual a 'note' (en mayúsculas) si existe en /orders/{id}/notes.
    Si no hay nota, intenta otras claves y, como último recurso, escanea /orders/{id}.
    """
    headers = _meli_headers(token)

    # 1) /orders/{id}/notes
    try:
        r = requests.get(
            f"https://api.mercadolibre.com/orders/{order_id}/notes",
            headers=headers,
            timeout=15,
        )
        if r.status_code == 200:
            notes = _extract_notes_list(r.json())
            if notes:
                # Tomamos la última (más reciente) y la normalizamos a MAYÚSCULAS
                return notes[-1].strip().upper()
    except Exception:
        pass

    # 2) Fallback /orders/{id} — solo si no hay ninguna nota
    try:
        r2 = requests.get(
            f"https://api.mercadolibre.com/orders/{order_id}",
            headers=headers,
            timeout=15,
        )
        if r2.status_code == 200:
            raw = json.dumps(r2.json(), ensure_ascii=False)
            # Si por algún motivo quieres aún reconocer FBC… lo dejamos como emergencia
            m = re.search(r"\bFBC[0-9A-Z]{3,}\b", raw, re.IGNORECASE)
            if m:
                return m.group(0).upper()
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
    """Extrae datos base del payload de /orders/search."""
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
    """Sincroniza últimos `days` días. No toca campos manuales; evita duplicados."""
    token = (st.session_state.get("meli_manual_token") or "").strip()
    if not token:
        st.error("❌ No hay Access Token guardado. Ve a PRUEBAS y guarda el token.")
        return

    seller_id = _meli_get_seller_id(token)
    if not seller_id:
        st.error("❌ No se pudo obtener seller_id con el token indicado.")
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

        basics = [_map_order(o) for o in orders]

        # Concurrencia: notas, imagen y estado de envío
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
                    notes[oid] = (val or "").upper()
                elif kind == "pic":
                    pics[oid] = val
                elif kind == "ship":
                    ships[oid] = val

        now_sync_iso = datetime.now(TZ).replace(tzinfo=None).isoformat()

        for (oid, status, created, qty, asin, title, _item_id, pack_id, _shipment_id) in basics:
            asignacion = (notes.get(oid) or "").upper()
            url_imagen = pics.get(oid, "")
            estado_envio = ships.get(oid, "")
            pack_final = pack_id or oid  # fallback a order_id

            row_sync = {
                "asignacion": asignacion,
                "guia": "",
                "estado_orden": status,
                "estado_envio": estado_envio,
                "asin": asin,
                "cantidad": qty,
                "titulo": title,
                "orden_meli": oid,
                "pack_id": pack_final,
                "url_imagen": url_imagen,
                "archivo_adjunto": "",
                "fecha_venta": created,
                "fecha_sincronizacion": now_sync_iso,
            }

            try:
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

# ====== UI DATOS: imagen GRANDE, ASIN link, edición limitada ======

if st.session_state.page == "datos":
    st.subheader("📦 Sincronización con Mercado Libre")
    colA, colC = st.columns([2, 8])
    with colA:
        if st.button("🔄 Sincronizar ventas (últimos 60 días)", use_container_width=True):
            sync_meli_orders(days=60)
    with colC:
        st.caption(f"Última sincronización: {st.session_state.get('last_sync', '—')}")

    st.markdown("---")
    st.subheader("Tabla de órdenes (DB)")

    # Miniaturas grandes (~700 px ancho) y filas altas (~260 px)
    st.markdown("""
        <style>
        [data-testid="stDataEditor"] tbody tr { height: 260px !important; }
        [data-testid="stDataFrame"]  tbody tr { height: 260px !important; }
        [data-testid="stDataEditor"] img { width: 700px !important; height: auto !important; object-fit: contain !important; }
        [data-testid="stDataFrame"]  img { width: 700px !important; height: auto !important; object-fit: contain !important; }
        </style>
    """, unsafe_allow_html=True)

    try:
        data = (
            supabase.table(TABLE_NAME)
            .select("*")
            .order("fecha_venta", desc=True)
            .order("id", desc=True)
            .execute()
            .data
            or []
        )
        df = pd.DataFrame(data)

        # ASIN clickeable -> Amazon
        def asin_to_url(x: Optional[str]) -> Optional[str]:
            x = (x or "").strip()
            if not x:
                return None
            return f"https://www.amazon.com/dp/{x}?th=1"

        df["asin_link"] = df.get("asin", "").apply(asin_to_url)

        # Orden de columnas (imagen→fecha_venta; orden_amazon a la derecha de cantidad)
        desired_order = [
            "id", "url_imagen", "fecha_venta", "asignacion", "orden_meli", "pack_id",
            "estado_orden", "estado_envio", "asin_link", "cantidad", "orden_amazon",
            "titulo", "guia", "archivo_adjunto", "descripcion",
            "comentario", "fecha_sincronizacion", "fecha_ingreso", "fecha_impresion",
        ]
        for col in desired_order:
            if col not in df.columns:
                df[col] = None
        df = df[desired_order]

        from streamlit import column_config as cc

        st.caption("👉 Editables: Guía, Archivo adjunto, Descripción, Orden Amazon. Luego pulsa **Guardar**.")
        edited = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "url_imagen": cc.ImageColumn("Imagen (grande)", help="Miniatura grande", width=700),
                "fecha_venta": cc.DatetimeColumn("Fecha venta"),
                "asignacion": cc.TextColumn("Asignación"),
                "orden_meli": cc.TextColumn("Orden ML"),
                "pack_id": cc.TextColumn("Pack ID"),
                "estado_orden": cc.TextColumn("Estado orden"),
                "estado_envio": cc.TextColumn("Estado envío"),
                "asin_link": cc.LinkColumn("ASIN", help="Abrir en Amazon", width=220),
                "cantidad": cc.NumberColumn("Cant.", step=1),
                "orden_amazon": cc.TextColumn("Orden Amazon"),
                "titulo": cc.TextColumn("Título"),
                "guia": cc.TextColumn("Guía (manual)"),
                "archivo_adjunto": cc.TextColumn("Archivo adjunto (URL PDF)"),
                "descripcion": cc.TextColumn("Descripción"),
                "comentario": cc.TextColumn("Comentario"),
                "fecha_sincronizacion": cc.DatetimeColumn("Fecha sincronización"),
                "fecha_ingreso": cc.DatetimeColumn("Fecha ingreso"),
                "fecha_impresion": cc.DatetimeColumn("Fecha impresión"),
            },
            # 🔒 Deshabilitar TODO excepto los 4 manuales
            disabled=[c for c in df.columns if c not in MANUAL_FIELDS],
            key="datos_table_editor",
        )

        if st.button("💾 Guardar cambios (campos manuales)", use_container_width=True):
            try:
                original = df.set_index("id")
                changed = edited.set_index("id")
                to_update = []
                for idx in changed.index:
                    if idx not in original.index:
                        continue
                    row_o = original.loc[idx]
                    row_n = changed.loc[idx]
                    updates = {}
                    for f in MANUAL_FIELDS:
                        vo = row_o.get(f)
                        vn = row_n.get(f)
                        if str(vo) != str(vn):
                            updates[f] = (vn or "").strip() if isinstance(vn, str) else vn
                    if updates:
                        to_update.append((idx, updates))
                if not to_update:
                    st.info("No hay cambios para guardar.")
                else:
                    for (row_id, payload) in to_update:
                        supabase.table(TABLE_NAME).update(payload).eq("id", int(row_id)).execute()
                    st.success(f"✅ Guardado: {len(to_update)} filas actualizadas.")
            except Exception as e:
                st.error(f"No se pudo guardar: {e}")

    except Exception as e:
        st.error(f"No se pudo cargar la tabla: {e}")

# =========================================================
# 🔧 PRUEBAS — Token manual + prueba de etiqueta + notas de ML
# =========================================================

def upsert_order_note(order_id: str, note_text: str, token: str) -> Tuple[bool, str]:
    """Crea o actualiza la nota de la orden. Si existe, intenta PUT al último note_id; si no, hace POST."""
    headers = _meli_headers(token, {"Content-Type": "application/json"})
    # leer notas existentes
    note_id = None
    try:
        r = requests.get(f"https://api.mercadolibre.com/orders/{order_id}/notes", headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            # normalizar como en _extract_notes_list
            if isinstance(data, list):
                for entry in data:
                    results = (entry or {}).get("results") or []
                    if results:
                        last = results[-1]
                        note_id = last.get("id") or last.get("note_id")
            elif isinstance(data, dict):
                results = data.get("results") or []
                if results:
                    last = results[-1]
                    note_id = last.get("id") or last.get("note_id")
    except Exception:
        pass

    try:
        payload = {"note": note_text}
        if note_id:
            r = requests.put(
                f"https://api.mercadolibre.com/orders/{order_id}/notes/{note_id}",
                headers=headers, json=payload, timeout=20
            )
            if r.status_code in (200, 201):
                return True, "Nota actualizada correctamente."
            return False, f"PUT {r.status_code}: {r.text[:200]}"
        else:
            r = requests.post(
                f"https://api.mercadolibre.com/orders/{order_id}/notes",
                headers=headers, json=payload, timeout=20
            )
            if r.status_code in (200, 201):
                return True, "Nota creada correctamente."
            return False, f"POST {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"Error de red: {e}"

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

    st.markdown("---")
    st.subheader("📝 Prueba de notas de orden (Asignación FBCXXXX)")

    coln1, coln2 = st.columns([2, 3])
    with coln1:
        order_id_notes = st.text_input("Order ID (para notas)")
    with coln2:
        asign_test = st.text_input("Asignación a guardar (FBCXXXX)", placeholder="FBC7759")

    cols_btn = st.columns(3)
    if cols_btn[0].button("👁️ Ver notas", use_container_width=True):
        token = (st.session_state.get("meli_manual_token") or "").strip()
        if not token:
            st.error("Falta token.")
        elif not order_id_notes:
            st.error("Falta Order ID.")
        else:
            r = requests.get(
                f"https://api.mercadolibre.com/orders/{order_id_notes}/notes",
                headers=_meli_headers(token),
                timeout=20
            )
            if r.status_code == 200:
                st.success("Notas actuales:")
                st.json(r.json())
            else:
                st.error(f"Error {r.status_code}: {r.text[:200]}")

    if cols_btn[1].button("💾 Crear/Actualizar nota", use_container_width=True):
        token = (st.session_state.get("meli_manual_token") or "").strip()
        if not token:
            st.error("Falta token.")
        elif not order_id_notes:
            st.error("Falta Order ID.")
        elif not asign_test:
            st.error("Falta texto de asignación.")
        else:
            ok, msg = upsert_order_note(order_id_notes, asign_test, token)
            if ok:
                st.success(msg)
            else:
                st.error(msg)

    if cols_btn[2].button("🔄 Probar extracción de asignación", use_container_width=True):
        token = (st.session_state.get("meli_manual_token") or "").strip()
        if not token:
            st.error("Falta token.")
        elif not order_id_notes:
            st.error("Falta Order ID.")
        else:
            asign = _get_order_note(order_id_notes, token)
            if asign:
                st.success(f"Asignación detectada: **{asign}**")
            else:
                st.warning("No se encontró nota en la orden.")
