# ================================================
#  Escáner Bodega — streamlit_app.py
#  NOTA:
#  Bloques marcados >>> NO TOCAR <<< están estabilizados.
# ================================================

import io
import time
import re
import json
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import pytz
import requests
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import Client, create_client

# ==============================
# ✅ CONFIG GENERAL  (>>> NO TOCAR <<<)
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
# ✅ UTILS (hora Chile + toast)
# ==============================

def now_chile_iso_naive() -> str:
    """Fecha/hora de Chile, sin tzinfo (para columnas timestamp Supabase)."""
    return datetime.now(TZ).replace(tzinfo=None).isoformat()

def toast(msg: str, icon: str = "✅"):
    """Notificación corta no intrusiva."""
    try:
        st.toast(msg, icon=icon)
    except Exception:
        if icon == "✅":
            st.success(msg)
        elif icon == "⚠️":
            st.warning(msg)
        else:
            st.info(msg)

# ==============================
# ✅ HELPERS STORAGE / DB  (>>> NO TOCAR <<<)
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
    """Sube PDF (si es válido) y retorna URL pública."""
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
    supabase.table(TABLE_NAME).update(
        {"fecha_ingreso": now_chile_iso_naive(), "estado_escaneo": "INGRESADO CORRECTAMENTE!"}
    ).eq("guia", guia).execute()

def set_impreso_ok(guia: str, archivo_public: str):
    supabase.table(TABLE_NAME).update(
        {
            "fecha_impresion": now_chile_iso_naive(),
            "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!",
            "archivo_adjunto": archivo_public,
        }
    ).eq("guia", guia).execute()

def insert_no_coincidente(guia: str):
    supabase.table(TABLE_NAME).insert(
        {
            "asignacion": "",
            "guia": guia,
            "fecha_ingreso": now_chile_iso_naive(),
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
    cutoff = (datetime.now(TZ) - timedelta(days=60)).replace(tzinfo=None).isoformat()
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
# ✅ MERCADO LIBRE helpers (token manual en sesión)  (>>> NO TOCAR <<<)
# ==============================

def _meli_headers(token: str, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    h = {"Authorization": f"Bearer {token}"}
    if extra:
        h.update(extra)
    return h

def derive_shipment_id(token: str, order_id: Optional[str], pack_id: Optional[str]) -> Optional[str]:
    headers = _meli_headers(token)
    if order_id:
        try:
            r = requests.get(f"https://api.mercadolibre.com/orders/{order_id}", headers=headers, timeout=20)
            if r.status_code == 200:
                sid = (r.json().get("shipping") or {}).get("id")
                if sid:
                    return str(sid)
        except Exception:
            pass
    if pack_id:
        try:
            r = requests.get(f"https://api.mercadolibre.com/packs/{pack_id}", headers=headers, timeout=20)
            if r.status_code == 200:
                sid = (r.json().get("shipment") or {}).get("id")
                if sid:
                    return str(sid)
        except Exception:
            pass
    return None

def download_label_pdf(token: str, shipment_id: str) -> Tuple[Optional[bytes], Optional[str]]:
    try:
        r = requests.get(
            "https://api.mercadolibre.com/shipment_labels",
            params={"shipment_ids": shipment_id, "response_type": "pdf"},
            headers=_meli_headers(token, {"Accept": "application/pdf"}),
            timeout=25,
        )
        if r.status_code == 200 and r.content[:4] == b"%PDF":
            return r.content, None
        try:
            j = r.json()
        except Exception:
            j = {"message": r.text[:300]}
        err = j.get("message") or j.get("error") or str(j)[:300]
        if r.status_code == 400 and "not_printable_status" in str(j):
            err = "400 not_printable_status: el envío no está listo para imprimir."
        elif r.status_code == 429:
            err = "429 local_rate_limited: intenta en unos segundos."
        elif r.status_code == 404:
            err = "404 not_found: revisa order/pack."
        return None, f"Error {r.status_code}: {err}"
    except Exception as e:
        return None, f"Error de red: {e}"

# ==============================
# ✅ ESCANEO (INGRESAR / IMPRIMIR)
# ==============================

def process_scan(guia: str):
    guia = (guia or "").strip()
    if not guia:
        toast("Ingresa una guía válida.", "⚠️")
        return

    match = lookup_by_guia(guia)
    if not match:
        insert_no_coincidente(guia)
        toast(f"Guía {guia} no encontrada. Se registró como NO COINCIDENTE.", "⚠️")
        return

    # ----- INGRESAR -----
    if st.session_state.page == "ingresar":
        update_ingreso(guia)
        toast(f"Guía {guia} ingresada correctamente.", "✅")
        return

    # ----- IMPRIMIR -----
    if st.session_state.page == "imprimir":
        asignacion = (match.get("asignacion") or "etiqueta").strip() or "etiqueta"
        archivo_public = match.get("archivo_adjunto") or ""

        # 1) Si ya hay PDF válido -> botón
        if archivo_public and url_disponible(archivo_public):
            try:
                pdf_bytes = requests.get(archivo_public, timeout=10).content
                if pdf_bytes[:4] == b"%PDF":
                    st.success(f"🖨️ Etiqueta {asignacion} lista para descargar.")
                    st.download_button(
                        f"📄 Descargar {asignacion}.pdf",
                        data=pdf_bytes,
                        file_name=f"{asignacion}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                    toast("Etiqueta disponible desde almacenamiento.", "✅")
                    return
                else:
                    st.warning("⚠️ El archivo no parece un PDF válido. Intentaré descargar una nueva etiqueta…")
            except Exception:
                st.warning("⚠️ No se pudo descargar el PDF desde Storage. Intentaré obtener una nueva etiqueta…")

        # 2) Token manual guardado en PRUEBAS
        token = (st.session_state.get("meli_manual_token") or "").strip()
        if not token:
            st.error("No hay token guardado (PRUEBAS). Guarda uno para poder imprimir.")
            toast("Falta token para imprimir.", "⚠️")
            return

        order_id = (match.get("orden_meli") or "").strip() or None
        pack_id = (match.get("pack_id") or "").strip() or None
        shipment_id = derive_shipment_id(token, order_id, pack_id)
        if not shipment_id:
            st.error("No se pudo derivar shipment_id desde Order/Pack.")
            toast("No se pudo derivar shipment_id.", "⚠️")
            return

        pdf, err = download_label_pdf(token, shipment_id)
        if pdf is None:
            st.error(err or "No se pudo descargar la etiqueta.")
            toast("No se imprimió. Revisa el estado del envío.", "⚠️")
            return

        # 3) Subir a Storage y actualizar fila
        url_publica = upload_pdf_to_storage(asignacion, pdf)
        if not url_publica:
            st.error("No se pudo subir la etiqueta a Storage.")
            toast("No se pudo subir el PDF.", "⚠️")
            return

        set_impreso_ok(guia, url_publica)

        st.success(f"Etiqueta lista (shipment_id={shipment_id}).")
        st.download_button(
            "📄 Descargar etiqueta (PDF)",
            data=pdf,
            file_name=f"{asignacion}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
        toast("Impresión registrada correctamente.", "✅")
        return

# ==============================
# ✅ PERSISTENCIA / NAV  (>>> NO TOCAR <<<)
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
# ✅ CAJA DE ESCANEO (limpieza segura en ambas páginas)
# ==============================

if st.session_state.page in ("ingresar", "imprimir"):
    scan_key = f"scan_{st.session_state.page}"
    clear_flag = f"{scan_key}_clear"

    # Limpiar en el run siguiente (evita StreamlitAPIException)
    if st.session_state.get(clear_flag):
        st.session_state[scan_key] = ""
        del st.session_state[clear_flag]

    st.text_area("Escanea aquí (o pega el número de guía)", key=scan_key)

    if st.button("Procesar escaneo"):
        val = (st.session_state.get(scan_key) or "").strip()
        process_scan(val)
        st.session_state[clear_flag] = True
        try:
            st.rerun()
        except Exception:
            st.experimental_rerun()

    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    # >>> NO TOCAR <<<
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

def _meli_get_seller_id(token: str) -> Optional[int]:
    try:
        r = requests.get("https://api.mercadolibre.com/users/me", headers=_meli_headers(token), timeout=20)
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

# ---- Notas FBC (igual a "note") ----
def _extract_notes_list(payload: Any) -> List[str]:
    texts: List[str] = []
    def pick_from_result(d: Dict[str, Any]):
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
            elif entry:
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
    headers = _meli_headers(token)
    try:
        r = requests.get(f"https://api.mercadolibre.com/orders/{order_id}/notes", headers=headers, timeout=15)
        if r.status_code == 200:
            notes = _extract_notes_list(r.json())
            if notes:
                return notes[-1].strip().upper()
    except Exception:
        pass
    try:
        r2 = requests.get(f"https://api.mercadolibre.com/orders/{order_id}", headers=headers, timeout=15)
        if r2.status_code == 200:
            raw = json.dumps(r2.json(), ensure_ascii=False)
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
        r = requests.get(f"https://api.mercadolibre.com/items/{item_id}", headers=_meli_headers(token), timeout=15)
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
    oid = str(order.get("id", ""))
    pays = order.get("payments") or []
    pay_status = (pays[0] or {}).get("status") if isinstance(pays, list) and pays else None
    status = pay_status or order.get("status", "")
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

        now_sync_iso = now_chile_iso_naive()

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

# ====== UI DATOS ======

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

    # CSS: miniatura grande pero columna angosta (~160 px)
    st.markdown("""
        <style>
        [data-testid="stDataEditor"] tbody tr,
        [data-testid="stDataFrame"]  tbody tr { height: 180px !important; }
        [data-testid="stDataEditor"] img,
        [data-testid="stDataFrame"]  img {
            max-width: 160px !important;
            height: auto !important;
            object-fit: contain !important;
            display: block; margin: 0 auto;
        }
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

        # -------- Filtros --------
        with st.expander("🔎 Filtros", expanded=False):
            estados_orden_all = sorted([x for x in df.get("estado_orden", []).dropna().unique().tolist()])
            estados_envio_all = sorted([x for x in df.get("estado_envio", []).dropna().unique().tolist()])
            asign_all = sorted([x for x in df.get("asignacion", []).dropna().unique().tolist()])

            c1, c2, c3 = st.columns(3)
            sel_est_orden = c1.multiselect("Estado orden", estados_orden_all, default=[])
            sel_est_envio = c2.multiselect("Estado envío", estados_envio_all, default=[])
            sel_asign = c3.multiselect("Asignación (FBC…)", asign_all, default=[])

            c4, c5, c6 = st.columns(3)
            txt_asin = c4.text_input("Filtrar ASIN (contiene)")
            txt_titulo = c5.text_input("Filtrar Título (contiene)")
            txt_pack = c6.text_input("Pack ID / Orden ML (contiene)")

            if "fecha_venta" in df.columns and not df["fecha_venta"].isna().all():
                min_dt = pd.to_datetime(df["fecha_venta"]).min().date()
                max_dt = pd.to_datetime(df["fecha_venta"]).max().date()
            else:
                today = date.today()
                min_dt, max_dt = today - timedelta(days=60), today
            date_from, date_to = st.slider(
                "Rango de fecha de venta",
                min_value=min_dt, max_value=max_dt,
                value=(min_dt, max_dt)
            )

        if sel_est_orden:
            df = df[df["estado_orden"].isin(sel_est_orden)]
        if sel_est_envio:
            df = df[df["estado_envio"].isin(sel_est_envio)]
        if sel_asign:
            df = df[df["asignacion"].isin(sel_asign)]
        if txt_asin:
            df = df[df["asin"].fillna("").str.contains(txt_asin, case=False, na=False)]
        if txt_titulo:
            df = df[df["titulo"].fillna("").str.contains(txt_titulo, case=False, na=False)]
        if txt_pack:
            df = df[
                df["pack_id"].fillna("").str.contains(txt_pack, case=False, na=False) |
                df["orden_meli"].fillna("").str.contains(txt_pack, case=False, na=False)
            ]
        if "fecha_venta" in df.columns:
            dt = pd.to_datetime(df["fecha_venta"], errors="coerce").dt.date
            df = df[(dt >= date_from) & (dt <= date_to)]

        # -------- Columnas derivadas (links) --------
        def asin_url_row(row) -> Optional[str]:
            asin = (row.get("asin") or "").strip()
            qty = int(row.get("cantidad") or 1)
            if not asin:
                return None
            return f"https://www.amazon.com/dp/{asin}?th={qty}"
        df["asin_link"] = df.apply(asin_url_row, axis=1)

        df["orden_meli_link"] = df["orden_meli"].apply(
            lambda x: f"https://www.mercadolibre.cl/ventas/{x}/detalle" if x else None
        )
        df["pack_id_link"] = df["pack_id"].apply(
            lambda x: f"https://www.mercadolibre.cl/ventas/{x}/detalle" if x else None
        )
        df["orden_amazon_link"] = df["orden_amazon"].apply(
            lambda x: f"https://www.amazon.com/your-orders/order-details?orderID={x}" if x else None
        )

        # Orden columnas (Guía ANTES de Título)
        desired_order = [
            "id",
            "url_imagen",
            "fecha_venta",
            "asignacion",
            "orden_meli", "orden_meli_link",
            "pack_id", "pack_id_link",
            "estado_orden",
            "estado_envio",
            "asin_link", "cantidad", "orden_amazon", "orden_amazon_link",
            "guia", "titulo",
            "archivo_adjunto", "descripcion",
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
                "url_imagen": cc.ImageColumn("Imagen", help="Miniatura", width=160),
                "fecha_venta": cc.DatetimeColumn("Fecha venta"),
                "asignacion": cc.TextColumn("Asignación"),
                "orden_meli": cc.TextColumn("Orden ML"),
                "orden_meli_link": cc.LinkColumn("🔗 Orden", help="Abrir en Mercado Libre", width=90, max_chars=30),
                "pack_id": cc.TextColumn("Pack ID"),
                "pack_id_link": cc.LinkColumn("🔗 Pack", help="Abrir en Mercado Libre", width=90, max_chars=30),
                "estado_orden": cc.TextColumn("Estado orden"),
                "estado_envio": cc.TextColumn("Estado envío"),
                "asin_link": cc.LinkColumn("ASIN", help="Abrir en Amazon (th=cantidad)", width=220, max_chars=40),
                "cantidad": cc.NumberColumn("Cant.", step=1),
                "orden_amazon": cc.TextColumn("Orden Amazon"),
                "orden_amazon_link": cc.LinkColumn("🔗 Amazon", help="Abrir detalle Amazon", width=110, max_chars=40),
                "guia": cc.TextColumn("Guía (manual)"),
                "titulo": cc.TextColumn("Título"),
                "archivo_adjunto": cc.TextColumn("Archivo adjunto (URL PDF)"),
                "descripcion": cc.TextColumn("Descripción"),
                "comentario": cc.TextColumn("Comentario"),
                "fecha_sincronizacion": cc.DatetimeColumn("Fecha sincronización"),
                "fecha_ingreso": cc.DatetimeColumn("Fecha ingreso"),
                "fecha_impresion": cc.DatetimeColumn("Fecha impresión"),
            },
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
# 🔧 PRUEBAS — Token manual + etiqueta + notas ML
# =========================================================

def upsert_order_note(order_id: str, note_text: str, token: str) -> Tuple[bool, str]:
    """Crea o actualiza la nota (note) de la orden."""
    headers = _meli_headers(token, {"Content-Type": "application/json"})
    note_id = None
    try:
        r = requests.get(f"https://api.mercadolibre.com/orders/{order_id}/notes", headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
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
                    headers=headers, timeout=20,
                )
                if r.status_code == 200:
                    shipment = (r.json().get("shipping") or {}).get("id")
            if not shipment and pack_id:
                r = requests.get(
                    f"https://api.mercadolibre.com/packs/{pack_id}",
                    headers=headers, timeout=20,
                )
                if r.status_code == 200:
                    shipment = (r.json().get("shipment") or {}).get("id")

            if not shipment:
                st.error("No se pudo derivar shipment_id desde order/pack.")
            else:
                r = requests.get(
                    "https://api.mercadolibre.com/shipment_labels",
                    params={"shipment_ids": shipment, "response_type": "pdf"},
                    headers=headers, timeout=25,
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
                headers=_meli_headers(token), timeout=20
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
