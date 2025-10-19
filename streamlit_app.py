# streamlit_app.py
import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import requests
import time
import io
import json
import os

# ==============================
# ✅ BLOQUE ESTABLE — CONFIGURACIÓN GENERAL (NO MODIFICAR)
# ==============================

st.set_page_config(page_title="Escáner Bodega", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
STORAGE_BUCKET = "etiquetas"
TZ = pytz.timezone("America/Santiago")

# ==============================
# ✅ BLOQUE ESTABLE — STORAGE (NO MODIFICAR)
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
    """Sube/reemplaza PDF como etiquetas/<asignacion>.pdf y retorna su URL pública."""
    if not asignacion or uploaded_file is None:
        return None
    key_path = f"{asignacion}.pdf"
    file_bytes = uploaded_file.read()
    try:
        # upsert para evitar error de duplicado
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, file_bytes, {"upsert": "true"})
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None

    # Forzar MIME application/pdf para descargas correctas
    try:
        headers = {"Authorization": f"Bearer {SUPABASE_KEY}", "apikey": SUPABASE_KEY, "Content-Type": "application/json"}
        requests.patch(
            f"{SUPABASE_URL}/storage/v1/object/info/{STORAGE_BUCKET}/{key_path}",
            headers=headers, json={"contentType": "application/pdf"}, timeout=5
        )
    except Exception:
        pass

    return _get_public_or_signed_url(key_path)

# 🔸 NUEVO helper para bytes (misma semántica que upload_pdf_to_storage)
def upload_pdf_bytes_to_storage(asignacion: str, pdf_bytes: bytes) -> str | None:
    if not asignacion or not pdf_bytes:
        return None
    key_path = f"{asignacion}.pdf"
    try:
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, pdf_bytes, {"upsert": "true"})
    except Exception as e:
        st.error(f"❌ Error subiendo PDF (bytes): {e}")
        return None
    try:
        headers = {"Authorization": f"Bearer {SUPABASE_KEY}", "apikey": SUPABASE_KEY, "Content-Type": "application/json"}
        requests.patch(
            f"{SUPABASE_URL}/storage/v1/object/info/{STORAGE_BUCKET}/{key_path}",
            headers=headers, json={"contentType": "application/pdf"}, timeout=5
        )
    except Exception:
        pass
    return _get_public_or_signed_url(key_path)

def url_disponible(url: str) -> bool:
    """HEAD 200 OK -> existe."""
    if not url:
        return False
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

# ==============================
# ✅ BLOQUE ESTABLE — DB HELPERS (NO MODIFICAR)
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
    """Devuelve últimos 60 días de acuerdo a la sección (ingreso/impresión)."""
    cutoff = (datetime.now(TZ) - timedelta(days=60)).isoformat()
    field = "fecha_ingreso" if page == "ingresar" else "fecha_impresion"
    res = supabase.table(TABLE_NAME).select("*").gte(field, cutoff).order(field, desc=True).execute()
    return res.data or []

# ==============================
# 🔵 NUEVO BLOQUE — MERCADO LIBRE ME2 (SOLO USADO EN "IMPRIMIR" Y "PRUEBAS")
# ==============================

TOKENS_PATH = "meli_tokens.json"

def _read_secrets_block():
    # Soporta dos formatos en secrets.
    app_id = st.secrets.get("MELI_APP_ID") or st.secrets.get("meli", {}).get("app_id")
    client_secret = st.secrets.get("MELI_CLIENT_SECRET") or st.secrets.get("meli", {}).get("client_secret")
    access_token = st.secrets.get("MELI_ACCESS_TOKEN") or st.secrets.get("meli", {}).get("access_token")
    refresh_token = st.secrets.get("MELI_REFRESH_TOKEN") or st.secrets.get("meli", {}).get("refresh_token")
    return app_id, client_secret, access_token, refresh_token

def _load_tokens():
    app_id, client_secret, acc, ref = _read_secrets_block()
    data = {"app_id": app_id, "client_secret": client_secret, "access_token": acc, "refresh_token": ref, "expires_at": 0}
    if os.path.exists(TOKENS_PATH):
        try:
            with open(TOKENS_PATH, "r", encoding="utf-8") as f:
                disk = json.load(f)
            for k in ["access_token", "refresh_token", "expires_at"]:
                if disk.get(k):
                    data[k] = disk[k]
        except Exception:
            pass
    return data

def _save_tokens(tok: dict):
    try:
        with open(TOKENS_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "access_token": tok.get("access_token"),
                "refresh_token": tok.get("refresh_token"),
                "expires_at": tok.get("expires_at", 0)
            }, f)
    except Exception:
        pass

def _refresh_access_token(tokens: dict) -> dict:
    url = "https://api.mercadolibre.com/oauth/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": tokens["app_id"],
        "client_secret": tokens["client_secret"],
        "refresh_token": tokens["refresh_token"]
    }
    r = requests.post(url, data=payload, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"Refresh token falló: {r.status_code} {r.text}")
    j = r.json()
    tokens["access_token"] = j["access_token"]
    tokens["refresh_token"] = j.get("refresh_token", tokens["refresh_token"])
    tokens["expires_at"] = int(time.time()) + int(j.get("expires_in", 10800)) - 60
    _save_tokens(tokens)
    return tokens

def _get_access_token() -> str:
    tok = _load_tokens()
    if not tok["access_token"]:
        raise RuntimeError("No hay ACCESS_TOKEN en secrets o tokens locales.")
    if int(time.time()) >= int(tok.get("expires_at", 0)) and tok.get("refresh_token"):
        try:
            tok = _refresh_access_token(tok)
        except Exception:
            pass
    _save_tokens(tok)
    return tok["access_token"]

def _meli_headers():
    return {"Authorization": f"Bearer {_get_access_token()}"}

def _meli_request(method: str, url: str, headers: dict | None = None, params=None, data=None, json_body=None, x_format_new: bool = False, retry_on_401: bool = True):
    h = dict(headers or {})
    h.update(_meli_headers())
    if x_format_new:
        h["x-format-new"] = "true"
    r = requests.request(method, url, headers=h, params=params, data=data, json=json_body, timeout=20)
    if r.status_code == 401 and retry_on_401:
        tok = _load_tokens()
        if tok.get("refresh_token"):
            _refresh_access_token(tok)
            h.update({"Authorization": f"Bearer {tok['access_token']}"})
            r = requests.request(method, url, headers=h, params=params, data=data, json=json_body, timeout=20)
    return r

def _meli_get_user_id():
    url = "https://api.mercadolibre.com/users/me"
    r = _meli_request("GET", url)
    if r.status_code == 200:
        return r.json().get("id")
    return None

def _meli_get_order(order_id: str) -> dict | None:
    url = f"https://api.mercadolibre.com/orders/{order_id}"
    r = _meli_request("GET", url)
    if r.status_code == 200:
        return r.json()
    return None

def _meli_get_pack(pack_id: str) -> dict | None:
    url = f"https://api.mercadolibre.com/packs/{pack_id}"
    r = _meli_request("GET", url)
    if r.status_code == 200:
        return r.json()
    return None

def _meli_get_shipment(shipment_id: str) -> dict | None:
    url = f"https://api.mercadolibre.com/shipments/{shipment_id}"
    r = _meli_request("GET", url, x_format_new=True)
    if r.status_code == 200:
        return r.json()
    return None

def _meli_ready_to_ship(shipment_id: str) -> bool:
    url = f"https://api.mercadolibre.com/shipments/{shipment_id}/process/ready_to_ship"
    r = _meli_request("POST", url)
    return r.status_code == 200

def _shipment_id_from_order(order_id: str) -> str | None:
    od = _meli_get_order(order_id)
    if not od:
        return None
    ship = (od.get("shipping") or {}).get("id")
    if ship:
        return str(ship)
    return None

def _shipment_id_from_pack(pack_id: str) -> str | None:
    pk = _meli_get_pack(pack_id)
    if not pk:
        return None
    ship = (pk.get("shipment") or {}).get("id")
    if ship:
        return str(ship)
    return None

def _explicacion_estado_label(sh: dict) -> str | None:
    mode = (sh.get("logistic") or {}).get("mode")
    ltype = (sh.get("logistic") or {}).get("type")
    status = sh.get("status")
    sub = sh.get("substatus")

    if mode != "me2":
        return "El envío no es ME2."
    if ltype == "fulfillment":
        return "Fulfillment: solo imprime etiqueta de stock (no de envío)."
    if status != "ready_to_ship":
        if sub == "buffered":
            date = (((sh.get("lead_time") or {}).get("buffering") or {}).get("date"))
            return f"Buffering: la etiqueta se habilita el {date}."
        return f"Estado no imprimible: {status}."
    if sub not in ("ready_to_print", "printed"):
        return f"Subestado no imprimible: {sub}."
    return None  # OK imprimible

def _download_label_pdf(shipment_id: str) -> bytes | None:
    url = "https://api.mercadolibre.com/shipment_labels"
    params = {"shipment_ids": shipment_id, "response_type": "pdf"}
    r = _meli_request("GET", url, params=params)
    if r.status_code == 200 and r.content[:4] == b"%PDF":
        return r.content
    return None

# ==============================
# ✅ BLOQUE ESTABLE — ESCANEO (NO AUTO-ABRIR PDF) (NO MODIFICAR)
#     🔸 Se añadió SOLO dentro de la rama "imprimir": intento ME2 si no hay PDF válido
# ==============================

def process_scan(guia: str):
    match = lookup_by_guia(guia)
    if not match:
        insert_no_coincidente(guia)
        st.error(f"⚠️ Guía {guia} no encontrada. Se registró como NO COINCIDENTE.")
        return

    # MODO INGRESAR
    if st.session_state.page == "ingresar":
        update_ingreso(guia)
        st.success(f"📦 Guía {guia} ingresada correctamente.")
        return

    # MODO IMPRIMIR (sin auto-abrir PDF)
    if st.session_state.page == "imprimir":
        update_impresion(guia)

        archivo_public = match.get("archivo_adjunto") or ""
        asignacion = (match.get("asignacion") or "etiqueta").strip()

        # 1) Mostrar botón de descarga si ya hay PDF en storage
        etiqueta_ok = False
        if archivo_public and url_disponible(archivo_public):
            try:
                pdf_bytes = requests.get(archivo_public, timeout=10).content
                if pdf_bytes[:4] == b"%PDF":
                    st.success(f"🖨️ Etiqueta {asignacion} lista (Storage).")
                    st.download_button(
                        label=f"📄 Descargar nuevamente {asignacion}.pdf",
                        data=pdf_bytes,
                        file_name=f"{asignacion}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                    etiqueta_ok = True
                else:
                    st.warning("⚠️ El archivo en storage no parece un PDF válido.")
            except Exception:
                st.warning("⚠️ No se pudo descargar el PDF desde Supabase.")

        # 2) Si no hay PDF válido, intentamos ME2 con orden_meli / pack_id
        if not etiqueta_ok:
            try:
                orden_meli = (match.get("orden_meli") or "").strip()
                pack_id = (match.get("pack_id") or "").strip()

                sid = None
                if pack_id:
                    try:
                        sid = _shipment_id_from_pack(pack_id)
                    except Exception as e:
                        st.info(f"ℹ️ No se pudo obtener shipment desde pack {pack_id}: {e}")
                if not sid and orden_meli:
                    try:
                        sid = _shipment_id_from_order(orden_meli)
                    except Exception as e:
                        st.info(f"ℹ️ No se pudo obtener shipment desde orden {orden_meli}: {e}")

                if sid:
                    st.info(f"Shipment ID detectado: {sid}")
                    sh = _meli_get_shipment(sid)
                    if not sh:
                        st.warning("⚠️ No se pudo leer el detalle del shipment.")
                    else:
                        cause = _explicacion_estado_label(sh)
                        if cause:
                            st.warning(f"🚫 Aún no imprimible: {cause}")
                        else:
                            pdf = _download_label_pdf(sid)
                            if pdf:
                                st.success("🖨️ Etiqueta generada desde ME2.")
                                url_pdf = upload_pdf_bytes_to_storage(asignacion, pdf)
                                if url_pdf:
                                    try:
                                        supabase.table(TABLE_NAME).update({"archivo_adjunto": url_pdf}).eq("guia", guia).execute()
                                    except Exception:
                                        pass
                                    st.download_button(
                                        label=f"📄 Descargar {asignacion}.pdf",
                                        data=pdf,
                                        file_name=f"{asignacion}.pdf",
                                        mime="application/pdf",
                                        use_container_width=True,
                                    )
                                else:
                                    st.download_button(
                                        label=f"📄 Descargar {asignacion}.pdf",
                                        data=pdf,
                                        file_name=f"{asignacion}.pdf",
                                        mime="application/pdf",
                                        use_container_width=True,
                                    )
                                etiqueta_ok = True
                            else:
                                st.warning("⚠️ ME2 no devolvió PDF. Revisa estado del envío.")
                else:
                    st.info("ℹ️ No hay pack_id ni se pudo derivar shipment desde orden_meli.")
            except Exception as e:
                st.warning(f"⚠️ No se pudo usar ME2: {e}")

        if not etiqueta_ok:
            st.warning("⚠️ No hay etiqueta PDF disponible aún para esta guía.")

        # 3) Log de impresión (persistente)
        try:
            now = datetime.now(TZ).isoformat()
            supabase.table(TABLE_NAME).insert({
                "asignacion": asignacion,
                "guia": guia,
                "fecha_impresion": now,
                "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!",
                "estado_orden": match.get("estado_orden"),
                "estado_envio": match.get("estado_envio"),
                "archivo_adjunto": match.get("archivo_adjunto"),
                "comentario": match.get("comentario"),
                "titulo": match.get("titulo"),
                "asin": match.get("asin"),
                "cantidad": match.get("cantidad"),
                "orden_meli": match.get("orden_meli"),
                "pack_id": match.get("pack_id"),
            }).execute()
        except Exception:
            pass

# ==============================
# ✅ BLOQUE ESTABLE — PERSISTENCIA DE SECCIÓN (NO MODIFICAR)
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
# ✅ BLOQUE ESTABLE — UI PRINCIPAL (NO MODIFICAR) + (🔸 Añado botón PRUEBAS)
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
    if st.button("🧪 PRUEBAS"):
        set_page("pruebas")

bg = {
    "ingresar": "#71A9D9",
    "imprimir": "#71D999",
    "datos": "#F2F4F4",
    "pruebas": "#FFE08A"
}.get(st.session_state.page, "#F2F4F4")
st.markdown(f"<style>.stApp{{background-color:{bg};}}</style>", unsafe_allow_html=True)

st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir"
          else ("🗃️ DATOS" if st.session_state.page == "datos" else "🧪 PRUEBAS"))
)

# ==============================
# ✅ BLOQUE ESTABLE — LOG DE ESCANEOS (NO MODIFICAR)
# ==============================

def render_log_with_download_buttons(rows: list, page: str):
    if not rows:
        st.info("No hay registros aún.")
        return
    # Encabezado
    if page == "imprimir":
        cols = ["Asignación", "Guía", "Fecha impresión", "Estado", "Descargar"]
    else:
        cols = ["Asignación", "Guía", "Fecha ingreso", "Estado", "Descargar"]
    hc = st.columns([2, 2, 2, 2, 1])
    for i, h in enumerate(cols):
        hc[i].markdown(f"**{h}**")

    # Filas
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
                        "⇩", data=pdf_bytes, file_name=f"{(asign or 'etiqueta')}.pdf", mime="application/pdf", key=f"dl_{asign}_{guia}_{time.time()}"
                    )
                else:
                    c[4].write("No válido")
            except Exception:
                c[4].write("No disponible")
        else:
            c[4].write("No disponible")

# ==============================
# SECCIONES INGRESAR / IMPRIMIR
# ==============================

if st.session_state.page in ("ingresar", "imprimir"):
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    render_log_with_download_buttons(rows, st.session_state.page)

# ==============================
# CRUD — PÁGINA DATOS
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
        id=None, asignacion="", guia="", fecha_ingreso=None, estado_escaneo="",
        asin="", cantidad=1, estado_orden="", estado_envio="",
        archivo_adjunto="", url_imagen="", comentario="", descripcion="",
        fecha_impresion=None, titulo="", orden_meli="", pack_id=""
    )

def datos_fetch(limit=200, offset=0, search: str = ""):
    q = supabase.table(TABLE_NAME).select("*").order("id", desc=True)
    if search:
        q = q.or_(
            f"asignacion.ilike.%{search}%,guia.ilike.%{search}%,orden_meli.ilike.%{search}%,pack_id.ilike.%{search}%,titulo.ilike.%{search}%"
        )
    return q.range(offset, offset + limit - 1).execute().data or []

def datos_find_duplicates(asignacion, orden_meli, pack_id):
    seen = {}
    for field, value in [("asignacion", asignacion), ("orden_meli", orden_meli), ("pack_id", pack_id)]:
        if value:
            res = supabase.table(TABLE_NAME).select("id,asignacion,orden_meli,pack_id,guia,titulo").eq(field, value).limit(50).execute()
            for r in (res.data or []): seen[r["id"]] = r
    return list(seen.values())

def datos_insert(payload: dict):
    clean = {k: v for k, v in payload.items() if k in ALL_COLUMNS and k != "id"}
    return supabase.table(TABLE_NAME).insert(clean).execute()

def datos_update(id_val: int, payload: dict):
    clean = {k: v for k, v in payload.items() if k in ALL_COLUMNS and k not in (LOCKED_FIELDS_EDIT + ["id"])}
    if not clean:
        return None
    return supabase.table(TABLE_NAME).update(clean).eq("id", id_val).execute()

# --- Estado modal

if "datos_modal_open" not in st.session_state:
    st.session_state.datos_modal_open = False
if "datos_modal_mode" not in st.session_state:
    st.session_state.datos_modal_mode = "new"
if "datos_modal_row" not in st.session_state:
    st.session_state.datos_modal_row = datos_defaults()
if "datos_offset" not in st.session_state:
    st.session_state.datos_offset = 0

def open_modal_new():
    st.session_state.datos_modal_mode = "new"
    st.session_state.datos_modal_row = datos_defaults()
    st.session_state.datos_modal_open = True

def open_modal_edit(row: dict):
    base = datos_defaults()
    base.update({k: row.get(k) for k in row.keys()})
    st.session_state.datos_modal_row = base
    st.session_state.datos_modal_mode = "edit"
    st.session_state.datos_modal_open = True

def close_modal():
    st.session_state.datos_modal_open = False

def _render_form_contents():
    mode = st.session_state.datos_modal_mode
    data = st.session_state.datos_modal_row.copy()

    st.write("**Modo:** ", "Crear nuevo" if mode == "new" else f"Editar ID {data.get('id')}")
    colA, colB, colC = st.columns(3)

    if mode == "edit":
        data["asignacion"] = colA.text_input("asignacion", value=data.get("asignacion") or "", disabled=True)
        data["orden_meli"] = colB.text_input("orden_meli", value=data.get("orden_meli") or "", disabled=True)
    else:
        data["asignacion"] = colA.text_input("asignacion *", value=data.get("asignacion") or "")
        data["orden_meli"] = colB.text_input("orden_meli *", value=data.get("orden_meli") or "")
    data["pack_id"] = colC.text_input("pack_id (opcional)", value=(data.get("pack_id") or ""))

    col1, col2, col3 = st.columns(3)
    data["guia"]   = col1.text_input("guia", value=(data.get("guia") or ""))
    data["titulo"] = col2.text_input("titulo", value=(data.get("titulo") or ""))
    data["asin"]   = col3.text_input("asin", value=(data.get("asin") or ""))

    col4, col5, col6 = st.columns(3)
    data["cantidad"]     = col4.number_input("cantidad", value=int(data.get("cantidad") or 1), min_value=0, step=1)
    data["estado_orden"] = col5.text_input("estado_orden", value=(data.get("estado_orden") or ""))
    data["estado_envio"] = col6.text_input("estado_envio", value=(data.get("estado_envio") or ""))

    # PDF actual (si existe)
    current_pdf = data.get("archivo_adjunto") or ""
    if current_pdf:
        st.markdown(f"[📥 Descargar etiqueta actual]({current_pdf})", unsafe_allow_html=True)

    data["archivo_adjunto"] = st.text_input("archivo_adjunto (URL)", value=current_pdf)
    data["url_imagen"]      = st.text_input("url_imagen (URL)", value=(data.get("url_imagen") or ""))
    data["comentario"]      = st.text_area("comentario", value=(data.get("comentario") or ""))
    data["descripcion"]     = st.text_area("descripcion", value=(data.get("descripcion") or ""))

    st.caption("Subir etiqueta PDF (reemplaza la actual si existe)")
    pdf_file = st.file_uploader("Seleccionar PDF", type=["pdf"], accept_multiple_files=False)

    col_btn1, col_btn2 = st.columns([1,1])
    submitted = col_btn1.button("💾 Guardar", use_container_width=True, key="datos_submit_btn")
    cancel    = col_btn2.button("✖️ Cancelar", use_container_width=True, key="datos_cancel_btn")

    if cancel:
        close_modal()
        st.rerun()

    if submitted:
        # Subir/reemplazar PDF si corresponde
        if pdf_file is not None:
            asign = (data.get("asignacion") or "").strip()
            if not asign:
                st.error("Debes completar 'asignacion' para subir el PDF.")
                return
            url_pdf = upload_pdf_to_storage(asign, pdf_file)
            if url_pdf:
                data["archivo_adjunto"] = url_pdf

        if mode == "new":
            missing = [f for f in REQUIRED_FIELDS if not str(data.get(f, "")).strip()]
            if missing:
                st.error(f"Faltan campos obligatorios: {', '.join(missing)}")
                return
            dups = datos_find_duplicates(data["asignacion"].strip(), data["orden_meli"].strip(), (data.get("pack_id") or "").strip())
            if dups:
                st.warning("⚠️ Existen registros coincidentes:")
                st.dataframe(pd.DataFrame(dups), use_container_width=True, hide_index=True)
                if st.checkbox("Forzar inserción", key="force_insert"):
                    datos_insert(data)
                    st.success("Registro insertado (forzado).")
                    close_modal()
                    st.rerun()
            else:
                datos_insert(data)
                st.success("Registro insertado correctamente.")
                close_modal()
                st.rerun()
        else:
            rid = int(data["id"])
            datos_update(rid, data)
            st.success(f"Registro {rid} actualizado.")
            close_modal()
            st.rerun()

def render_modal_if_needed():
    if not st.session_state.datos_modal_open:
        return
    if hasattr(st, "dialog"):
        @st.dialog("Formulario de registro")
        def _show_dialog():
            _render_form_contents()
        _show_dialog()
    else:
        with st.expander("Formulario de registro", expanded=True):
            _render_form_contents()

# --- Página DATOS

if st.session_state.page == "datos":
    st.markdown("### Base de datos")

    colf1, colf2, colf4 = st.columns([2,1,1])
    with colf1:
        search = st.text_input("Buscar (asignacion / guia / orden_meli / pack_id / titulo)", "")
    with colf2:
        page_size = st.selectbox("Filas por página", [25, 50, 100, 200], index=1)
    with colf4:
        if st.button("➕ Nuevo registro", use_container_width=True):
            open_modal_new()

    # Paginación simple
    colp1, colp2, colp3 = st.columns([1,1,6])
    with colp1:
        if st.button("⟵ Anterior") and st.session_state.datos_offset >= page_size:
            st.session_state.datos_offset -= page_size
    with colp2:
        if st.button("Siguiente ⟶"):
            st.session_state.datos_offset += page_size

    data_rows = datos_fetch(limit=page_size, offset=st.session_state.datos_offset, search=search)
    df_all = pd.DataFrame(data_rows)

    # Filtro "Solo sin guía"
    solo_sin_guia = st.checkbox("Solo sin guía", value=False)
    if solo_sin_guia and not df_all.empty and "guia" in df_all.columns:
        df_all = df_all[df_all["guia"].isna() | (df_all["guia"].astype(str).str.strip() == "")]

    if df_all.empty:
        st.info("Sin registros para mostrar.")
    else:
        show_cols = [c for c in ALL_COLUMNS if c in df_all.columns]
        df_all = df_all.copy()

        # Columna EDITAR como ButtonColumn
        df_all["Editar"] = False
        has_button_col = hasattr(st, "column_config") and hasattr(st.column_config, "ButtonColumn")
        if has_button_col:
            column_config = {
                "Editar": st.column_config.ButtonColumn("Editar", help="Editar fila", icon="✏️", width="small")
            }
        else:
            column_config = {"Editar": st.column_config.CheckboxColumn("Editar", help="Editar fila", default=False)}

        ordered_cols = ["Editar"] + show_cols

        edited_df = st.data_editor(
            df_all[ordered_cols],
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            disabled=show_cols,
            column_config=column_config
        )

        try:
            if "Editar" in edited_df.columns:
                clicked = edited_df.index[edited_df["Editar"] == True].tolist()
                if clicked:
                    idx = clicked[0]
                    row_dict = edited_df.loc[idx].to_dict()
                    row_dict.pop("Editar", None)
                    open_modal_edit(row_dict)
        except Exception:
            pass

    render_modal_if_needed()

# ==============================
# 🧪 NUEVA PÁGINA — PRUEBAS (NO TOCA LA BASE / DATOS)
# ==============================

def _mask(s: str, head=6, tail=4) -> str:
    if not s:
        return "-"
    if len(s) <= head + tail:
        return s[0:2] + "…"
    return s[:head] + "…" + s[-tail:]

if st.session_state.page == "pruebas":
    st.markdown("### Conexión Mercado Libre")

    app_id, client_secret, acc, ref = _read_secrets_block()
    colT1, colT2, colT3, colT4 = st.columns(4)
    colT1.metric("App ID", app_id or "—")
    colT2.metric("Client Secret", _mask(client_secret or ""))
    colT3.metric("Access Token", _mask(acc or ""))
    colT4.metric("Refresh Token", _mask(ref or ""))

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("🔄 Probar users/me"):
            try:
                uid = _meli_get_user_id()
                if uid:
                    st.success(f"OK — user_id: {uid}")
                else:
                    st.error("Fallo users/me (revisa tokens).")
            except Exception as e:
                st.error(f"Error: {e}")
    with c2:
        if st.button("♻️ Forzar refresh token"):
            try:
                tok = _load_tokens()
                if not tok.get("refresh_token"):
                    st.warning("No hay refresh_token disponible en secrets o archivo.")
                else:
                    _refresh_access_token(tok)
                    st.success("Refresh OK. Access token actualizado.")
            except Exception as e:
                st.error(f"Error al refrescar: {e}")
    with c3:
        if st.button("🔍 Mostrar access_token actual"):
            try:
                t = _get_access_token()
                st.code(_mask(t, 12, 8))
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown("---")
    st.markdown("### Prueba de impresión de etiqueta (sin tocar la base)")
    colI1, colI2, colI3 = st.columns(3)
    with colI1:
        input_shipment = st.text_input("shipment_id (opcional)", "")
    with colI2:
        input_order = st.text_input("order_id (opcional)", "")
    with colI3:
        input_pack = st.text_input("pack_id (opcional)", "")

    colName1, colName2 = st.columns([2,1])
    with colName1:
        asignacion_test = st.text_input("asignación (para nombrar PDF si subes a Storage)", "")
    with colName2:
        subir_storage = st.checkbox("Subir a Storage", value=False)

    colB1, colB2, colB3 = st.columns(3)
    do_ready_btn = colB2.button("🚀 Marcar 'Ya tengo el producto' (ready_to_ship)")
    probar_btn = colB1.button("🧪 Probar")
    descargar_btn = colB3.button("⬇️ Descargar etiqueta")

    st.caption("Secuencia: si no indicas shipment_id, intentamos derivarlo desde order_id o pack_id.")

    # Estado compartido en pruebas
    if "pruebas_sid" not in st.session_state:
        st.session_state.pruebas_sid = None
    if "pruebas_pdf" not in st.session_state:
        st.session_state.pruebas_pdf = None

    def _resolve_sid():
        sid = (input_shipment or "").strip()
        if sid:
            return sid
        if input_pack.strip():
            sid = _shipment_id_from_pack(input_pack.strip())
            if sid:
                return sid
        if input_order.strip():
            sid = _shipment_id_from_order(input_order.strip())
            if sid:
                return sid
        return None

    if probar_btn:
        try:
            sid = _resolve_sid()
            st.session_state.pruebas_sid = sid
            if not sid:
                st.warning("No se pudo resolver shipment_id.")
            else:
                st.info(f"Shipment ID: {sid}")
                sh = _meli_get_shipment(sid)
                if not sh:
                    st.error("No se pudo leer /shipments/{id}.")
                else:
                    st.json({
                        "status": sh.get("status"),
                        "substatus": sh.get("substatus"),
                        "logistic_mode": (sh.get("logistic") or {}).get("mode"),
                        "logistic_type": (sh.get("logistic") or {}).get("type"),
                    })
                    cause = _explicacion_estado_label(sh)
                    if cause:
                        st.warning(f"No imprimible aún: {cause}")
                    else:
                        st.success("Imprimible ✅ (ready_to_ship + ready_to_print/printed)")
                        pdf = _download_label_pdf(sid)
                        if pdf:
                            st.session_state.pruebas_pdf = pdf
                            st.success("PDF obtenido. Puedes descargar o subir a Storage.")
                        else:
                            st.error("No se obtuvo PDF desde /shipment_labels.")
        except Exception as e:
            st.error(f"Error de prueba: {e}")

    if do_ready_btn:
        try:
            sid = _resolve_sid()
            if not sid:
                st.warning("Primero indica o resuelve un shipment_id.")
            else:
                ok = _meli_ready_to_ship(sid)
                st.success("Marcado como ready_to_ship.") if ok else st.error("No se pudo marcar ready_to_ship.")
        except Exception as e:
            st.error(f"Error: {e}")

    if descargar_btn:
        try:
            sid = st.session_state.pruebas_sid or _resolve_sid()
            if not sid:
                st.warning("Sin shipment_id.")
            else:
                pdf = st.session_state.pruebas_pdf or _download_label_pdf(sid)
                if not pdf:
                    st.error("No se obtuvo PDF.")
                else:
                    st.download_button("Descargar etiqueta (PDF)", data=pdf, file_name=f"{(asignacion_test or 'etiqueta')}.pdf", mime="application/pdf", use_container_width=True)
                    if subir_storage:
                        if not asignacion_test.strip():
                            st.warning("Indica una asignación para subir a Storage.")
                        else:
                            url_pdf = upload_pdf_bytes_to_storage(asignacion_test.strip(), pdf)
                            if url_pdf:
                                st.success(f"Subido a Storage: {url_pdf}")
                            else:
                                st.error("Falló la subida a Storage.")
        except Exception as e:
            st.error(f"Error al descargar/subir: {e}")
