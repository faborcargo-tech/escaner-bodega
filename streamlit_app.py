import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import pytz
import requests
import time
from typing import Optional, Dict, Any

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
# STORAGE HELPERS
# ==============================
def _get_public_or_signed_url(path: str) -> Optional[str]:
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            return url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        return url
    except Exception:
        return None

def upload_pdf_to_storage(asignacion: str, uploaded_file) -> Optional[str]:
    """Sube/reemplaza PDF como <asignacion>.pdf y retorna URL pública."""
    if not asignacion or uploaded_file is None:
        return None
    key_path = f"{asignacion}.pdf"
    file_bytes = uploaded_file.read()
    try:
        supabase.storage.from_(STORAGE_BUCKET).upload(
            key_path, file_bytes, {"upsert": "true"}
        )
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None

    # Forzar MIME application/pdf
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
        r = requests.head(url, timeout=6)
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
    field = "fecha_ingreso" if page == "ingresar" else "fecha_impresion"
    res = supabase.table(TABLE_NAME).select("*").gte(field, cutoff).order(field, desc=True).execute()
    return res.data or []

# ==============================
# MERCADO LIBRE - AUTH + API
# ==============================
ML_AUTH_HOST = "https://auth.mercadolibre.cl"
ML_API = "https://api.mercadolibre.com"

def _ss_init():
    ss = st.session_state
    ss.setdefault("page", "ingresar")
    ss.setdefault("meli_local", {
        "client_id": st.secrets.get("MELI_CLIENT_ID", ""),
        "client_secret": st.secrets.get("MELI_CLIENT_SECRET", ""),
        "redirect_uri": st.secrets.get("MELI_REDIRECT_URI", ""),
        "access_token": "",
        "refresh_token": st.secrets.get("MELI_REFRESH_TOKEN", ""),
        "expires_at": 0,  # epoch seconds
    })
_ss_init()

def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def meli_oauth_url(client_id: str, redirect_uri: str, state: str = "ok") -> str:
    return (
        f"{ML_AUTH_HOST}/authorization"
        f"?response_type=code&client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&state={state}"
    )

def meli_exchange_code_for_tokens(code: str) -> Optional[Dict[str, Any]]:
    ml = st.session_state.meli_local
    if not (ml["client_id"] and ml["client_secret"] and ml["redirect_uri"] and code):
        return None
    data = {
        "grant_type": "authorization_code",
        "client_id": ml["client_id"],
        "client_secret": ml["client_secret"],
        "code": code,
        "redirect_uri": ml["redirect_uri"],
    }
    try:
        r = requests.post(f"{ML_API}/oauth/token", data=data, timeout=15)
        if r.status_code == 200:
            tok = r.json()
            ml["access_token"] = tok.get("access_token", "")
            ml["refresh_token"] = tok.get("refresh_token", "")
            ml["expires_at"] = _now_epoch() + int(tok.get("expires_in", 10800)) - 60
            return tok
        else:
            st.error(f"Token exchange error {r.status_code}: {r.text}")
    except Exception as e:
        st.error(f"Token exchange exception: {e}")
    return None

def meli_refresh_access_token() -> bool:
    ml = st.session_state.meli_local
    rt = ml.get("refresh_token") or st.secrets.get("MELI_REFRESH_TOKEN", "")
    if not (ml["client_id"] and ml["client_secret"] and rt):
        return False
    data = {
        "grant_type": "refresh_token",
        "client_id": ml["client_id"],
        "client_secret": ml["client_secret"],
        "refresh_token": rt,
    }
    try:
        r = requests.post(f"{ML_API}/oauth/token", data=data, timeout=15)
        if r.status_code == 200:
            tok = r.json()
            ml["access_token"] = tok.get("access_token", "")
            ml["refresh_token"] = tok.get("refresh_token", rt)
            ml["expires_at"] = _now_epoch() + int(tok.get("expires_in", 10800)) - 60
            return True
        else:
            st.error(f"Refresh error {r.status_code}: {r.text}")
            return False
    except Exception as e:
        st.error(f"Refresh exception: {e}")
        return False

def meli_get_access_token() -> Optional[str]:
    ml = st.session_state.meli_local
    at = ml.get("access_token", "")
    if at and _now_epoch() < int(ml.get("expires_at", 0)):
        return at
    # Si no hay AT válido, intenta refrescar
    if meli_refresh_access_token():
        return st.session_state.meli_local.get("access_token")
    return None

def meli_get(path: str, params: Optional[dict] = None, raw: bool = False):
    at = meli_get_access_token()
    if not at:
        raise RuntimeError("No hay ACCESS_TOKEN disponible. Conecta o refresca.")
    headers = {"Authorization": f"Bearer {at}"}
    url = f"{ML_API}{path}"
    r = requests.get(url, headers=headers, params=params or {}, timeout=20)
    if raw:
        return r
    if r.status_code >= 400:
        raise RuntimeError(f"GET {path} -> {r.status_code}: {r.text}")
    return r.json()

def meli_post(path: str, json_body: Optional[dict] = None):
    at = meli_get_access_token()
    if not at:
        raise RuntimeError("No hay ACCESS_TOKEN disponible. Conecta o refresca.")
    headers = {"Authorization": f"Bearer {at}", "Content-Type": "application/json"}
    url = f"{ML_API}{path}"
    r = requests.post(url, headers=headers, json=json_body or {}, timeout=20)
    if r.status_code >= 400:
        raise RuntimeError(f"POST {path} -> {r.status_code}: {r.text}")
    return r.json() if r.text else {}

def derive_shipment_id(order_id: str, pack_id: str) -> Optional[str]:
    """Obtiene shipment_id desde order o pack."""
    if order_id:
        j = meli_get(f"/orders/{order_id}")
        shipping = (j or {}).get("shipping") or {}
        sid = shipping.get("id")
        if sid:
            return str(sid)
    if pack_id:
        j = meli_get(f"/packs/{pack_id}")
        shp = (j or {}).get("shipment") or {}
        sid = shp.get("id")
        if sid:
            return str(sid)
    return None

def download_label_pdf(shipment_id: str) -> bytes:
    r = meli_get("/shipment_labels", params={"shipment_ids": shipment_id, "response_type": "pdf"}, raw=True)
    if hasattr(r, "status_code") and r.status_code == 200:
        return r.content
    raise RuntimeError(f"Labels -> {getattr(r, 'status_code', '???')}")

def mark_ready_to_ship(shipment_id: str):
    return meli_post(f"/shipments/{shipment_id}/process/ready_to_ship", json_body={})

# --- Captura global del parámetro ?code=... (funciona en cualquier página)
def _capture_oauth_code_if_present():
    try:
        qp = st.query_params
        code_from_url = qp.get("code", [None])[0]
        state = qp.get("state", [None])[0]
        current_page = qp.get("page", [st.session_state.page])[0]
    except Exception:
        qp = st.experimental_get_query_params()
        code_from_url = qp.get("code", [None])[0]
        state = qp.get("state", [None])[0]
        current_page = qp.get("page", [st.session_state.page])[0]

    if code_from_url:
        tok = meli_exchange_code_for_tokens(code_from_url)
        if tok:
            st.success("¡Conexión lista! Tokens guardados.")
        else:
            st.error("No se pudo intercambiar el code. Reintenta reconectar.")
        # limpiar URL y mantener la página actual
        try:
            st.query_params["page"] = current_page
        except Exception:
            st.experimental_set_query_params(page=current_page)

_capture_oauth_code_if_present()

# ==============================
# SCAN FLOW (NO CAMBIAR)
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
                        label=f"📄 Descargar nuevamente {asignacion}.pdf",
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

        # log persistente
        try:
            now = datetime.now(TZ).isoformat()
            supabase.table(TABLE_NAME).insert({
                "asignacion": asignacion,
                "guia": guia,
                "fecha_impresion": now,
                "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!",
                "estado_orden": match.get("estado_orden"),
                "estado_envio": match.get("estado_envio"),
                "archivo_adjunto": archivo_public,
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
# PAGE STATE / NAV
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
# UI TOP NAV
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

bg = {"ingresar": "#71A9D9", "imprimir": "#71D999", "datos": "#F2F4F4", "pruebas": "#F6DD9B"}.get(st.session_state.page, "#F2F4F4")
st.markdown(f"<style>.stApp{{background-color:{bg};}}</style>", unsafe_allow_html=True)

st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir"
          else ("🗃️ DATOS" if st.session_state.page == "datos" else "🧪 PRUEBAS"))
)

# ==============================
# LOG VISUALIZATION
# ==============================
def render_log_with_download_buttons(rows: list, page: str):
    if not rows:
        st.info("No hay registros aún.")
        return

    if page == "imprimir":
        cols = ["Asignación", "Guía", "Fecha impresión", "Estado", "Descargar"]
    else:
        cols = ["Asignación", "Guía", "Fecha ingreso", "Estado", "Descargar"]
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

# ==============================
# PAGES: INGRESAR / IMPRIMIR
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    render_log_with_download_buttons(rows, st.session_state.page)

# ==============================
# PAGE: DATOS (CRUD resumido)
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
            for r in (res.data or []):
                seen[r["id"]] = r
    return list(seen.values())

def datos_insert(payload: dict):
    clean = {k: v for k, v in payload.items() if k in ALL_COLUMNS and k != "id"}
    return supabase.table(TABLE_NAME).insert(clean).execute()

def datos_update(id_val: int, payload: dict):
    clean = {k: v for k, v in payload.items() if k in ALL_COLUMNS and k not in (LOCKED_FIELDS_EDIT + ["id"])}
    if not clean:
        return None
    return supabase.table(TABLE_NAME).update(clean).eq("id", id_val).execute()

# Estado modal
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

    colp1, colp2, colp3 = st.columns([1,1,6])
    with colp1:
        if st.button("⟵ Anterior") and st.session_state.datos_offset >= page_size:
            st.session_state.datos_offset -= page_size
    with colp2:
        if st.button("Siguiente ⟶"):
            st.session_state.datos_offset += page_size

    data_rows = datos_fetch(limit=page_size, offset=st.session_state.datos_offset, search=search)
    df_all = pd.DataFrame(data_rows)

    solo_sin_guia = st.checkbox("Solo sin guía", value=False)
    if solo_sin_guia and not df_all.empty and "guia" in df_all.columns:
        df_all = df_all[df_all["guia"].isna() | (df_all["guia"].astype(str).str.strip() == "")]

    if df_all.empty:
        st.info("Sin registros para mostrar.")
    else:
        show_cols = [c for c in ALL_COLUMNS if c in df_all.columns]
        df_all = df_all.copy()

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
# PAGE: PRUEBAS
# ==============================
if st.session_state.page == "pruebas":
    st.subheader("Prueba de impresión de etiqueta (no toca la base)")

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        shipment_id = st.text_input("shipment_id (opcional)", "")
    with c2:
        order_id = st.text_input("order_id (opcional)", "")
    with c3:
        pack_id = st.text_input("pack_id (opcional)", "")

    asignacion = st.text_input("asignación (para nombrar PDF si subes a Storage)", "")
    subir = st.checkbox("Subir a Storage (reemplaza si existe)", value=False)

    colA, colB = st.columns([1,1])
    with colA:
        if st.button("🔍 Probar", use_container_width=True):
            try:
                sid = shipment_id.strip()
                if not sid:
                    sid = derive_shipment_id(order_id.strip(), pack_id.strip())
                    if not sid:
                        st.error("No se encontró shipment_id (¿order/pack correctos y visibles con este token?).")
                        st.stop()
                pdf_bytes = download_label_pdf(sid)
                if pdf_bytes[:4] != b"%PDF":
                    st.error("La respuesta no parece un PDF válido.")
                    st.stop()
                st.success(f"Etiqueta lista (shipment_id={sid}).")
                st.download_button("📄 Descargar etiqueta", data=pdf_bytes, file_name=f"{(asignacion or sid)}.pdf", mime="application/pdf", use_container_width=True)
                if subir:
                    if not asignacion.strip():
                        st.warning("Para subir a Storage, completa 'asignación'.")
                    else:
                        class _B:
                            def __init__(self, b): self._b=b
                            def read(self): return self._b
                        url_pdf = upload_pdf_to_storage(asignacion.strip(), _B(pdf_bytes))
                        if url_pdf:
                            st.success(f"Subido a Storage: {url_pdf}")
                        else:
                            st.warning("No se pudo subir a Storage.")
            except Exception as e:
                st.error(str(e))

    with colB:
        if st.button("📌 Marcar 'Ya tengo el producto' (ready_to_ship)", use_container_width=True):
            try:
                sid = shipment_id.strip()
                if not sid:
                    sid = derive_shipment_id(order_id.strip(), pack_id.strip())
                    if not sid:
                        st.error("No se encontró shipment_id para marcar.")
                        st.stop()
                mark_ready_to_ship(sid)
                st.success(f"OK ready_to_ship (shipment_id={sid}).")
            except Exception as e:
                st.error(str(e))

    # ----- Credenciales / Reconexión -----
    with st.expander("🔐 Cargar/gestionar credenciales locales (opcional)", expanded=False):
        ml = st.session_state.meli_local
        ml["client_id"] = st.text_input("App ID", ml.get("client_id",""))
        ml["client_secret"] = st.text_input("Client Secret", ml.get("client_secret",""), type="password")
        ml["access_token"] = st.text_input("Access token", ml.get("access_token",""), type="password")
        ml["refresh_token"] = st.text_input("Refresh token", ml.get("refresh_token",""), type="password")
        default_redirect = ml.get("redirect_uri") or st.secrets.get("MELI_REDIRECT_URI", "") or "https://escaner-bodega.streamlit.app/?page=pruebas"
        ml["redirect_uri"] = st.text_input("Redirect URI (opcional)", default_redirect)

        colX, colY, colZ = st.columns([1,1,1])
        with colX:
            if st.button("💾 Guardar credenciales locales", use_container_width=True):
                st.success("Guardado en memoria de la app.")
        with colY:
            if st.button("🧹 Borrar tokens locales", use_container_width=True):
                ml["access_token"] = ""
                ml["refresh_token"] = ""
                ml["expires_at"] = 0
                st.success("Tokens locales borrados.")
        with colZ:
            if st.button("🔄 Refrescar access token", use_container_width=True):
                ok = meli_refresh_access_token()
                if ok:
                    st.success("Access token refrescado.")
                else:
                    st.error("No se pudo refrescar el token.")

        st.divider()
        st.markdown("**Conectar/Reconectar con Mercado Libre (OAuth)**")
        if ml["client_id"] and ml["redirect_uri"]:
            auth_url = meli_oauth_url(ml["client_id"], ml["redirect_uri"], state="stk")
            st.link_button("🔗 Conectar/Reconectar con Mercado Libre", auth_url, use_container_width=True)
        else:
            st.info("Completa App ID y Redirect URI para mostrar el botón de conexión.")

        # Mostrar expiración estimada
        exp_in = max(0, st.session_state.meli_local.get("expires_at", 0) - _now_epoch())
        st.caption(f"Access token expira en ~{exp_in} segundos.")
