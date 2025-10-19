import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import requests
import time

# --- MERCADO LIBRE: helpers mínimos para OAuth + etiqueta ---
from typing import Optional
import json, base64, secrets as pysecrets

MELI_API_BASE = "https://api.mercadolibre.com"

def _build_auth_url(client_id: str, redirect_uri: str, state: str, scopes: list[str] = None) -> str:
    scopes = scopes or ["offline_access", "read", "write"]
    scope_str = "%20".join(scopes)
    return (
        "https://auth.mercadolibre.com/authorization"
        f"?response_type=code&client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&scope={scope_str}"
        f"&state={state}"
    )

def _exchange_code_for_tokens(client_id: str, client_secret: str, redirect_uri: str, code: str) -> Optional[dict]:
    try:
        r = requests.post(
            f"{MELI_API_BASE}/oauth/token",
            data={
                "grant_type": "authorization_code",
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "redirect_uri": redirect_uri,
            },
            timeout=20,
        )
        if r.status_code != 200:
            st.error(f"❌ Intercambio de code falló ({r.status_code}): {r.text[:500]}")
            return None
        return r.json()
    except Exception as e:
        st.error(f"❌ Error al intercambiar code: {e}")
        return None

def _meli_get_shipment_id_from_order(order_id: str, access_token: str) -> Optional[str]:
    try:
        r = requests.get(
            f"{MELI_API_BASE}/orders/{order_id}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=20,
        )
        if r.status_code != 200:
            return None
        data = r.json() or {}
        shipping = data.get("shipping") or {}
        sid = shipping.get("id") or shipping.get("shipment_id")
        return str(sid) if sid else None
    except Exception:
        return None

def _meli_download_label_pdf(shipment_id: str, access_token: str) -> Optional[bytes]:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/pdf"}
    # batch
    try:
        url = f"{MELI_API_BASE}/shipment_labels?shipment_ids={shipment_id}&response_type=pdf"
        r = requests.get(url, headers=headers, timeout=25)
        if r.status_code == 200 and r.content[:4] == b"%PDF":
            return r.content
    except Exception:
        pass
    # individual
    try:
        url2 = f"{MELI_API_BASE}/shipments/{shipment_id}/labels?response_type=pdf"
        r2 = requests.get(url2, headers=headers, timeout=25)
        if r2.status_code == 200 and r2.content[:4] == b"%PDF":
            return r2.content
    except Exception:
        pass
    return None


# ==============================
# ✅ BLOQUE ESTABLE — CONFIGURACIÓN GENERAL (NO MODIFICAR)
# - Centraliza URL/KEY de Supabase y parámetros base.
# - Evita errores de inicialización y mantiene timezone consistente.
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
# - Sube/reemplaza PDFs con upsert.
# - Fuerza MIME application/pdf para evitar que el navegador muestre "código".
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
# - Operaciones atómicas de lectura/actualización.
# - Inserta NO COINCIDENTE si la guía no existe.
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
# ✅ BLOQUE ESTABLE — ESCANEO (NO AUTO-ABRIR PDF) (NO MODIFICAR)
# - Punto 3: elimina intento de auto-abrir PDF.
# - Mantiene botón "Descargar nuevamente ..." que funciona perfecto.
# - Inserta un nuevo registro de impresión (log persistente).
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

        # 1) Mostrar botón de descarga confiable (funciona bien)
        if archivo_public and url_disponible(archivo_public):
            try:
                pdf_bytes = requests.get(archivo_public, timeout=10).content
                # Validar encabezado PDF
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

        # 2) Insertar un NUEVO registro para el log de impresión (aunque se repita)
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
            # RLS estricta: si falla inserción del log, no rompe el flujo.
            pass


# ==============================
# ✅ BLOQUE ESTABLE — PERSISTENCIA DE SECCIÓN (NO MODIFICAR)
# - Punto 2: cada sección usa ?page=... en la URL.
# - Al refrescar, NO cambia de sección.
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
# ✅ BLOQUE ESTABLE — UI PRINCIPAL (NO MODIFICAR)
# - Mantiene estilos por sección.
# ==============================
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("INGRESAR PAQUETES"):
        set_page("ingresar")
with col2:
    if st.button("IMPRIMIR GUIAS"):
        set_page("imprimir")
with col3:
    if st.button("🗃️ DATOS"):
        set_page("datos")

bg = {"ingresar": "#71A9D9", "imprimir": "#71D999", "datos": "#F2F4F4"}.get(st.session_state.page, "#F2F4F4")
st.markdown(f"<style>.stApp{{background-color:{bg};}}</style>", unsafe_allow_html=True)

st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else "🗃️ DATOS")
)


# ==============================
# ✅ BLOQUE ESTABLE — LOG DE ESCANEOS (NO MODIFICAR)
# - Punto 5: el “botón” dentro de la columna ARCHIVO_ADJUNTO descarga igual que el botón principal.
# - Para lograrlo sin corromper PDFs, renderizamos cada fila con botón Streamlit.
# - Se mantiene una fila “tipo tabla” con columnas clave.
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
# - Punto 1: vuelve el botón “➕ Nuevo registro”.
# - Punto 6: columna “Editar” usa ButtonColumn (no checkbox) y abre el formulario.
# - Punto 7: checkbox “Solo sin guía”.
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

# ---- Estado modal
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

# ---- Página DATOS
if st.session_state.page == "datos":
    st.markdown("### Base de datos")

  # Mantener el expander abierto mientras conectas
st.session_state.setdefault("meli_open", True)

with st.expander("🔐 Conectar Mercado Libre (OAuth)", expanded=st.session_state["meli_open"]):
    st.caption("Autoriza tu cuenta principal. Nada se guarda en disco; descarga un JSON con los tokens.")

    # --- Credenciales (desde secrets, editables) ---
    colA, colB = st.columns(2)
    client_id = colA.text_input("Client ID", value=st.secrets.get("MELI_CLIENT_ID", ""))
    client_secret = colB.text_input("Client Secret", type="password", value=st.secrets.get("MELI_CLIENT_SECRET", ""))
    redirect_uri = st.text_input("Redirect URI", value=st.secrets.get("MELI_REDIRECT_URI", ""))

    # --- STATE anti-CSRF FIJADO EN LA URL ---
    try:
        _qp = st.query_params
    except Exception:
        _qp = st.experimental_get_query_params()

    if "meli_oauth_state" not in st.session_state:
        _state_qp = (_qp.get("meli_state") or [""])[0] if isinstance(_qp.get("meli_state"), list) else _qp.get("meli_state", "")
        if _state_qp:
            st.session_state.meli_oauth_state = _state_qp
        else:
            import secrets as pysecrets
            st.session_state.meli_oauth_state = pysecrets.token_urlsafe(16)
            try:
                st.query_params["meli_state"] = st.session_state.meli_oauth_state
            except Exception:
                st.experimental_set_query_params(**{**_qp, "meli_state": st.session_state.meli_oauth_state})

    # --- Link de autorización ---
    if client_id and redirect_uri:
        auth_url = _build_auth_url(client_id, redirect_uri, st.session_state.meli_oauth_state)
        st.session_state["meli_open"] = True  # mantener abierto
        if hasattr(st, "link_button"):
            st.link_button("➡️ Autorizar en Mercado Libre", auth_url, use_container_width=True)
        else:
            st.markdown(f"[➡️ Autorizar en Mercado Libre]({auth_url})")

    st.divider()

    # --- Capturar ?code y ?state devueltos por ML ---
    try:
        _qp = st.query_params
    except Exception:
        _qp = st.experimental_get_query_params()
    _code = (_qp.get("code") or [""])[0] if isinstance(_qp.get("code"), list) else _qp.get("code", "")
    _state = (_qp.get("state") or [""])[0] if isinstance(_qp.get("state"), list) else _qp.get("state", "")

    colC, colD = st.columns(2)
    code_in = colC.text_input("Code (si no volvió automático, pégalo aquí)", value=_code)
    state_in = colD.text_input("State recibido", value=_state)

    # --- Obtener tokens (code -> access/refresh) ---
    # --- Forzar state si acabo de autorizar (bypass seguro para uso interno) ---
forzar_state = st.checkbox("Confirmo que acabo de autorizar y quiero forzar el state")


    if st.button("🔄 Obtener tokens", use_container_width=True,
             disabled=not (client_id and client_secret and redirect_uri and code_in)):
    st.session_state["meli_open"] = True  # mantener el expander abierto

    # si NO marco 'forzar' y el state no coincide -> error
    if (not forzar_state) and state_in and state_in != st.session_state.meli_oauth_state:
        st.error("El parámetro state no coincide. Genera un nuevo enlace y vuelve a autorizar.")
    else:
        # si marco 'forzar', alineo el state en sesión con el recibido y sigo
        if forzar_state and state_in:
            st.session_state.meli_oauth_state = state_in

        tokens = _exchange_code_for_tokens(client_id, client_secret, redirect_uri, code_in)
        if tokens:
            st.session_state.meli_tokens = tokens
            st.success("✅ Tokens obtenidos.")
            st.json(tokens)  # se verá refresh_token en la respuesta


    # --- Mostrar tokens y prueba de etiqueta ---
    tokens = st.session_state.get("meli_tokens")
    if tokens:
        access_token = tokens.get("access_token", "")
        refresh_token = tokens.get("refresh_token", "")
        st.text_area("access_token", value=access_token, height=90)
        st.text_input("refresh_token (guárdalo en Secrets)", value=refresh_token)


        st.markdown("---")
        st.write("### Prueba rápida de etiqueta")
        order_id_test = st.text_input("order_id para probar", value="")
        if st.button("🔎 Probar descarga PDF", disabled=not (order_id_test and access_token)):
            sid = _meli_get_shipment_id_from_order(order_id_test.strip(), access_token)
            if not sid:
                st.error("No se encontró shipment_id (¿es Mercado Envíos / lista para imprimir?).")
            else:
                pdf = _meli_download_label_pdf(sid, access_token)
                if pdf and pdf[:4] == b"%PDF":
                    st.success(f"PDF OK (shipment_id={sid})")
                    st.download_button("📄 Descargar etiqueta.pdf", data=pdf, file_name="etiqueta.pdf", mime="application/pdf", use_container_width=True)
                else:
                    st.error("No se pudo descargar la etiqueta.")

        buf = json.dumps({
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "obtained_at": datetime.now(TZ).isoformat(),
        }, ensure_ascii=False, indent=2).encode("utf-8")

        st.download_button("💾 Descargar meli_tokens.json", data=buf, file_name="meli_tokens.json", mime="application/json", use_container_width=True)


    colf1, colf2, colf4 = st.columns([2,1,1])
    with colf1:
        search = st.text_input("Buscar (asignacion / guia / orden_meli / pack_id / titulo)", "")
    with colf2:
        page_size = st.selectbox("Filas por página", [25, 50, 100, 200], index=1)
    with colf4:
        # Punto 1: Botón "Nuevo registro" restaurado
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

    # Punto 7: Filtro "Solo sin guía"
    solo_sin_guia = st.checkbox("Solo sin guía", value=False)
    if solo_sin_guia and not df_all.empty and "guia" in df_all.columns:
        df_all = df_all[df_all["guia"].isna() | (df_all["guia"].astype(str).str.strip() == "")]

    if df_all.empty:
        st.info("Sin registros para mostrar.")
    else:
        show_cols = [c for c in ALL_COLUMNS if c in df_all.columns]
        df_all = df_all.copy()

        # Punto 6: Columna EDITAR con ButtonColumn (sin checkbox)
        df_all["Editar"] = False
        has_button_col = hasattr(st, "column_config") and hasattr(st.column_config, "ButtonColumn")
        if has_button_col:
            column_config = {
                "Editar": st.column_config.ButtonColumn("Editar", help="Editar fila", icon="✏️", width="small")
            }
        else:
            # Fallback si la versión no soporta ButtonColumn
            column_config = {"Editar": st.column_config.CheckboxColumn("Editar", help="Editar fila", default=False)}

        ordered_cols = ["Editar"] + show_cols

        edited_df = st.data_editor(
            df_all[ordered_cols],
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            disabled=show_cols,   # no permitir edición inline; usar modal
            column_config=column_config
        )

        # Detectar fila solicitada para editar
        try:
            if "Editar" in edited_df.columns:
                # tanto para ButtonColumn (True en la fila clickeada)
                # como para Checkbox fallback
                clicked = edited_df.index[edited_df["Editar"] == True].tolist()
                if clicked:
                    idx = clicked[0]
                    row_dict = edited_df.loc[idx].to_dict()
                    row_dict.pop("Editar", None)
                    open_modal_edit(row_dict)
        except Exception:
            pass

    render_modal_if_needed()
