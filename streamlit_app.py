# streamlit_app.py
# -------------------------------------------------------------
# Escáner Bodega — App original + pestaña PRUEBAS (OAuth/labels)
# Fix: nunca usar secrets para refrescar; solo el refresh_token
# de sesión obtenido vía OAuth. Botón de reconexión a PRUEBAS.
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
        supabase.storage.from_(STORAGE_BUCKET).upload(
            key_path, data, {"upsert": "true"}
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
                    "estado_orden": match.get("estado_orden"),
                    "estado_envio": match.get("estado_envio"),
                    "archivo_adjunto": archivo_public,
                    "comentario": match.get("comentario"),
                    "titulo": match.get("titulo"),
                    "asin": match.get("asin"),
                    "cantidad": match.get("cantidad"),
                    "orden_meli": match.get("orden_meli"),
                    "pack_id": match.get("pack_id"),
                }
            ).execute()
        except Exception:
            pass

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

# ==============================
# ✅ LOG DE ESCANEOS
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
# CRUD — PÁGINA DATOS (igual que antes)
# ==============================

ALL_COLUMNS = [
    "id",
    "asignacion",
    "guia",
    "fecha_ingreso",
    "estado_escaneo",
    "asin",
    "cantidad",
    "estado_orden",
    "estado_envio",
    "archivo_adjunto",
    "url_imagen",
    "comentario",
    "descripcion",
    "fecha_impresion",
    "titulo",
    "orden_meli",
    "pack_id",
]
REQUIRED_FIELDS = ["asignacion", "orden_meli"]
LOCKED_FIELDS_EDIT = ["asignacion", "orden_meli"]

def datos_defaults():
    return dict(
        id=None,
        asignacion="",
        guia="",
        fecha_ingreso=None,
        estado_escaneo="",
        asin="",
        cantidad=1,
        estado_orden="",
        estado_envio="",
        archivo_adjunto="",
        url_imagen="",
        comentario="",
        descripcion="",
        fecha_impresion=None,
        titulo="",
        orden_meli="",
        pack_id="",
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
    for field, value in [
        ("asignacion", asignacion),
        ("orden_meli", orden_meli),
        ("pack_id", pack_id),
    ]:
        if value:
            res = (
                supabase.table(TABLE_NAME)
                .select("id,asignacion,orden_meli,pack_id,guia,titulo")
                .eq(field, value)
                .limit(50)
                .execute()
            )
            for r in (res.data or []):
                seen[r["id"]] = r
    return list(seen.values())

def datos_insert(payload: dict):
    clean = {k: v for k, v in payload.items() if k in ALL_COLUMNS and k != "id"}
    return supabase.table(TABLE_NAME).insert(clean).execute()

def datos_update(id_val: int, payload: dict):
    clean = {
        k: v
        for k, v in payload.items()
        if k in ALL_COLUMNS and k not in (LOCKED_FIELDS_EDIT + ["id"])
    }
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
        data["asignacion"] = colA.text_input(
            "asignacion", value=data.get("asignacion") or "", disabled=True
        )
        data["orden_meli"] = colB.text_input(
            "orden_meli", value=data.get("orden_meli") or "", disabled=True
        )
    else:
        data["asignacion"] = colA.text_input("asignacion *", value=data.get("asignacion") or "")
        data["orden_meli"] = colB.text_input("orden_meli *", value=data.get("orden_meli") or "")
    data["pack_id"] = colC.text_input("pack_id (opcional)", value=(data.get("pack_id") or ""))

    col1, col2, col3 = st.columns(3)
    data["guia"] = col1.text_input("guia", value=(data.get("guia") or ""))
    data["titulo"] = col2.text_input("titulo", value=(data.get("titulo") or ""))
    data["asin"] = col3.text_input("asin", value=(data.get("asin") or ""))

    col4, col5, col6 = st.columns(3)
    data["cantidad"] = col4.number_input(
        "cantidad", value=int(data.get("cantidad") or 1), min_value=0, step=1
    )
    data["estado_orden"] = col5.text_input("estado_orden", value=(data.get("estado_orden") or ""))
    data["estado_envio"] = col6.text_input("estado_envio", value=(data.get("estado_envio") or ""))

    current_pdf = data.get("archivo_adjunto") or ""
    if current_pdf:
        st.markdown(f"[📥 Descargar etiqueta actual]({current_pdf})", unsafe_allow_html=True)

    data["archivo_adjunto"] = st.text_input("archivo_adjunto (URL)", value=current_pdf)
    data["url_imagen"] = st.text_input("url_imagen (URL)", value=(data.get("url_imagen") or ""))
    data["comentario"] = st.text_area("comentario", value=(data.get("comentario") or ""))
    data["descripcion"] = st.text_area("descripcion", value=(data.get("descripcion") or ""))

    st.caption("Subir etiqueta PDF (reemplaza la actual si existe)")
    pdf_file = st.file_uploader("Seleccionar PDF", type=["pdf"], accept_multiple_files=False)

    col_btn1, col_btn2 = st.columns([1, 1])
    submitted = col_btn1.button("💾 Guardar", use_container_width=True, key="datos_submit_btn")
    cancel = col_btn2.button("✖️ Cancelar", use_container_width=True, key="datos_cancel_btn")

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
            dups = datos_find_duplicates(
                data["asignacion"].strip(),
                data["orden_meli"].strip(),
                (data.get("pack_id") or "").strip(),
            )
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

    colf1, colf2, colf4 = st.columns([2, 1, 1])
    with colf1:
        search = st.text_input("Buscar (asignacion / guia / orden_meli / pack_id / titulo)", "")
    with colf2:
        page_size = st.selectbox("Filas por página", [25, 50, 100, 200], index=1)
    with colf4:
        if st.button("➕ Nuevo registro", use_container_width=True):
            open_modal_new()

    colp1, colp2, _ = st.columns([1, 1, 6])
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
                "Editar": st.column_config.ButtonColumn(
                    "Editar", help="Editar fila", icon="✏️", width="small"
                )
            }
        else:
            column_config = {
                "Editar": st.column_config.CheckboxColumn(
                    "Editar", help="Editar fila", default=False
                )
            }

        ordered_cols = ["Editar"] + show_cols

        edited_df = st.data_editor(
            df_all[ordered_cols],
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            disabled=show_cols,
            column_config=column_config,
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

# ============================================================
# 🔧 PRUEBAS — OAuth + impresión etiqueta
# ============================================================

if "meli" not in st.session_state:
    st.session_state.meli = {"access_token": "", "refresh_token": "", "expires_at": 0}

def _cfg(key: str, default: str = "") -> str:
    return (st.session_state.get(key) or st.secrets.get(key) or default)

MELI_AUTH_BASE = "https://auth.mercadolibre.cl/authorization"
MELI_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
REDIRECT_PRUEBAS = "https://escaner-bodega.streamlit.app/?page=pruebas"

def _now_epoch() -> int:
    return int(time.time())

def _token_seconds_left() -> int:
    return max(0, st.session_state.meli.get("expires_at", 0) - _now_epoch())

def meli_request(method: str, url: str, *, params=None, json=None, x_format_new=False):
    token = st.session_state.meli.get("access_token") or ""
    headers = {"Authorization": f"Bearer {token}"}
    if x_format_new:
        headers["x-format-new"] = "true"
    return requests.request(method, url, params=params, json=json, headers=headers, timeout=20)

def exchange_code_for_token(code: str, redirect_uri: str) -> bool:
    payload = {
        "grant_type": "authorization_code",
        "client_id": _cfg("MELI_CLIENT_ID"),
        "client_secret": _cfg("MELI_CLIENT_SECRET"),
        "code": code,
        "redirect_uri": redirect_uri,
    }
    try:
        r = requests.post(MELI_TOKEN_URL, data=payload, timeout=25)
        if r.status_code == 200:
            data = r.json()
            st.session_state.meli["access_token"] = data.get("access_token", "")
            st.session_state.meli["refresh_token"] = data.get("refresh_token", "")
            st.session_state.meli["expires_at"] = _now_epoch() + int(data.get("expires_in", 0))
            st.success("Conectado con Mercado Libre. Tokens guardados en sesión.")
            return True
        st.error(f"Intercambio code->token falló: {r.status_code} {r.text}")
    except Exception as e:
        st.error(f"Error de red en token: {e}")
    return False

def refresh_access_token() -> bool:
    # 👉 NUNCA usar secrets aquí. Solo el refresh_token VIGENTE de sesión.
    refresh_token = st.session_state.meli.get("refresh_token")
    if not refresh_token:
        st.warning("No hay refresh token en la sesión. Conecta de nuevo.")
        return False
    payload = {
        "grant_type": "refresh_token",
        "client_id": _cfg("MELI_CLIENT_ID"),
        "client_secret": _cfg("MELI_CLIENT_SECRET"),
        "refresh_token": refresh_token,
    }
    try:
        r = requests.post(MELI_TOKEN_URL, data=payload, timeout=25)
        if r.status_code == 200:
            data = r.json()
            st.session_state.meli["access_token"] = data.get("access_token", "")
            # MELI rota el refresh_token: GUARDAR el nuevo
            st.session_state.meli["refresh_token"] = data.get("refresh_token", refresh_token)
            st.session_state.meli["expires_at"] = _now_epoch() + int(data.get("expires_in", 0))
            return True
        st.error(f"Refresh falló: {r.status_code} {r.text}")
    except Exception as e:
        st.error(f"Error de red en refresh: {e}")
    return False

def derive_shipment_id(order_id: str | None, pack_id: str | None):
    if order_id:
        r = meli_request("GET", f"https://api.mercadolibre.com/orders/{order_id}")
        if r.status_code == 200:
            sid = (r.json().get("shipping") or {}).get("id")
            if sid:
                return str(sid), "orders"
        return None, f"GET /orders/{order_id} -> {r.status_code}: {r.text}"
    if pack_id:
        r = meli_request("GET", f"https://api.mercadolibre.com/packs/{pack_id}")
        if r.status_code == 200:
            sid = (r.json().get("shipment") or {}).get("id")
            if sid:
                return str(sid), "packs"
        return None, f"GET /packs/{pack_id} -> {r.status_code}: {r.text}"
    return None, "Sin parámetros"

def get_label_pdf_bytes(shipment_id: str) -> bytes | None:
    url = "https://api.mercadolibre.com/shipment_labels"
    params = {"shipment_ids": shipment_id, "response_type": "pdf"}
    r = meli_request("GET", url, params=params)
    if r.status_code == 200 and r.content:
        return r.content
    st.error(f"Etiqueta: {r.status_code} {r.text}")
    return None

def mark_ready_to_ship(shipment_id: str) -> bool:
    url = f"https://api.mercadolibre.com/shipments/{shipment_id}/process/ready_to_ship"
    r = meli_request("POST", url)
    if r.status_code == 200:
        return True
    st.error(f"ready_to_ship: {r.status_code} {r.text}")
    return False

# Intercambio automático si regresamos con ?code=... a PRUEBAS
if st.session_state.page == "pruebas":
    try:
        qp = st.query_params
    except Exception:
        qp = st.experimental_get_query_params()
    code = (qp.get("code") or [None])[0]
    if code:
        if exchange_code_for_token(code, REDIRECT_PRUEBAS):
            _set_page_param("pruebas")
            st.rerun()

if st.session_state.page == "pruebas":
    st.subheader("Prueba de impresión de etiqueta (no toca la base)")

# ============================================================
# 🧩 CAMPO TEMPORAL PARA PEGAR ACCESS_TOKEN MANUALMENTE
# ============================================================
st.markdown("#### Token manual (solo para pruebas)")
token_manual = st.text_input(
    "Access Token generado externamente (Postman / OAuth manual)",
    value=st.session_state.meli.get("access_token", ""),
    type="password",
    help="Pega aquí un access_token válido de Mercado Libre Chile (ME2)."
)
if token_manual.strip():
    st.session_state.meli["access_token"] = token_manual.strip()
    st.success("✅ Token manual cargado en sesión. Ahora puedes probar imprimir la etiqueta.")
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

    colbtn1, colbtn2 = st.columns([1, 2])
    probar = colbtn1.button("🔍 Probar")
    mark_rts = colbtn2.button("📌 Marcar 'Ya tengo el producto' (ready_to_ship)")

    st.markdown("---")

    # Panel de credenciales locales
    with st.expander("🔐 Cargar/gestionar credenciales locales (opcional)", expanded=False):
        # Prefill tokens SIEMPRE desde sesión (no desde secrets)
        local_app_id = st.text_input("App ID", value=_cfg("MELI_CLIENT_ID"))
        local_client_secret = st.text_input(
            "Client Secret", value=_cfg("MELI_CLIENT_SECRET"), type="password"
        )
        local_access_token = st.text_input(
            "Access token", value=st.session_state.meli.get("access_token", ""), type="password"
        )
        local_refresh_token = st.text_input(
            "Refresh token", value=st.session_state.meli.get("refresh_token", ""), type="password"
        )
        local_redirect = st.text_input("Redirect URI (opcional)", value=REDIRECT_PRUEBAS)
        local_expires = st.number_input(
            "Expires in (segundos)", value=int(_token_seconds_left() or 10800), step=60
        )

        colB1, colB2, colB3 = st.columns([1, 1, 1])
        if colB1.button("💾 Guardar credenciales locales", use_container_width=True):
            st.session_state["MELI_CLIENT_ID"] = local_app_id
            st.session_state["MELI_CLIENT_SECRET"] = local_client_secret
            st.session_state["MELI_REDIRECT_URI"] = local_redirect
            st.session_state.meli["access_token"] = local_access_token
            st.session_state.meli["refresh_token"] = local_refresh_token
            st.session_state.meli["expires_at"] = _now_epoch() + int(local_expires)
            st.success("Credenciales locales guardadas.")
        if colB2.button("🧹 Borrar tokens locales", use_container_width=True):
            st.session_state.meli["access_token"] = ""
            st.session_state.meli["refresh_token"] = ""
            st.session_state.meli["expires_at"] = 0
            st.success("Tokens locales borrados.")
        if colB3.button("🔄 Refrescar access token", use_container_width=True):
            if refresh_access_token():
                st.success("Access token refrescado.")
            else:
                st.error("No se pudo refrescar el token. Conecta de nuevo.")

        st.caption(f"Access token expira en ~{_token_seconds_left()} segundos.")

        force_auth_url = (
            f"{MELI_AUTH_BASE}?response_type=code"
            f"&client_id={_cfg('MELI_CLIENT_ID')}"
            f"&redirect_uri={quote_plus(REDIRECT_PRUEBAS)}"
            f"&state=stk"
        )
        try:
            st.link_button(
                "🔗 Conectar/Reconectar con Mercado Libre (OAuth)",
                force_auth_url,
                use_container_width=True,
            )
        except Exception:
            st.markdown(
                f"[🔗 Conectar/Reconectar con Mercado Libre (OAuth)]({force_auth_url})",
                unsafe_allow_html=True,
            )

    def _need_token() -> bool:
        if not st.session_state.meli.get("access_token"):
            st.error("No hay ACCESS_TOKEN. Conecta o refresca el token en el panel de arriba.")
            return True
        return False

    if probar:
        if _need_token():
            st.stop()
        sid = shipment_id_input.strip()
        if not sid:
            sid, msg = derive_shipment_id(order_id_input.strip(), pack_id_input.strip())
            if not sid:
                st.error(msg or "No fue posible derivar shipment_id.")
                st.stop()
        pdf_bytes = get_label_pdf_bytes(sid)
        if pdf_bytes and pdf_bytes[:4] == b"%PDF":
            st.success(f"Etiqueta OK — shipment_id={sid}")
            st.download_button(
                "📄 Descargar etiqueta (PDF)",
                data=pdf_bytes,
                file_name=f"{(asignacion_input or f'etiqueta_{sid}')}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )
            if subir_storage and asignacion_input.strip():
                url_pdf = upload_pdf_to_storage(asignacion_input.strip(), io.BytesIO(pdf_bytes))
                if url_pdf:
                    st.info(f"Subido a Storage: {url_pdf}")
        else:
            st.error("No se pudo obtener PDF de etiqueta.")

    if mark_rts:
        if _need_token():
            st.stop()
        sid = shipment_id_input.strip()
        if not sid:
            sid, _ = derive_shipment_id(order_id_input.strip(), pack_id_input.strip())
        if not sid:
            st.error("Debes indicar shipment_id o derivarlo por order_id/pack_id.")
        else:
            if mark_ready_to_ship(sid):
                st.success(f"Marcado ready_to_ship OK (shipment_id={sid})")
