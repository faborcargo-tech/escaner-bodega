# streamlit_app.py
import os
import json
import time
import io
from datetime import datetime, timedelta

import pytz
import requests
import pandas as pd
import streamlit as st
from supabase import create_client, Client

# ============================================================
# ✅ BLOQUE ESTABLE — CONFIGURACIÓN GENERAL (NO MODIFICAR)
# Centraliza URL/KEY de Supabase y parámetros base.
# Evita errores de inicialización y mantiene timezone consistente.
# ============================================================

st.set_page_config(page_title="Escáner Bodega", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
STORAGE_BUCKET = "etiquetas"
TZ = pytz.timezone("America/Santiago")

# ============================================================
# 🔐 TOKENS MERCADO LIBRE — Helpers robustos (NUEVO)
# - Acepta MELI_APP_ID o MELI_CLIENT_ID (y formato anidado [meli])
# - Si solo hay REFRESH_TOKEN, refresca automáticamente
# - Guarda tokens locales en meli_tokens.json (opcional)
# ============================================================

TOKENS_PATH = "meli_tokens.json"


def _read_secrets_block():
    """
    Lee secrets en formato plano o anidado [meli].
    Acepta alias: APP_ID/CLIENT_ID, ACCESS_TOKEN/access_token, etc.
    """
    def _get_from(obj, *keys, default=""):
        for k in keys:
            if k in obj and obj.get(k):
                return str(obj[k])
        return default

    s = {}
    try:
        s = dict(st.secrets)
    except Exception:
        pass

    nested = s.get("meli", {}) if isinstance(s.get("meli", {}), dict) else {}

    app_id = _get_from(s, "MELI_APP_ID", "MELI_CLIENT_ID") or _get_from(nested, "app_id", "client_id")
    client_secret = _get_from(s, "MELI_CLIENT_SECRET") or _get_from(nested, "client_secret")
    access_token = _get_from(s, "MELI_ACCESS_TOKEN") or _get_from(nested, "access_token")
    refresh_token = _get_from(s, "MELI_REFRESH_TOKEN") or _get_from(nested, "refresh_token")
    redirect_uri = _get_from(s, "MELI_REDIRECT_URI") or _get_from(nested, "redirect_uri")

    return app_id, client_secret, access_token, refresh_token, redirect_uri


def _load_tokens():
    """Combina secrets + archivo local (el archivo puede sobreescribir si trae valores)."""
    app_id, client_secret, acc, ref, redirect_uri = _read_secrets_block()
    data = {
        "app_id": app_id or "",
        "client_secret": client_secret or "",
        "access_token": acc or "",
        "refresh_token": ref or "",
        "redirect_uri": redirect_uri or "",
        "expires_at": 0,
    }
    if os.path.exists(TOKENS_PATH):
        try:
            with open(TOKENS_PATH, "r", encoding="utf-8") as f:
                disk = json.load(f)
            for k in ["app_id", "client_secret", "access_token", "refresh_token", "redirect_uri", "expires_at"]:
                if disk.get(k):
                    data[k] = disk[k]
        except Exception:
            pass
    return data


def _save_tokens(tok: dict):
    try:
        with open(TOKENS_PATH, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "app_id": tok.get("app_id"),
                    "client_secret": tok.get("client_secret"),
                    "access_token": tok.get("access_token"),
                    "refresh_token": tok.get("refresh_token"),
                    "redirect_uri": tok.get("redirect_uri"),
                    "expires_at": tok.get("expires_at", 0),
                },
                f,
            )
    except Exception:
        pass


def _save_tokens_direct(app_id, client_secret, access_token, refresh_token, expires_in=10800, redirect_uri=""):
    tok = {
        "app_id": app_id,
        "client_secret": client_secret,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "redirect_uri": redirect_uri,
        "expires_at": int(time.time()) + int(expires_in) - 60,
    }
    _save_tokens(tok)
    return tok


def _delete_local_tokens():
    try:
        if os.path.exists(TOKENS_PATH):
            os.remove(TOKENS_PATH)
            return True
    except Exception:
        pass
    return False


def _refresh_with_refresh_token(app_id, client_secret, refresh_token):
    url = "https://api.mercadolibre.com/oauth/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": app_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }
    r = requests.post(url, data=payload, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Refresh falló ({r.status_code}): {r.text}")
    return r.json()


def ensure_access_token():
    """
    Devuelve un access_token válido. Si está vencido y hay refresh_token, renueva.
    Lanza excepción si faltan credenciales.
    """
    t = _load_tokens()
    app_id, client_secret = t.get("app_id"), t.get("client_secret")
    acc, ref = t.get("access_token"), t.get("refresh_token")
    exp = int(t.get("expires_at") or 0)

    # válido aún
    if acc and exp and time.time() < exp:
        return acc

    # renovar con refresh
    if ref and app_id and client_secret:
        j = _refresh_with_refresh_token(app_id, client_secret, ref)
        new_acc = j.get("access_token")
        new_ref = j.get("refresh_token") or ref
        exp_in = int(j.get("expires_in", 10800))
        t["access_token"] = new_acc
        t["refresh_token"] = new_ref
        t["expires_at"] = int(time.time()) + exp_in - 60
        _save_tokens(t)
        return new_acc

    # si hay access sin expires, intentar igual
    if acc:
        return acc

    raise RuntimeError("Faltan credenciales: ACCESS_TOKEN o REFRESH_TOKEN + APP_ID + CLIENT_SECRET.")

# ============================================================
# ✅ BLOQUE ESTABLE — STORAGE (NO MODIFICAR)
# Sube/reemplaza PDFs con upsert.
# Fuerza MIME application/pdf para evitar que el navegador muestre "código".
# ============================================================


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
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, file_bytes, {"upsert": "true"})
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


def upload_pdf_bytes(asignacion: str, pdf_bytes: bytes) -> str | None:
    """Variante para subir bytes directamente (usada en PRUEBAS)."""
    if not asignacion or not pdf_bytes:
        return None
    key_path = f"{asignacion}.pdf"
    try:
        supabase.storage.from_(STORAGE_BUCKET).upload(key_path, pdf_bytes, {"upsert": "true"})
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None
    # Forzar MIME
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
    """HEAD 200 OK -> existe."""
    if not url:
        return False
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False


# ============================================================
# ✅ BLOQUE ESTABLE — DB HELPERS (NO MODIFICAR)
# Operaciones atómicas de lectura/actualización.
# Inserta NO COINCIDENTE si la guía no existe.
# ============================================================

def lookup_by_guia(guia: str):
    res = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
    return res.data[0] if res.data else None


def update_ingreso(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update(
        {
            "fecha_ingreso": now.isoformat(),
            "estado_escaneo": "INGRESADO CORRECTAMENTE!",
        }
    ).eq("guia", guia).execute()


def update_impresion(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update(
        {
            "fecha_impresion": now.isoformat(),
            "estado_escaneo": "IMPRIMIDO CORRECTAMENTE!",
        }
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
    """Devuelve últimos 60 días de acuerdo a la sección (ingreso/impresión)."""
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


# ============================================================
# ✅ BLOQUE ESTABLE — ESCANEO (NO AUTO-ABRIR PDF) (NO MODIFICAR)
# Punto 3: elimina intento de auto-abrir PDF.
# Mantiene botón "Descargar nuevamente ..." que funciona perfecto.
# Inserta un nuevo registro de impresión (log persistente).
# ============================================================

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

        # 1) Mostrar botón de descarga confiable
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

        # 2) Insertar un NUEVO registro para el log de impresión
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
            # RLS estricta: si falla inserción del log, no rompe el flujo.
            pass


# ============================================================
# ✅ BLOQUE ESTABLE — PERSISTENCIA DE SECCIÓN (NO MODIFICAR)
# Punto 2: cada sección usa ?page=... en la URL.
# Al refrescar, NO cambia de sección.
# ============================================================

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


# ============================================================
# ✅ BLOQUE ESTABLE — UI PRINCIPAL (NO MODIFICAR)
# Mantiene estilos por sección.
# ============================================================

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
    "pruebas": "#F9E0A6",
}.get(st.session_state.page, "#F2F4F4")
st.markdown(f"<style>.stApp{{background-color:{bg};}}</style>", unsafe_allow_html=True)

st.header(
    "📦 INGRESAR PAQUETES"
    if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else ("🗃️ DATOS" if st.session_state.page == "datos" else "🧪 PRUEBAS"))
)

# ============================================================
# ✅ BLOQUE ESTABLE — LOG DE ESCANEOS (NO MODIFICAR)
# Botón por fila que descarga el PDF desde Supabase si existe.
# ============================================================

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


# ============================================================
# SECCIONES INGRESAR / IMPRIMIR
# ============================================================

if st.session_state.page in ("ingresar", "imprimir"):
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    render_log_with_download_buttons(rows, st.session_state.page)

# ============================================================
# CRUD — PÁGINA DATOS
# ============================================================

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
    for field, value in [("asignacion", asignacion), ("orden_meli", orden_meli), ("pack_id", pack_id)]:
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
    data["guia"] = col1.text_input("guia", value=(data.get("guia") or ""))
    data["titulo"] = col2.text_input("titulo", value=(data.get("titulo") or ""))
    data["asin"] = col3.text_input("asin", value=(data.get("asin") or ""))

    col4, col5, col6 = st.columns(3)
    data["cantidad"] = col4.number_input("cantidad", value=int(data.get("cantidad") or 1), min_value=0, step=1)
    data["estado_orden"] = col5.text_input("estado_orden", value=(data.get("estado_orden") or ""))
    data["estado_envio"] = col6.text_input("estado_envio", value=(data.get("estado_envio") or ""))

    # PDF actual (si existe)
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


# --- Página DATOS
if st.session_state.page == "datos":
    st.markdown("### Base de datos")

    colf1, colf2, colf4 = st.columns([2, 1, 1])
    with colf1:
        search = st.text_input("Buscar (asignacion / guia / orden_meli / pack_id / titulo)", "")
    with colf2:
        page_size = st.selectbox("Filas por página", [25, 50, 100, 200], index=1)
    with colf4:
        # Punto 1: Botón "Nuevo registro" restaurado
        if st.button("➕ Nuevo registro", use_container_width=True):
            open_modal_new()

    # Paginación simple
    colp1, colp2, colp3 = st.columns([1, 1, 6])
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
            disabled=show_cols,  # no permitir edición inline; usar modal
            column_config=column_config,
        )

        # Detectar fila solicitada para editar
        try:
            if "Editar" in edited_df.columns:
                # tanto para ButtonColumn (True en la fila clickeada)
                # como para Checkbox fallback
                clicked = edited_df.index[edited_df["Editar"] is True].tolist()
                if not clicked:
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
# 🧪 PÁGINA PRUEBAS — Imprimir etiquetas sin tocar la base
# ============================================================

def derive_shipment_id(order_id: str, pack_id: str, token: str):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    # Desde order_id
    if order_id:
        try:
            r = requests.get(f"https://api.mercadolibre.com/orders/{order_id}", headers=headers, timeout=25)
            if r.status_code != 200:
                return None, f"GET /orders/{order_id} -> {r.status_code}: {r.text}"
            j = r.json()
            ship = (j.get("shipping") or {}).get("id")
            if ship:
                return str(ship), None
            return None, "La orden existe pero aún no tiene shipment_id (se crea con demora)."
        except Exception as e:
            return None, f"Error consultando order: {e}"

    # Desde pack_id
    if pack_id:
        try:
            r = requests.get(f"https://api.mercadolibre.com/packs/{pack_id}", headers=headers, timeout=25)
            if r.status_code != 200:
                return None, f"GET /packs/{pack_id} -> {r.status_code}: {r.text}"
            j = r.json()
            ship = (j.get("shipment") or {}).get("id")
            if ship:
                return str(ship), None
            return None, "El pack no tiene shipment_id (ventas not_specified u otras condiciones)."
        except Exception as e:
            return None, f"Error consultando pack: {e}"

    return None, "Debes informar shipment_id o (order_id / pack_id) para derivarlo."


def get_shipment_info(shipment_id: str, token: str):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "x-format-new": "true"}
    r = requests.get(f"https://api.mercadolibre.com/shipments/{shipment_id}", headers=headers, timeout=25)
    return r.status_code, r.text, (r.json() if r.headers.get("Content-Type", "").startswith("application/json") else None)


def get_label_pdf(shipment_id: str, token: str) -> bytes:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/pdf"}
    url = f"https://api.mercadolibre.com/shipment_labels?shipment_ids={shipment_id}&response_type=pdf"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Labels: {r.status_code} {r.text}")
    return r.content


def mark_ready_to_ship(shipment_id: str, token: str):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = requests.post(
        f"https://api.mercadolibre.com/shipments/{shipment_id}/process/ready_to_ship", headers=headers, timeout=25
    )
    return r.status_code, r.text


if st.session_state.page == "pruebas":
    st.markdown("#### Prueba de impresión de etiqueta (no toca la base)")

    c1, c2, c3 = st.columns(3)
    with c1:
        input_shipment = st.text_input("shipment_id (opcional)")
    with c2:
        input_order = st.text_input("order_id (opcional)")
    with c3:
        input_pack = st.text_input("pack_id (opcional)")

    asignacion_name = st.text_input("asignación (para nombrar PDF si subes a Storage)")
    subir_storage = st.checkbox("Subir a Storage (reemplaza si existe)", value=False)

    bcol1, bcol2, bcol3 = st.columns([1.3, 1.7, 1])
    probar = bcol1.button("🔎 Probar")
    marcar = bcol2.button("📌 Marcar 'Ya tengo el producto' (ready_to_ship)")
    # espacio para el botón de descarga una vez obtenido el PDF

    # Bloque credenciales locales (opcional)
    with st.expander("⚙️ Cargar/gestionar credenciales locales (opcional)"):
        tok = _load_tokens()
        app_id_in = st.text_input("App ID", value=tok.get("app_id") or "")
        client_secret_in = st.text_input("Client Secret", value=tok.get("client_secret") or "", type="password")
        access_in = st.text_input("Access token", value=tok.get("access_token") or "", type="password")
        refresh_in = st.text_input("Refresh token", value=tok.get("refresh_token") or "", type="password")
        redirect_in = st.text_input("Redirect URI (opcional)", value=tok.get("redirect_uri") or "")
        expires_in = st.number_input("Expires in (segundos)", value=10800, step=60)

        lc1, lc2, lc3 = st.columns(3)
        if lc1.button("💾 Guardar credenciales locales"):
            if not app_id_in or not client_secret_in:
                st.error("App ID y Client Secret son obligatorios para refrescar.")
            elif not access_in and not refresh_in:
                st.error("Proporciona al menos Access token o Refresh token.")
            else:
                _save_tokens_direct(
                    app_id_in.strip(),
                    client_secret_in.strip(),
                    access_in.strip(),
                    refresh_in.strip(),
                    int(expires_in),
                    redirect_in.strip(),
                )
                st.success("Credenciales guardadas en meli_tokens.json")
        if lc2.button("🗑️ Borrar tokens locales"):
            st.success("Eliminado.") if _delete_local_tokens() else st.info("No había tokens locales.")
        if lc3.button("👀 Ver tokens locales (enmascarado)"):
            tt = _load_tokens()
            st.write(
                {
                    "app_id": tt.get("app_id"),
                    "client_secret": tt.get("client_secret") and "•••" + tt["client_secret"][-6:],
                    "access_token": tt.get("access_token") and "•••" + tt["access_token"][-10:],
                    "refresh_token": tt.get("refresh_token") and "•••" + tt["refresh_token"][-10:],
                    "expires_at": tt.get("expires_at"),
                }
            )

    # Acciones
    if probar:
        try:
            token = ensure_access_token()
        except Exception as e:
            st.error(f"Error de credenciales: {e}")
            st.stop()

        sid = (input_shipment or "").strip()
        note_msgs = []

        if not sid:
            sid, err = derive_shipment_id((input_order or "").strip(), (input_pack or "").strip(), token)
            if not sid:
                st.error(err)
                st.stop()
            else:
                note_msgs.append(f"shipment_id derivado: **{sid}**")

        # Obtener info del shipment (con x-format-new: true)
        code, raw_text, j = get_shipment_info(sid, token)
        with st.expander("Respuesta /shipments/{id}"):
            st.code(raw_text or "", language="json")
        if code != 200 or not isinstance(j, dict):
            st.error(f"/shipments/{sid} -> {code}")
            st.stop()

        status = j.get("status")
        substatus = j.get("substatus")
        logistic = j.get("logistic") or {}
        mode = logistic.get("mode")
        ltype = logistic.get("type")

        st.info(f"Estado actual: **{status} / {substatus}** | mode: **{mode}** | type: **{ltype}**")

        if mode != "me2":
            st.error("El envío no es Mercado Envíos 2 (me2). No es imprimible por vendedor.")
            st.stop()

        if status != "ready_to_ship" or (substatus not in {"ready_to_print", "printed", "ready_for_dropoff", "ready_for_pickup"}):
            st.warning("Aún no está en estado imprimible. Debe estar ready_to_ship con substatus ready_to_print/printed.")
            # no detener: algunos sitios permiten descargar igualmente cuando está 'printed'
            # st.stop()

        # Descargar etiqueta PDF
        try:
            pdf_bytes = get_label_pdf(sid, token)
            if pdf_bytes[:4] != b"%PDF":
                raise RuntimeError("El archivo obtenido no es un PDF válido.")
        except Exception as e:
            st.error(f"No se pudo obtener etiqueta: {e}")
            st.stop()

        st.success("Etiqueta obtenida.")
        st.download_button(
            "📄 Descargar etiqueta.pdf",
            data=pdf_bytes,
            file_name=f"{(asignacion_name or 'etiqueta')}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )

        if subir_storage:
            if not asignacion_name.strip():
                st.warning("Para subir a Storage, completa el campo 'asignación'.")
            else:
                url_pdf = upload_pdf_bytes(asignacion_name.strip(), pdf_bytes)
                if url_pdf:
                    st.success(f"Subido a Storage como **{asignacion_name}.pdf**")
                    st.markdown(f"[Abrir en navegador]({url_pdf})")
                else:
                    st.error("No se pudo subir a Storage.")

        if note_msgs:
            for m in note_msgs:
                st.caption(m)

    if marcar:
        try:
            token = ensure_access_token()
        except Exception as e:
            st.error(f"Error de credenciales: {e}")
            st.stop()

        sid = (input_shipment or "").strip()
        if not sid:
            sid, err = derive_shipment_id((input_order or "").strip(), (input_pack or "").strip(), token)
            if not sid:
                st.error(err)
                st.stop()

        code, txt = mark_ready_to_ship(sid, token)
        if code == 200:
            st.success("Marcado como 'ready_to_ship' correctamente.")
        else:
            st.error(f"Error marcando ready_to_ship -> {code}: {txt}")
