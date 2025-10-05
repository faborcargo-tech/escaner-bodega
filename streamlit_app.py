import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import time

# ==============================
# CONFIG
# ==============================
st.set_page_config(page_title="Escáner Bodega", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
STORAGE_BUCKET = "etiquetas"  # PDFs de etiquetas
TZ = pytz.timezone("America/Santiago")

# ==============================
# STORAGE HELPERS (PDF)
# ==============================
def ensure_storage_bucket() -> bool:
    try:
        supabase.storage.from_(STORAGE_BUCKET).list()
        return True
    except Exception:
        # No podemos verificar con anon key, asumimos que existe
        return True
    try:
        buckets = supabase.storage.list_buckets()
        names = [b.get("name") for b in buckets]
        if STORAGE_BUCKET not in names:
            st.error(
                f"El bucket '{STORAGE_BUCKET}' no existe. "
                f"Créalo en Supabase → Storage (público) y agrega las policies de SELECT/INSERT/UPDATE."
            )
            return False
        return True
    except Exception:
        # Si el SDK no soporta list_buckets dejamos continuar
        return True

def _get_public_or_signed_url(path: str) -> str | None:
    """Intenta devolver URL pública; si no, una firmada (30 días)."""
    try:
        url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(path)
        if isinstance(url, dict):
            url = url.get("publicUrl") or url.get("public_url") or url.get("publicURL")
        if url:
            return url
    except Exception:
        pass
    try:
        signed = supabase.storage.from_(STORAGE_BUCKET).create_signed_url(path, 60 * 60 * 24 * 30)
        if isinstance(signed, dict):
            return signed.get("signedUrl") or signed.get("signedURL") or signed.get("signed_url")
        return signed
    except Exception as e:
        st.error(f"❌ No se pudo generar URL del PDF: {e}")
        return None

def upload_pdf_to_storage(asignacion: str, uploaded_file) -> str | None:
    """
    Sube el PDF como <asignacion>.pdf al bucket y devuelve URL.
    Sube bytes reales y especifica 'application/pdf'. Reemplaza si existe.
    """
    if not asignacion:
        st.error("La asignación es requerida para subir el PDF.")
        return None
    if uploaded_file is None:
        return None
    if not ensure_storage_bucket():
        return None

    key_path = f"{asignacion}.pdf"
    try:
        file_bytes = uploaded_file.read()  # bytes reales del uploader

        # 1) Intento con firma típica (algunos SDKs aceptan kwargs)
        try:
            supabase.storage.from_(STORAGE_BUCKET).upload(
                path=key_path,
                file=file_bytes,
                file_options={"content-type": "application/pdf"},
                upsert=True,
            )
        except TypeError:
            # 2) SDK antiguo: exige posicionales y no soporta 'upsert'
            try:
                supabase.storage.from_(STORAGE_BUCKET).upload(key_path, file_bytes)
            except Exception:
                # 3) Si ya existe, lo reemplazamos con update()
                supabase.storage.from_(STORAGE_BUCKET).update(key_path, file_bytes)

        return _get_public_or_signed_url(key_path)
    except Exception as e:
        st.error(f"❌ Error subiendo PDF: {e}")
        return None

# ==============================
# DB HELPERS (EXISTENTES)
# ==============================
def lookup_by_guia(guia: str):
    response = supabase.table(TABLE_NAME).select("*").eq("guia", guia).execute()
    return response.data[0] if response.data else None

def update_ingreso(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update({
        "fecha_ingreso": now.isoformat(),
        "estado_escaneo": "INGRESADO CORRECTAMENTE!"
    }).eq("guia", guia).execute()

def update_impresion(guia: str):
    now = datetime.now(TZ)
    supabase.table(TABLE_NAME).update({
        "fecha_impresion": now.isoformat()
    }).eq("guia", guia).execute()

def insert_no_coincidente(guia: str):
    now = datetime.now(TZ)
    try:
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
    except Exception as e:
        st.error(f"❌ Error al insertar NO COINCIDENTE ({guia}): {e}")

def get_logs(page: str):
    cutoff = (datetime.now(TZ) - timedelta(days=60)).isoformat()
    if page == "ingresar":
        response = supabase.table(TABLE_NAME).select("*").gte("fecha_ingreso", cutoff).order("fecha_ingreso", desc=True).execute()
    else:
        response = supabase.table(TABLE_NAME).select("*").gte("fecha_impresion", cutoff).order("fecha_impresion", desc=True).execute()
    return response.data if response.data else []

# ==============================
# ESCANEO
# ==============================
def process_scan(guia: str):
    match = lookup_by_guia(guia)
    if match:
        if st.session_state.page == "ingresar":
            update_ingreso(guia)
            st.success(f"📦 Guía {guia} ingresada correctamente")
        elif st.session_state.page == "imprimir":
            update_impresion(guia)
            archivo = match.get("archivo_adjunto", "")
            asignacion = match.get("asignacion", "etiqueta")
            if archivo:
                st.success("Etiqueta disponible, descargando…")
                js = f"""
                var a=document.createElement('a');
                a.href='{archivo}';
                a.download='{asignacion}.pdf';
                document.body.appendChild(a);a.click();a.remove();
                """
                st.components.v1.html(f"<script>{js}</script>", height=0)
            else:
                st.warning("⚠️ Etiqueta no disponible")
    else:
        insert_no_coincidente(guia)
        st.error(f"⚠️ Guía {guia} no encontrada. Se registró como NO COINCIDENTE.")

# ==============================
# DATOS (CRUD)
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
        pack_id=""
    )

def datos_fetch(limit=200, offset=0, search:str=""):
    q = supabase.table(TABLE_NAME).select("*").order("id", desc=True)
    if search:
        # búsqueda secuencial simple (equivalente a OR)
        res = q.ilike("asignacion", f"%{search}%").range(offset, offset + limit - 1).execute()
        data = res.data or []
        if not data:
            for col in ["guia", "orden_meli", "pack_id", "titulo"]:
                res = supabase.table(TABLE_NAME).select("*").ilike(col, f"%{search}%").order("id", desc=True).range(offset, offset + limit - 1).execute()
                if res.data:
                    data = res.data
                    break
        return data
    else:
        return q.range(offset, offset + limit - 1).execute().data or []

def datos_find_duplicates(asignacion, orden_meli, pack_id):
    seen = {}
    if asignacion:
        res = supabase.table(TABLE_NAME).select("id,asignacion,orden_meli,pack_id,guia,titulo").eq("asignacion", asignacion).limit(50).execute()
        for r in (res.data or []): seen[r["id"]] = r
    if orden_meli:
        res = supabase.table(TABLE_NAME).select("id,asignacion,orden_meli,pack_id,guia,titulo").eq("orden_meli", orden_meli).limit(50).execute()
        for r in (res.data or []): seen[r["id"]] = r
    if pack_id:
        res = supabase.table(TABLE_NAME).select("id,asignacion,orden_meli,pack_id,guia,titulo").eq("pack_id", pack_id).limit(50).execute()
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

# ----------------- UI STATE MODAL -----------------
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
    st.session_state.datos_modal_mode = "edit"
    base = datos_defaults()
    base.update({k: row.get(k) for k in row.keys()})
    st.session_state.datos_modal_row = base
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

    # Si ya hay PDF, botón de descarga
    current_pdf = data.get("archivo_adjunto") or ""
    if current_pdf:
        st.markdown(f"[📥 Descargar etiqueta actual]({current_pdf})", unsafe_allow_html=True)

    data["archivo_adjunto"] = st.text_input("archivo_adjunto (URL)", value=current_pdf)
    data["url_imagen"]      = st.text_input("url_imagen (URL)", value=(data.get("url_imagen") or ""))
    data["comentario"]      = st.text_area("comentario", value=(data.get("comentario") or ""))
    data["descripcion"]     = st.text_area("descripcion", value=(data.get("descripcion") or ""))

    # Subir/reemplazar PDF
    st.caption("Subir etiqueta PDF (reemplaza la actual si existe)")
    pdf_file = st.file_uploader("Seleccionar PDF", type=["pdf"], accept_multiple_files=False)

    col_btn1, col_btn2 = st.columns([1,1])
    submitted = col_btn1.button("💾 Guardar", use_container_width=True, key="datos_submit_btn")
    cancel    = col_btn2.button("✖️ Cancelar", use_container_width=True, key="datos_cancel_btn")

    if cancel:
        close_modal()
        st.rerun()

    if submitted:
        # Si hay PDF adjunto, subir antes y setear URL
        if pdf_file is not None:
            asign = (data.get("asignacion") or "").strip()
            if not asign:
                st.error("Debes completar 'asignacion' para subir el PDF.")
                return
            url_pdf = upload_pdf_to_storage(asign, pdf_file)
            if url_pdf:
                data["archivo_adjunto"] = url_pdf

        if st.session_state.datos_modal_mode == "new":
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

# ==============================
# UI (NAV & COLORES)
# ==============================
if "page" not in st.session_state:
    st.session_state.page = "ingresar"
if "last_input" not in st.session_state:
    st.session_state.last_input = ""
if "last_time" not in st.session_state:
    st.session_state.last_time = time.time()

col1, col2, col3 = st.columns([1,1,1])
with col1:
    if st.button("INGRESAR PAQUETES"):
        st.session_state.page = "ingresar"
with col2:
    if st.button("IMPRIMIR GUIAS"):
        st.session_state.page = "imprimir"
with col3:
    if st.button("🗃️ DATOS"):
        st.session_state.page = "datos"

if st.session_state.page == "ingresar":
    st.markdown("<style>.stApp{background-color: #71A9D9;}</style>", unsafe_allow_html=True)
elif st.session_state.page == "imprimir":
    st.markdown("<style>.stApp{background-color: #71D999;}</style>", unsafe_allow_html=True)
else:
    st.markdown("<style>.stApp{background-color: #F2F4F4;}</style>", unsafe_allow_html=True)

st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else "🗃️ DATOS")
)

# ==============================
# ESCANEO (ingresar / imprimir)
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    auto_scan = st.checkbox("Escaneo automático", value=False)
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)", key="scan_input")
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())
    if auto_scan:
        if scan_val != st.session_state.last_input and len(scan_val.strip()) > 8:
            now_t = time.time()
            if now_t - st.session_state.last_time > 1:
                process_scan(scan_val.strip())
                st.session_state.last_input = scan_val
                st.session_state.last_time = now_t

# ==============================
# LOG DE ESCANEOS (ingresar / imprimir)
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    df = pd.DataFrame(rows)

    visible_cols = ["asignacion", "guia", "fecha_ingreso", "estado_escaneo",
                    "estado_orden", "estado_envio", "archivo_adjunto", "comentario", "titulo", "asin"]
    df = df[[c for c in visible_cols if c in df.columns]]

    if "archivo_adjunto" in df.columns:
        def make_button(url):
            if url:
                return f'<a href="{url}" target="_blank" download><button>Descargar</button></a>'
            return "No disponible"
        df["archivo_adjunto"] = df["archivo_adjunto"].apply(make_button)

    if not df.empty:
        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No hay registros aún.")

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download Filtered CSV", csv, f"log_{st.session_state.page}.csv", "text/csv")

# ==============================
# PÁGINA DATOS
# ==============================
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

    # Filtro rápido pegado a la tabla
    solo_sin_guia = st.checkbox("Solo sin guía", value=False)
    if solo_sin_guia and not df_all.empty and "guia" in df_all.columns:
        df_all = df_all[df_all["guia"].isna() | (df_all["guia"].astype(str).str.strip() == "")]

    if df_all.empty:
        st.info("Sin registros para mostrar.")
    else:
        show_cols = [c for c in ALL_COLUMNS if c in df_all.columns]
        df_all = df_all.copy()

        # Columna 'Editar' como primera
        has_button_col = hasattr(st, "column_config") and hasattr(st.column_config, "ButtonColumn")
        df_all["Editar"] = False
        if has_button_col:
            column_config = {"Editar": st.column_config.ButtonColumn("Editar", help="Editar fila", icon="✏️", width="small")}
        else:
            column_config = {"Editar": st.column_config.CheckboxColumn("Editar", help="Marca para editar", default=False)}

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
            if "Editar" in edited_df.columns and edited_df["Editar"].any():
                idx = edited_df.index[edited_df["Editar"]].tolist()[0]
                row_dict = edited_df.loc[idx].to_dict()
                row_dict.pop("Editar", None)
                open_modal_edit(row_dict)
        except Exception:
            pass

    render_modal_if_needed()



