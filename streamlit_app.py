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
TZ = pytz.timezone("America/Santiago")

# ==============================
# FUNCIONES BASE DE DATOS (EXISTENTES)
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
    """Obtiene logs de Supabase según la página (ingreso o impresión)."""
    cutoff = (datetime.now(TZ) - timedelta(days=60)).isoformat()

    if page == "ingresar":
        response = supabase.table(TABLE_NAME).select("*").gte("fecha_ingreso", cutoff).order("fecha_ingreso", desc=True).execute()
    else:
        response = supabase.table(TABLE_NAME).select("*").gte("fecha_impresion", cutoff).order("fecha_impresion", desc=True).execute()

    return response.data if response.data else []

# ==============================
# FUNCION PRINCIPAL DE ESCANEO (EXISTENTE)
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
            if archivo:
                st.success("Etiqueta disponible, abriendo en nueva pestaña...")
                st.markdown(f"""<script>window.open("{archivo}", "_blank");</script>""", unsafe_allow_html=True)
            else:
                st.warning("⚠️ Etiqueta no disponible")

    else:
        insert_no_coincidente(guia)
        st.error(f"⚠️ Guía {guia} no encontrada. Se registró como NO COINCIDENTE.")

# ==============================
# NUEVO: HELPERS PARA SECCIÓN DATOS (MODAL + CRUD)
# ==============================
ALL_COLUMNS = [
    "id", "asignacion", "guia", "fecha_ingreso", "estado_escaneo",
    "asin", "cantidad", "estado_orden", "estado_envio",
    "archivo_adjunto", "url_imagen", "comentario", "descripcion",
    "fecha_impresion", "titulo", "orden_meli", "pack_id"
]
REQUIRED_FIELDS = ["asignacion", "orden_meli"]
LOCKED_FIELDS_EDIT = ["asignacion", "orden_meli"]  # bloqueadas SOLO en edición

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
        # Búsqueda simple por asignacion/guia/orden_meli/pack_id/titulo
        q = q.or_(f"asignacion.ilike.%{search}%,guia.ilike.%{search}%,orden_meli.ilike.%{search}%,pack_id.ilike.%{search}%,titulo.ilike.%{search}%")
    q = q.range(offset, offset + limit - 1)
    res = q.execute()
    return res.data or []

def datos_find_duplicates(asignacion, orden_meli, pack_id):
    clauses = []
    if asignacion: clauses.append(f"asignacion.eq.{asignacion}")
    if orden_meli: clauses.append(f"orden_meli.eq.{orden_meli}")
    if pack_id:    clauses.append(f"pack_id.eq.{pack_id}")
    if not clauses:
        return []
    res = supabase.table(TABLE_NAME).select("id,asignacion,orden_meli,pack_id,guia,titulo") \
        .or_(",".join(clauses)).limit(50).execute()
    return res.data or []

def datos_insert(payload: dict):
    clean = {k: v for k, v in payload.items() if k in ALL_COLUMNS and k != "id"}
    return supabase.table(TABLE_NAME).insert(clean).execute()

def datos_update(id_val: int, payload: dict):
    # En edición, nunca aceptamos cambios en LOCKED_FIELDS_EDIT
    clean = {k: v for k, v in payload.items() if k in ALL_COLUMNS and k not in (LOCKED_FIELDS_EDIT + ["id"])}
    if not clean:
        return None
    return supabase.table(TABLE_NAME).update(clean).eq("id", id_val).execute()

# ======== UI STATE para modal ========
if "datos_modal_open" not in st.session_state:
    st.session_state.datos_modal_open = False
if "datos_modal_mode" not in st.session_state:
    st.session_state.datos_modal_mode = "new"  # "new" | "edit"
if "datos_modal_row" not in st.session_state:
    st.session_state.datos_modal_row = datos_defaults()

def open_modal_new():
    st.session_state.datos_modal_mode = "new"
    st.session_state.datos_modal_row = datos_defaults()
    st.session_state.datos_modal_open = True

def open_modal_edit(row: dict):
    st.session_state.datos_modal_mode = "edit"
    # Prepara row con todas las llaves esperadas
    base = datos_defaults()
    base.update({k: row.get(k) for k in row.keys()})
    st.session_state.datos_modal_row = base
    st.session_state.datos_modal_open = True

def close_modal():
    st.session_state.datos_modal_open = False

# ======== Modal (con fallback si la versión de Streamlit no lo soporta) ========
def _render_form_contents():
    mode = st.session_state.datos_modal_mode
    data = st.session_state.datos_modal_row.copy()

    st.write("**Modo:** ", "Crear nuevo" if mode == "new" else f"Editar ID {data.get('id')}")
    colA, colB, colC = st.columns(3)

    # asignacion / orden_meli bloqueados solo en edición
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

    data["archivo_adjunto"] = st.text_input("archivo_adjunto (URL)", value=(data.get("archivo_adjunto") or ""))
    data["url_imagen"]      = st.text_input("url_imagen (URL)", value=(data.get("url_imagen") or ""))
    data["comentario"]      = st.text_area("comentario", value=(data.get("comentario") or ""))
    data["descripcion"]     = st.text_area("descripcion", value=(data.get("descripcion") or ""))

    col_btn1, col_btn2 = st.columns([1,1])
    submitted = col_btn1.button("💾 Guardar", use_container_width=True, key="datos_submit_btn")
    cancel    = col_btn2.button("✖️ Cancelar", use_container_width=True, key="datos_cancel_btn")

    if cancel:
        close_modal()
        st.experimental_rerun()

    if submitted:
        if st.session_state.datos_modal_mode == "new":
            # validar requeridos
            missing = [f for f in REQUIRED_FIELDS if not str(data.get(f, "")).strip()]
            if missing:
                st.error(f"Faltan campos obligatorios: {', '.join(missing)}")
                return
            # duplicados
            dups = datos_find_duplicates(data["asignacion"].strip(), data["orden_meli"].strip(), (data.get("pack_id") or "").strip())
            if dups:
                st.warning("⚠️ Existen registros coincidentes por asignacion / orden_meli / pack_id:")
                st.dataframe(pd.DataFrame(dups), use_container_width=True, hide_index=True)
                if st.checkbox("Forzar inserción a pesar de duplicados", key="force_insert"):
                    datos_insert(data)
                    st.success("Registro insertado (forzado).")
                    close_modal()
                    st.experimental_rerun()
            else:
                datos_insert(data)
                st.success("Registro insertado correctamente.")
                close_modal()
                st.experimental_rerun()
        else:
            # edición: nunca tocamos asignacion/orden_meli
            rid = int(data["id"])
            datos_update(rid, data)
            st.success(f"Registro {rid} actualizado.")
            close_modal()
            st.experimental_rerun()

# wrapper modal: usa st.dialog si existe; fallback a expander
def render_modal_if_needed():
    if not st.session_state.datos_modal_open:
        return
    if hasattr(st, "dialog"):
        @st.dialog("Formulario de registro")
        def _show_dialog():
            _render_form_contents()
        _show_dialog()
    else:
        with st.expander("Formulario de registro (vista emergente)", expanded=True):
            _render_form_contents()

# ==============================
# UI (NAV, COLORES, TITULOS)
# ==============================
if "page" not in st.session_state:
    st.session_state.page = "ingresar"
if "last_input" not in st.session_state:
    st.session_state.last_input = ""
if "last_time" not in st.session_state:
    st.session_state.last_time = time.time()

# Barra de navegación
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

# Fondo según página
if st.session_state.page == "ingresar":
    st.markdown("<style>.stApp{background-color: #71A9D9;}</style>", unsafe_allow_html=True)
elif st.session_state.page == "imprimir":
    st.markdown("<style>.stApp{background-color: #71D999;}</style>", unsafe_allow_html=True)
else:
    st.markdown("<style>.stApp{background-color: #F2F4F4;}</style>", unsafe_allow_html=True)

# Título
st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else "🗃️ DATOS")
)

# ==============================
# ESCANEO (solo ingresar/imprimir)
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
# TABLA LOG (solo ingresar/imprimir)
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
                return f'<a href="{url}" target="_blank"><button>Descargar</button></a>'
            return "No disponible"
        df["archivo_adjunto"] = df["archivo_adjunto"].apply(make_button)

    if not df.empty:
        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No hay registros aún.")

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download Filtered CSV", csv, f"log_{st.session_state.page}.csv", "text/csv")

# ==============================
# NUEVO: PÁGINA DATOS
# ==============================
if st.session_state.page == "datos":
    st.markdown("<div style='background:#F2F4F4;padding:10px;border-radius:8px;'><h3>Base de datos</h3></div>", unsafe_allow_html=True)

    # ---- Controles de tabla ----
    colf1, colf2, colf3 = st.columns([2,1,1])
    with colf1:
        search = st.text_input("Buscar (asignacion / guia / orden_meli / pack_id / titulo)", "")
    with colf2:
        page_size = st.selectbox("Filas por página", [25, 50, 100, 200], index=1)
    with colf3:
        if st.button("➕ Nuevo registro", use_container_width=True):
            open_modal_new()

    # Paginación simple
    if "datos_offset" not in st.session_state:
        st.session_state.datos_offset = 0
    colp1, colp2, colp3 = st.columns([1,1,6])
    with colp1:
        if st.button("⟵ Anterior") and st.session_state.datos_offset >= page_size:
            st.session_state.datos_offset -= page_size
    with colp2:
        if st.button("Siguiente ⟶"):
            st.session_state.datos_offset += page_size

    # Trae datos
    data_rows = datos_fetch(limit=page_size, offset=st.session_state.datos_offset, search=search)
    df_all = pd.DataFrame(data_rows)

    if df_all.empty:
        st.info("Sin registros para mostrar.")
    else:
        # Mostrar tabla de datos
        show_cols = [c for c in ALL_COLUMNS if c in df_all.columns]
        st.dataframe(df_all[show_cols], use_container_width=True, hide_index=True)

        # Botones de edición por fila (para las filas visibles)
        st.subheader("Acciones por fila")
        for _, row in df_all.iterrows():
            with st.container():
                c1, c2, c3, c4 = st.columns([6,1,1,1])
                c1.caption(f"ID {row['id']} · asignacion: {row.get('asignacion','')} · orden_meli: {row.get('orden_meli','')}")
                if c2.button("✏️ Editar", key=f"edit_{row['id']}"):
                    open_modal_edit(row.to_dict())
                # podrías agregar más acciones aquí si lo necesitas
        st.caption("Tip: Usa el buscador para filtrar y luego edita la fila que necesites.")

    # Renderiza la ventana emergente si corresponde
    render_modal_if_needed()
