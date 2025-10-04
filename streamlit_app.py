import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import pytz
import time

# ==============================
# CONFIG
# ==============================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "paquetes_mercadoenvios_chile"
TZ = pytz.timezone("America/Santiago")

# ==============================
# FUNCIONES BASE DE DATOS
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
# FUNCION PRINCIPAL DE ESCANEO
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
# NUEVO: HELPERS PARA SECCIÓN DATOS
# ==============================
# No rompe nada existente; usa la misma tabla y cliente.
ALL_COLUMNS = [
    "id", "asignacion", "guia", "fecha_ingreso", "estado_escaneo",
    "asin", "cantidad", "estado_orden", "estado_envio",
    "archivo_adjunto", "url_imagen", "comentario", "descripcion",
    "fecha_impresion", "titulo", "orden_meli", "pack_id"
]
REQUIRED_FIELDS = ["asignacion", "orden_meli"]
LOCKED_FIELDS   = ["asignacion", "orden_meli", "pack_id"]  # no editables

def datos_column_defaults():
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

def datos_get_all_rows(limit=5000):
    return supabase.table(TABLE_NAME).select("*").order("id", desc=True).limit(limit).execute()

def datos_insert_row(payload: dict):
    clean = {k: v for k, v in payload.items() if k in ALL_COLUMNS and k != "id"}
    return supabase.table(TABLE_NAME).insert(clean).execute()

def datos_update_rows_bulk(rows_to_update):
    """Actualiza por id; ignora columnas bloqueadas y 'id'."""
    errors = []
    for row in rows_to_update:
        rid = row.get("id")
        if not rid:
            continue
        upd = {k: v for k, v in row.items() if k in ALL_COLUMNS and k not in (LOCKED_FIELDS + ["id"])}
        if not upd:
            continue
        try:
            supabase.table(TABLE_NAME).update(upd).eq("id", rid).execute()
        except Exception as e:
            errors.append((rid, str(e)))
    return errors

def datos_find_duplicates(asignacion, orden_meli, pack_id):
    """Devuelve filas que coinciden por asignacion OR orden_meli OR pack_id."""
    clauses = []
    if asignacion: clauses.append(f"asignacion.eq.{asignacion}")
    if orden_meli: clauses.append(f"orden_meli.eq.{orden_meli}")
    if pack_id:    clauses.append(f"pack_id.eq.{pack_id}")
    if not clauses:
        return []
    res = supabase.table(TABLE_NAME).select("id,asignacion,orden_meli,pack_id,guia,titulo") \
        .or_(",".join(clauses)).limit(50).execute()
    return res.data or []

# ==============================
# UI
# ==============================
st.set_page_config(page_title="Escáner Bodega", layout="wide")

if "page" not in st.session_state:
    st.session_state.page = "ingresar"
if "last_input" not in st.session_state:
    st.session_state.last_input = ""
if "last_time" not in st.session_state:
    st.session_state.last_time = time.time()

# Barra de navegación (se agrega botón DATOS)
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

# Fondo según página (se agrega color para DATOS)
if st.session_state.page == "ingresar":
    st.markdown("<style>.stApp{background-color: #71A9D9;}</style>", unsafe_allow_html=True)
elif st.session_state.page == "imprimir":
    st.markdown("<style>.stApp{background-color: #71D999;}</style>", unsafe_allow_html=True)
else:  # datos
    st.markdown("<style>.stApp{background-color: #F2F4F4;}</style>", unsafe_allow_html=True)

# Título
st.header(
    "📦 INGRESAR PAQUETES" if st.session_state.page == "ingresar"
    else ("🖨️ IMPRIMIR GUIAS" if st.session_state.page == "imprimir" else "🗃️ DATOS")
)

# ==============================
# ESCANEO (se mantiene igual y sólo se ejecuta en ingresar/imprimir)
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    # Checkbox automático (desactivado por defecto)
    auto_scan = st.checkbox("Escaneo automático", value=False)

    # Input de escaneo
    scan_val = st.text_area("Escanea aquí (o pega el número de guía)", key="scan_input")

    # Procesar manual
    if st.button("Procesar escaneo"):
        process_scan(scan_val.strip())

    # Escaneo automático
    if auto_scan:
        if scan_val != st.session_state.last_input and len(scan_val.strip()) > 8:
            now_t = time.time()
            if now_t - st.session_state.last_time > 1:
                process_scan(scan_val.strip())
                st.session_state.last_input = scan_val
                st.session_state.last_time = now_t

# ==============================
# TABLA LOG DIRECTO DE SUPABASE (solo para ingresar/imprimir)
# ==============================
if st.session_state.page in ("ingresar", "imprimir"):
    st.subheader("Registro de escaneos (últimos 60 días)")
    rows = get_logs(st.session_state.page)
    df = pd.DataFrame(rows)

    # Columnas visibles
    visible_cols = ["asignacion", "guia", "fecha_ingreso", "estado_escaneo",
                    "estado_orden", "estado_envio", "archivo_adjunto", "comentario", "titulo", "asin"]

    df = df[[c for c in visible_cols if c in df.columns]]

    # Botón en columna archivo_adjunto
    if "archivo_adjunto" in df.columns:
        def make_button(url):
            if url:
                return f'<a href="{url}" target="_blank"><button>Descargar</button></a>'
            return "No disponible"
        df["archivo_adjunto"] = df["archivo_adjunto"].apply(make_button)

    # Mostrar tabla
    if not df.empty:
        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No hay registros aún.")

    # Export CSV
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download Filtered CSV", csv, f"log_{st.session_state.page}.csv", "text/csv")

# ==============================
# NUEVO: SECCIÓN DATOS (formulario + editor)
# ==============================
if st.session_state.page == "datos":
    st.markdown("<div style='background:#F2F4F4;padding:10px;border-radius:8px;'><h3>Ingreso manual y edición de datos</h3></div>", unsafe_allow_html=True)

    # ---------- Formulario de alta manual ----------
    st.subheader("Ingresar registro manual")
    with st.form("form_datos_manual"):
        c = datos_column_defaults()

        colA, colB, colC = st.columns(3)
        c["asignacion"] = colA.text_input("asignacion *", value=c["asignacion"])
        c["orden_meli"] = colB.text_input("orden_meli *", value=c["orden_meli"])
        c["pack_id"]    = colC.text_input("pack_id (opcional)", value=c["pack_id"])

        col1, col2, col3 = st.columns(3)
        c["guia"]   = col1.text_input("guia", value=c["guia"])
        c["titulo"] = col2.text_input("titulo", value=c["titulo"])
        c["asin"]   = col3.text_input("asin", value=c["asin"])

        col4, col5, col6 = st.columns(3)
        c["cantidad"]     = col4.number_input("cantidad", value=int(c["cantidad"]), min_value=0, step=1)
        c["estado_orden"] = col5.text_input("estado_orden", value=c["estado_orden"])
        c["estado_envio"] = col6.text_input("estado_envio", value=c["estado_envio"])

        c["archivo_adjunto"] = st.text_input("archivo_adjunto (URL)", value=c["archivo_adjunto"])
        c["url_imagen"]      = st.text_input("url_imagen (URL)", value=c["url_imagen"])
        c["comentario"]      = st.text_area("comentario", value=c["comentario"])
        c["descripcion"]     = st.text_area("descripcion", value=c["descripcion"])

        submitted = st.form_submit_button("➕ Guardar registro")

    if submitted:
        missing = [f for f in REQUIRED_FIELDS if not str(c.get(f, "")).strip()]
        if missing:
            st.error(f"Faltan campos obligatorios: {', '.join(missing)}")
        else:
            dups = datos_find_duplicates(c["asignacion"].strip(), c["orden_meli"].strip(), (c["pack_id"] or "").strip())
            if dups:
                st.warning("⚠️ Ya existen registros que coinciden por asignacion / orden_meli / pack_id:")
                st.dataframe(pd.DataFrame(dups), use_container_width=True, hide_index=True)
                if st.checkbox("Forzar inserción a pesar de duplicados"):
                    try:
                        datos_insert_row(c)
                        st.success("Registro insertado (forzado).")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error insertando: {e}")
            else:
                try:
                    datos_insert_row(c)
                    st.success("Registro insertado.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error insertando: {e}")

    st.divider()

    # ---------- Editor de base (bloqueando 3 columnas) ----------
    st.subheader("Editar base de datos (bloqueado: asignacion, orden_meli, pack_id)")

    res_all = datos_get_all_rows()
    df_all = pd.DataFrame(res_all.data) if res_all and res_all.data else pd.DataFrame()

    if df_all.empty:
        st.info("No hay registros.")
    else:
        show_cols = [c for c in ALL_COLUMNS if c in df_all.columns]
        df_all = df_all[show_cols]

        edited_df = st.data_editor(
            df_all,
            use_container_width=True,
            hide_index=True,
            disabled=LOCKED_FIELDS,   # ← bloquea esas columnas
            num_rows="fixed"          # no permite agregar filas desde la grilla
        )

        if st.button("💾 Guardar cambios"):
            diffs = []
            for i in range(len(df_all)):
                before = df_all.iloc[i].to_dict()
                after  = edited_df.iloc[i].to_dict()
                delta = {}
                for k in show_cols:
                    if k in LOCKED_FIELDS or k == "id":
                        continue
                    b, a = before.get(k), after.get(k)
                    if (pd.isna(b) and pd.isna(a)) or (b == a):
                        continue
                    delta[k] = a
                if delta:
                    delta["id"] = int(after["id"])
                    diffs.append(delta)

            if not diffs:
                st.info("No hay cambios que guardar.")
            else:
                errs = datos_update_rows_bulk(diffs)
                if errs:
                    st.error(f"Guardado con {len(errs)} error(es).")
                    st.write(errs[:10])
                else:
                    st.success(f"Cambios guardados en {len(diffs)} fila(s).")
                    st.rerun()


