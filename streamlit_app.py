# streamlit_app.py
# App de Streamlit para imprimir etiquetas ME2 con soporte de packs, buffering y refresh automático de token.
# Usa el módulo meli_envios2.py (del mismo directorio).
# Python 3.8+

import io
import os
import json
import pandas as pd
import streamlit as st

# Importamos helpers del módulo que te pasé antes
from meli_envios2 import (
    descargar_etiqueta_por_order_o_pack,
    _meli_ready_to_ship,
    _meli_get_user_id,
    _meli_get_shipment,
    _meli_get_shipment_id_from_order,
    _meli_get_shipment_id_from_pack,
    _explicacion_estado_label,
)

# ===================== CONFIG BÁSICA =====================
st.set_page_config(page_title="Escáner Bodega - Guías ME2", page_icon="📦", layout="wide")

# [ANCHOR: ESTILO]
HIDE_FOOTER = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.block-container {padding-top: 1.2rem;}
</style>
"""
st.markdown(HIDE_FOOTER, unsafe_allow_html=True)

# ===================== SIDEBAR =====================
with st.sidebar:
    st.title("📦 Escáner Bodega")
    st.caption("Impresión de guías ME2 (packs, buffering y refresh token).")

    # Estado de usuario (seller)
    try:
        seller_id = _meli_get_user_id()
    except Exception:
        seller_id = None

    st.markdown("**Estado del acceso**")
    if seller_id:
        st.success(f"Autenticado. Seller ID: {seller_id}")
    else:
        st.warning("No se pudo leer /users/me. Verifica tokens (access/refresh).")

    st.markdown("---")
    st.markdown("**Tips**")
    st.write("- Si el envío está **buffered**, la etiqueta se habilita en la fecha indicada.")
    st.write("- Solo imprime si status = ready_to_ship y substatus = ready_to_print/printed.")
    st.write("- Fulfillment no permite imprimir etiqueta de envío (solo stock).")

# ===================== UTILIDADES =====================

# [ANCHOR: SAVE_PDF]
def _guardar_pdf_en_memoria(pdf_bytes: bytes, nombre: str = "etiqueta.pdf") -> None:
    st.download_button(
        label=f"⬇️ Descargar {nombre}",
        data=pdf_bytes,
        file_name=nombre,
        mime="application/pdf",
        use_container_width=True,
    )

# [ANCHOR: MOSTRAR_RESULTADOS]
def _mostrar_resultado_descarga(pdf: bytes, titulo: str = "Etiqueta lista"):
    st.success(titulo)
    _guardar_pdf_en_memoria(pdf, "etiqueta.pdf")

# [ANCHOR: DIAGNOSTICAR]
def _diagnosticar_por_ids(order_id: str = "", pack_id: str = "") -> None:
    sid_diag = None
    detalle = None

    if pack_id:
        sid_diag = _meli_get_shipment_id_from_pack(pack_id, seller_id=_meli_get_user_id())
    if not sid_diag and order_id:
        sid_diag = _meli_get_shipment_id_from_order(order_id)

    if sid_diag:
        detalle = _meli_get_shipment(sid_diag)

    if not sid_diag:
        st.error("No se encontró shipment_id a partir de order/pack.")
        return

    st.info(f"Shipment ID encontrado: {sid_diag}")

    if not detalle:
        st.error("No se pudo leer el detalle del shipment.")
        return

    reason = _explicacion_estado_label(detalle)
    if reason:
        st.warning(f"No imprimible aún: {reason}")
    else:
        st.success("El shipment está listo para imprimir (ready_to_ship / ready_to_print o printed).")

# ===================== CABECERA: PRUEBA RÁPIDA (siempre visible) =====================

st.subheader("⚡ Prueba rápida de guía (sin escanear)")  # [ANCHOR: PRUEBA_RAPIDA]

col_q1, col_q2, col_q3 = st.columns([1.1, 1.1, 2.2])
with col_q1:
    quick_order = st.text_input("Order ID (opcional)", value="", key="quick_order")
with col_q2:
    quick_pack = st.text_input("Pack ID (opcional)", value="", key="quick_pack")
with col_q3:
    quick_url = st.text_input("URL adjunta PDF (fallback opcional)", value="", key="quick_url")

col_btn_q1, col_btn_q2 = st.columns([1, 1])

with col_btn_q1:
    if st.button("Generar etiqueta PDF (prueba)", type="primary", use_container_width=True, key="btn_quick_pdf"):
        pdf = descargar_etiqueta_por_order_o_pack(
            order_id=(quick_order.strip() or None),
            pack_id=(quick_pack.strip() or None),
            archivo_adjunto_url=(quick_url.strip() or None),
        )
        if pdf:
            _mostrar_resultado_descarga(pdf, "Etiqueta lista (prueba).")
        else:
            st.error("No se pudo obtener la etiqueta (prueba). Revisa Diagnóstico abajo.")
            _diagnosticar_por_ids(order_id=quick_order.strip(), pack_id=quick_pack.strip())

with col_btn_q2:
    if st.button("Marcar 'Ya tengo el producto' (ready_to_ship)", use_container_width=True, key="btn_quick_ready"):
        # Intento marcar ready_to_ship al último shipment derivado de pack u order
        sid = None
        if quick_pack.strip():
            sid = _meli_get_shipment_id_from_pack(quick_pack.strip(), seller_id=_meli_get_user_id())
        if not sid and quick_order.strip():
            sid = _meli_get_shipment_id_from_order(quick_order.strip())
        if sid:
            ok = _meli_ready_to_ship(sid)
            if ok:
                st.success(f"OK. Shipment {sid} marcado como ready_to_ship.")
            else:
                st.warning("No se pudo marcar ready_to_ship (verifica que sea ME2 y permisos).")
        else:
            st.warning("No hay shipment_id derivable para marcar.")

st.markdown("---")

# ===================== TABS =====================

tab_imp, tab_diag, tab_cfg = st.tabs(["📄 Imprimir guías", "🩺 Diagnóstico", "⚙️ Configuración"])

# --------------------- TAB: IMPRIMIR GUÍAS ---------------------
with tab_imp:
    st.markdown("### Imprimir guías por Orden o Pack")
    col_i1, col_i2, col_i3 = st.columns([1.1, 1.1, 2.2])

    with col_i1:
        order_id = st.text_input("Order ID", value="", key="imp_order")
    with col_i2:
        pack_id = st.text_input("Pack ID", value="", key="imp_pack")
    with col_i3:
        adj_url = st.text_input("URL adjunta PDF (fallback)", value="", key="imp_url")

    col_btn1, col_btn2 = st.columns([1, 1])
    with col_btn1:
        if st.button("Imprimir etiqueta (PDF)", type="primary", use_container_width=True, key="btn_imp_pdf"):
            pdf = descargar_etiqueta_por_order_o_pack(
                order_id=(order_id.strip() or None),
                pack_id=(pack_id.strip() or None),
                archivo_adjunto_url=(adj_url.strip() or None),
            )
            if pdf:
                _mostrar_resultado_descarga(pdf)
            else:
                st.error("No se pudo obtener la etiqueta. Revisa 'Diagnóstico'.")
    with col_btn2:
        if st.button("Marcar 'Ya tengo el producto' (ready_to_ship)", use_container_width=True, key="btn_imp_ready"):
            sid = None
            if pack_id.strip():
                sid = _meli_get_shipment_id_from_pack(pack_id.strip(), seller_id=_meli_get_user_id())
            if not sid and order_id.strip():
                sid = _meli_get_shipment_id_from_order(order_id.strip())
            if sid:
                ok = _meli_ready_to_ship(sid)
                if ok:
                    st.success(f"OK. Shipment {sid} marcado como ready_to_ship.")
                else:
                    st.warning("No se pudo marcar ready_to_ship (verifica que sea ME2 y permisos).")
            else:
                st.warning("No hay shipment_id derivable para marcar.")

    st.markdown("#### Procesar desde archivo (opcional)")
    st.caption("Sube un CSV o XLSX con columnas: order_id, pack_id y/o adjunto_url. Procesa de a una fila seleccionada.")
    up_file = st.file_uploader("Archivo", type=["csv", "xlsx"], key="up_file")

    df = None
    if up_file:
        try:
            if up_file.name.lower().endswith(".csv"):
                df = pd.read_csv(up_file)
            else:
                df = pd.read_excel(up_file)
        except Exception as e:
            st.error(f"No se pudo leer el archivo: {e}")

    if df is not None and not df.empty:
        st.dataframe(df, use_container_width=True)
        st.markdown("")
        idx = st.number_input("Fila a procesar (0-based)", min_value=0, max_value=len(df) - 1, value=0, step=1, key="fila_proc")

        proc = st.button("Procesar fila seleccionada", key="btn_proc_fila", use_container_width=True)
        if proc:
            row = df.iloc[int(idx)]
            r_order = str(row.get("order_id")) if not pd.isna(row.get("order_id")) else ""
            r_pack = str(row.get("pack_id")) if not pd.isna(row.get("pack_id")) else ""
            r_url = str(row.get("adjunto_url")) if not pd.isna(row.get("adjunto_url")) else ""

            pdf = descargar_etiqueta_por_order_o_pack(
                order_id=(r_order.strip() or None),
                pack_id=(r_pack.strip() or None),
                archivo_adjunto_url=(r_url.strip() or None),
            )
            if pdf:
                _mostrar_resultado_descarga(pdf, f"Etiqueta lista (fila {idx}).")
            else:
                st.error(f"No se pudo obtener la etiqueta para la fila {idx}.")
                with st.expander("Ver diagnóstico"):
                    _diagnosticar_por_ids(order_id=r_order.strip(), pack_id=r_pack.strip())

# --------------------- TAB: DIAGNÓSTICO ---------------------
with tab_diag:
    st.markdown("### Diagnóstico de estado de envío")
    st.caption("Consulta estados y causa cuando no es imprimible (buffering, fulfillment, etc.).")

    col_d1, col_d2 = st.columns([1, 1])
    with col_d1:
        d_order = st.text_input("Order ID (opcional)", value="", key="diag_order")
        d_pack = st.text_input("Pack ID (opcional)", value="", key="diag_pack")
    with col_d2:
        d_ship = st.text_input("Shipment ID directo (opcional)", value="", key="diag_ship")

    col_dd = st.columns(3)
    if col_dd[0].button("Diagnosticar por Order/Pack", use_container_width=True, key="btn_diag_ids"):
        _diagnosticar_por_ids(order_id=d_order.strip(), pack_id=d_pack.strip())

    if col_dd[1].button("Diagnosticar por Shipment ID", use_container_width=True, key="btn_diag_ship"):
        sid = d_ship.strip()
        if not sid:
            st.warning("Ingresa un Shipment ID.")
        else:
            detalle = _meli_get_shipment(sid)
            if not detalle:
                st.error("No se pudo leer el shipment.")
            else:
                reason = _explicacion_estado_label(detalle)
                if reason:
                    st.warning(f"No imprimible aún: {reason}")
                else:
                    st.success("El shipment está listo para imprimir (ready_to_ship / ready_to_print o printed).")

    if col_dd[2].button("Intentar imprimir por Shipment ID (PDF)", use_container_width=True, key="btn_diag_ship_pdf"):
        # Reutilizamos el flujo normal pidiendo por order/pack. Si el usuario trae solo shipment,
        # mostramos la causa y sugerimos usar la pestaña Imprimir con order/pack.
        sid = d_ship.strip()
        if not sid:
            st.warning("Ingresa un Shipment ID para diagnóstico.")
        else:
            detalle = _meli_get_shipment(sid)
            if not detalle:
                st.error("No se pudo leer el shipment.")
            else:
                reason = _explicacion_estado_label(detalle)
                if reason:
                    st.warning(f"No imprimible: {reason}")
                else:
                    # Para imprimir por shipment directo necesitaríamos un helper específico.
                    # Recomendación: usar la pestaña Imprimir con order/pack para respetar validaciones y lookup.
                    st.info("Usa la pestaña 'Imprimir guías' con Order o Pack para descargar el PDF.")

# --------------------- TAB: CONFIGURACIÓN ---------------------
with tab_cfg:
    st.markdown("### Configuración y estado de credenciales")
    st.caption("El módulo refresca el access_token automáticamente usando el refresh_token y guarda meli_tokens.json.")

    st.write("**Carga de credenciales**")
    st.write("1) `st.secrets['meli']` con app_id, client_secret, access_token, refresh_token")
    st.write("2) Variables de entorno: MELI_APP_ID, MELI_CLIENT_SECRET, MELI_ACCESS_TOKEN, MELI_REFRESH_TOKEN")
    st.write("3) Archivo `meli_tokens.json` (se actualiza tras refresh)")

    PATH_TOKENS = "meli_tokens.json"
    if os.path.exists(PATH_TOKENS):
        try:
            with open(PATH_TOKENS, "r", encoding="utf-8") as f:
                data = json.load(f)
            with st.expander("Ver meli_tokens.json (enmascarado)"):
                mask = dict(data)
                if mask.get("access_token"):
                    mask["access_token"] = mask["access_token"][:12] + "...(oculto)"
                if mask.get("refresh_token"):
                    mask["refresh_token"] = mask["refresh_token"][:12] + "...(oculto)"
                st.json(mask)
        except Exception as e:
            st.error(f"No se pudo leer meli_tokens.json: {e}")
    else:
        st.info("No existe meli_tokens.json todavía. Se creará al primer refresh automático.")

    st.markdown("---")
    st.write("**Checklist rápido**")
    st.write("- ¿El usuario es administrador (no operador)?")
    st.write("- ¿El `redirect_uri` en la app coincide exactamente con el configurado?")
    st.write("- ¿No es un envío Fulfillment?")
    st.write("- ¿Status/substatus: ready_to_ship / ready_to_print o printed?")
    st.write("- Si substatus es `buffered`, espera hasta la fecha indicada en `lead_time.buffering.date`.")

