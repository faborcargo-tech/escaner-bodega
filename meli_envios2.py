# meli_envios2.py
# Helpers completos y listos para ME2 (packs, buffering, labels PDF, refresh token).
# Solo requiere 'requests'. Streamlit es opcional.
# Python 3.8+

import os
import json
import requests
from typing import Optional, Dict, Any, List

# ===================== Config =====================

MELI_API_BASE = "https://api.mercadolibre.com"
TIMEOUT = 25

# Tipos de logística admitidos para imprimir etiqueta en ME2 (no fulfillment)
LABEL_ALLOWED_TYPES = {"drop_off", "xd_drop_off", "cross_docking", "self_service"}

# Archivo local donde se guardarán los tokens al refrescar
TOKEN_FILE = "meli_tokens.json"


# ===================== Token Store =====================

class MeliTokenStore:
    """
    Carga tokens desde:
      1) Variables de entorno: MELI_APP_ID, MELI_CLIENT_SECRET, MELI_ACCESS_TOKEN, MELI_REFRESH_TOKEN
      2) st.secrets["meli"] si Streamlit está disponible
      3) Archivo local TOKEN_FILE (lectura y escritura)

    Hace refresh automático y persiste el nuevo refresh_token.
    """

    def __init__(self, file_path: str = TOKEN_FILE):
        self.file_path = file_path
        self.app_id: Optional[str] = None
        self.client_secret: Optional[str] = None
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self._load_all_sources()

    def _load_all_sources(self) -> None:
        # 1) ENV
        self.app_id = os.getenv("MELI_APP_ID") or None
        self.client_secret = os.getenv("MELI_CLIENT_SECRET") or None
        self.access_token = os.getenv("MELI_ACCESS_TOKEN") or None
        self.refresh_token = os.getenv("MELI_REFRESH_TOKEN") or None

        # 2) Streamlit secrets (opcional)
        try:
            import streamlit as st  # type: ignore
            if "meli" in st.secrets:
                sec = st.secrets["meli"]
                self.app_id = sec.get("app_id", self.app_id)
                self.client_secret = sec.get("client_secret", self.client_secret)
                self.access_token = sec.get("access_token", self.access_token)
                self.refresh_token = sec.get("refresh_token", self.refresh_token)
        except Exception:
            pass

        # 3) Archivo local
        if not self.access_token or not self.refresh_token or not self.app_id or not self.client_secret:
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.app_id = data.get("app_id", self.app_id)
                self.client_secret = data.get("client_secret", self.client_secret)
                self.access_token = data.get("access_token", self.access_token)
                self.refresh_token = data.get("refresh_token", self.refresh_token)
            except Exception:
                # No hay archivo o no se pudo leer. Seguimos con lo que haya.
                pass

    def _save_file(self) -> bool:
        try:
            data = {
                "app_id": self.app_id,
                "client_secret": self.client_secret,
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
            }
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False

    def can_refresh(self) -> bool:
        return bool(self.app_id and self.client_secret and self.refresh_token)

    def refresh(self) -> bool:
        """Refresca el access_token usando el refresh_token. Persiste el nuevo refresh_token."""
        if not self.can_refresh():
            return False
        try:
            r = requests.post(
                f"{MELI_API_BASE}/oauth/token",
                headers={
                    "accept": "application/json",
                    "content-type": "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.app_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                },
                timeout=TIMEOUT,
            )
            if r.status_code == 200:
                payload = r.json() or {}
                self.access_token = payload.get("access_token") or self.access_token
                new_refresh = payload.get("refresh_token")
                if new_refresh:
                    self.refresh_token = new_refresh
                self._save_file()
                return True
        except Exception:
            pass
        return False


# Instancia global de tokens
_token_store = MeliTokenStore()


def _meli_get_access_token() -> Optional[str]:
    """Para compatibilidad con tu app actual."""
    return _token_store.access_token


# ===================== HTTP helpers =====================

def _meli_headers(access_token: str, accept: Optional[str] = None, new_format: bool = False) -> Dict[str, str]:
    h = {"Authorization": f"Bearer {access_token}"}
    if accept:
        h["Accept"] = accept
    if new_format:
        h["x-format-new"] = "true"
    return h


def _full_url(path_or_url: str) -> str:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    if not path_or_url.startswith("/"):
        path_or_url = "/" + path_or_url
    return f"{MELI_API_BASE}{path_or_url}"


def _meli_request(method: str,
                  path_or_url: str,
                  *,
                  params: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None,
                  data: Optional[Any] = None,
                  json_body: Optional[Any] = None,
                  timeout: int = TIMEOUT) -> requests.Response:
    """
    Llama a la API de Meli y, si hay 401/403, intenta un refresh_token y reintenta una vez.
    """
    url = _full_url(path_or_url)
    base_headers = _meli_headers(_token_store.access_token or "")
    if headers:
        base_headers.update(headers)

    r = requests.request(method, url, headers=base_headers, params=params, data=data, json=json_body, timeout=timeout)

    if r.status_code in (401, 403):
        # intenta refresh una sola vez
        if _token_store.refresh():
            base_headers = _meli_headers(_token_store.access_token or "")
            if headers:
                base_headers.update(headers)
            r = requests.request(method, url, headers=base_headers, params=params, data=data, json=json_body, timeout=timeout)

    return r


# ===================== Utilidades generales =====================

def url_disponible(url: str) -> bool:
    try:
        h = requests.head(url, allow_redirects=True, timeout=10)
        return h.status_code < 400
    except Exception:
        return False


def _meli_get_user_id() -> Optional[int]:
    try:
        r = _meli_request("GET", "/users/me")
        if r.status_code == 200:
            return (r.json() or {}).get("id")
    except Exception:
        pass
    return None


# ===================== Packs / Orders / Shipments =====================

def _meli_get_shipment_id_from_pack(pack_id: str, seller_id: Optional[int] = None) -> Optional[str]:
    # 1) /packs/{id}
    try:
        r = _meli_request("GET", f"/packs/{pack_id}")
        if r.status_code == 200:
            d = r.json() or {}
            sh = (d.get("shipment") or {}).get("id")
            if sh:
                return str(sh)
    except Exception:
        pass

    # 2) Fallback: /shipments/search?pack=... (&seller= opcional)
    try:
        params: Dict[str, Any] = {"pack": pack_id}
        if seller_id:
            params["seller"] = seller_id
        r2 = _meli_request("GET", "/shipments/search", params=params, headers={"x-format-new": "true"})
        if r2.status_code == 200:
            results = (r2.json() or {}).get("results") or []
            if results:
                sid = results[0].get("id") or results[0].get("shipment_id")
                if sid:
                    return str(sid)
    except Exception:
        pass

    return None


def _meli_get_shipment_id_from_order(order_id: str) -> Optional[str]:
    # 1) /orders/{id}
    try:
        r = _meli_request("GET", f"/orders/{order_id}")
        if r.status_code == 200:
            data = r.json() or {}
            # Si trae shipping.id, listo
            shipping = data.get("shipping") or {}
            if shipping.get("id"):
                return str(shipping["id"])
            # Si no, usar pack_id -> /packs
            pack_id = data.get("pack_id") or (data.get("pack") or {}).get("id")
            if pack_id:
                seller_id = _meli_get_user_id()
                return _meli_get_shipment_id_from_pack(str(pack_id), seller_id=seller_id)
    except Exception:
        pass

    # 2) Fallback: /shipments/search?order=...
    try:
        seller_id = _meli_get_user_id()
        candidates: List[Dict[str, Any]] = []
        # con seller
        params = {"order": order_id}
        if seller_id:
            params["seller"] = seller_id
        r2 = _meli_request("GET", "/shipments/search", params=params, headers={"x-format-new": "true"})
        if r2.status_code == 200:
            candidates.extend((r2.json() or {}).get("results") or [])
        # sin seller si vacío
        if not candidates:
            r3 = _meli_request("GET", "/shipments/search", params={"order": order_id}, headers={"x-format-new": "true"})
            if r3.status_code == 200:
                candidates.extend((r3.json() or {}).get("results") or [])
        if candidates:
            sid = candidates[0].get("id") or candidates[0].get("shipment_id")
            if sid:
                return str(sid)
    except Exception:
        pass

    return None


def _meli_get_shipment(shipment_id: str) -> Optional[Dict[str, Any]]:
    try:
        r = _meli_request("GET", f"/shipments/{shipment_id}", headers={"x-format-new": "true"})
        if r.status_code == 200:
            return r.json() or {}
    except Exception:
        pass
    return None


def _meli_ready_to_ship(shipment_id: str) -> bool:
    try:
        r = _meli_request("POST", f"/shipments/{shipment_id}/process/ready_to_ship")
        return r.status_code == 200
    except Exception:
        return False


# ===================== Validaciones ME2 y Labels =====================

def _explicacion_estado_label(sh_json: Dict[str, Any]) -> Optional[str]:
    """
    Devuelve None si está todo ok para imprimir.
    Si no, devuelve un string con la razón (para logs/diagnóstico).
    """
    # Modo/logística
    logistic = sh_json.get("logistic") or {}
    mode = logistic.get("mode")
    ltype = logistic.get("type") or logistic.get("logistic_type")  # compat

    if mode != "me2":
        return "El envío no es ME2 (mode != 'me2')."

    if ltype is None:
        return "No se encontró logistic.type."
    if ltype == "fulfillment":
        return "Fulfillment: la etiqueta de envío la gestiona Mercado Libre."
    if ltype not in LABEL_ALLOWED_TYPES:
        return f"Tipo de logística no permitido para etiqueta: {ltype}."

    # Estado/subestado
    status = sh_json.get("status")
    substatus = sh_json.get("substatus")

    if status == "pending" and substatus == "buffered":
        # Chequear lead_time.buffering.date
        lead_time = sh_json.get("lead_time") or {}
        buffering = lead_time.get("buffering") or {}
        dt = buffering.get("date")
        if dt:
            return f"Etiqueta no disponible aún (buffering). Disponible desde: {dt}."
        return "Etiqueta no disponible aún (buffering)."

    if status != "ready_to_ship":
        return f"Estado no permitido para imprimir etiqueta: status={status}."

    if substatus not in {"ready_to_print", "printed"}:
        return f"Subestado no permitido para imprimir etiqueta: substatus={substatus}."

    # OK para imprimir
    return None


def _meli_download_label_pdf(shipment_id: str) -> Optional[bytes]:
    """
    Valida condiciones ME2 y descarga la etiqueta PDF.
    Devuelve bytes del PDF o None si no pudo (usa _explicacion_estado_label para diagnosticar).
    """
    sh = _meli_get_shipment(shipment_id)
    if not sh:
        return None

    reason = _explicacion_estado_label(sh)
    if reason is not None:
        # Puedes registrar 'reason' en logs si querés
        return None

    # Descarga PDF
    try:
        params = {"shipment_ids": shipment_id, "response_type": "pdf"}
        r = _meli_request(
            "GET",
            "/shipment_labels",
            params=params,
            headers={"Accept": "application/pdf"},
        )
        if r.status_code == 200:
            content = r.content or b""
            # Chequeo mínimo de PDF
            if content[:4] == b"%PDF":
                return content
    except Exception:
        pass
    return None


# ===================== Alto nivel =====================

def descargar_etiqueta_por_order_o_pack(order_id: Optional[str] = None,
                                        pack_id: Optional[str] = None,
                                        archivo_adjunto_url: Optional[str] = None) -> Optional[bytes]:
    """
    Flujo completo:
      1) Si pack_id -> buscar shipment y pedir etiqueta
      2) Si no, order_id -> buscar shipment y pedir etiqueta
      3) Fallback: descargar desde archivo_adjunto_url (si es PDF)
    """
    # 1) pack primero (modelo 2024/2025)
    if pack_id:
        sid = _meli_get_shipment_id_from_pack(str(pack_id), seller_id=_meli_get_user_id())
        if sid:
            pdf = _meli_download_label_pdf(sid)
            if pdf:
                return pdf

    # 2) orden como respaldo
    if order_id:
        sid = _meli_get_shipment_id_from_order(str(order_id))
        if sid:
            pdf = _meli_download_label_pdf(sid)
            if pdf:
                return pdf

    # 3) respaldo por URL pública
    if archivo_adjunto_url and url_disponible(archivo_adjunto_url):
        try:
            b = requests.get(archivo_adjunto_url, timeout=10).content
            if b[:4] == b"%PDF":
                return b
        except Exception:
            pass

    return None


# ===================== Panel de prueba (opcional Streamlit) =====================

def render_panel_prueba():
    """
    Si estás usando Streamlit, puedes montar un panel rápido con:
        from meli_envios2 import render_panel_prueba
        render_panel_prueba()
    """
    try:
        import streamlit as st  # type: ignore
    except Exception:
        return

    st.header("Prueba rápida de etiqueta ME2")
    col1, col2 = st.columns(2)
    with col1:
        order_id = st.text_input("Order ID (opcional)", value="")
    with col2:
        pack_id = st.text_input("Pack ID (opcional)", value="")

    archivo_url = st.text_input("URL adjunta (fallback opcional)", value="")

    if st.button("Descargar etiqueta PDF", use_container_width=True):
        pdf = descargar_etiqueta_por_order_o_pack(order_id.strip() or None,
                                                  pack_id.strip() or None,
                                                  archivo_url.strip() or None)
        if pdf:
            st.success("Etiqueta lista.")
            st.download_button("Bajar PDF", data=pdf, file_name="etiqueta.pdf", mime="application/pdf", use_container_width=True)
        else:
            # Intentar diagnosticar si hay buffering o estado inválido
            sid_diag = None
            if pack_id.strip():
                sid_diag = _meli_get_shipment_id_from_pack(pack_id.strip(), seller_id=_meli_get_user_id())
            if not sid_diag and order_id.strip():
                sid_diag = _meli_get_shipment_id_from_order(order_id.strip())

            if sid_diag:
                sh = _meli_get_shipment(sid_diag)
                if sh:
                    reason = _explicacion_estado_label(sh)
                    if reason:
                        st.warning(f"No se puede imprimir la etiqueta aún: {reason}")
                    else:
                        st.error("No se pudo descargar el PDF, pero el estado parece válido. Reintenta.")
                else:
                    st.error("No se pudo obtener el detalle del shipment para diagnosticar.")
            else:
                st.error("No se encontró shipment_id a partir de order/pack y no se pudo usar el adjunto.")

    st.markdown("---")
    sid_manual = st.text_input("Shipment ID directo (diagnóstico)", value="")
    if sid_manual and st.button("Diagnosticar estado del shipment", use_container_width=True):
        sh = _meli_get_shipment(sid_manual.strip())
        if not sh:
            st.error("No se pudo leer el shipment.")
        else:
            reason = _explicacion_estado_label(sh)
            if reason:
                st.info(f"Diagnóstico: {reason}")
            else:
                st.success("El shipment está listo para imprimir (ready_to_ship / ready_to_print o printed).")

    st.markdown("---")
    if st.button("Marcar 'Ya tengo el producto' (ready_to_ship) para el último shipment encontrado", use_container_width=True):
        # Busca primero por pack y luego por order
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
            st.warning("No hay shipment_id para marcar.")


# ===================== Uso CLI simple (opcional) =====================

if __name__ == "__main__":
    # Pequeña prueba por consola: intenta descargar una etiqueta por ORDER_ID o PACK_ID pasados por env.
    order = os.getenv("TEST_ORDER_ID")
    pack = os.getenv("TEST_PACK_ID")
    if not order and not pack:
        print("Define TEST_ORDER_ID o TEST_PACK_ID en el entorno para una prueba rápida.")
    else:
        pdf_bytes = descargar_etiqueta_por_order_o_pack(order_id=order, pack_id=pack)
        if pdf_bytes:
            out = "etiqueta_test.pdf"
            with open(out, "wb") as f:
                f.write(pdf_bytes)
            print(f"Etiqueta guardada en {out}")
        else:
            print("No se pudo obtener la etiqueta.")
