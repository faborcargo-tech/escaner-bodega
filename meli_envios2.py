# meli_envios2.py
# Helpers para ME2 (packs, labels PDF, ready_to_ship) y manejo de token manual opcional.

import os
import json
import requests
from typing import Optional, Dict, Any, List

MELI_API_BASE = "https://api.mercadolibre.com"
TIMEOUT = 25

LABEL_ALLOWED_TYPES = {"drop_off", "xd_drop_off", "cross_docking", "self_service"}

TOKEN_FILE = "meli_tokens.json"  # opcional (no usado por defecto en la app)

# ===================== Token Store (opcional) =====================

class MeliTokenStore:
    """
    Fuente de tokens (opcional):
    1) Variables de entorno: MELI_APP_ID, MELI_CLIENT_SECRET, MELI_ACCESS_TOKEN, MELI_REFRESH_TOKEN
    2) st.secrets["meli"] si existe
    3) Archivo local TOKEN_FILE (lectura/escritura)
    """
    def __init__(self, file_path: str = TOKEN_FILE):
        self.file_path = file_path
        self.app_id: Optional[str] = None
        self.client_secret: Optional[str] = None
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self._load_all_sources()

    def _load_all_sources(self) -> None:
        # ENV
        self.app_id = os.getenv("MELI_APP_ID") or None
        self.client_secret = os.getenv("MELI_CLIENT_SECRET") or None
        self.access_token = os.getenv("MELI_ACCESS_TOKEN") or None
        self.refresh_token = os.getenv("MELI_REFRESH_TOKEN") or None

        # Streamlit secrets (opcional)
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

        # Archivo local (opcional)
        if not self.access_token or not self.refresh_token or not self.app_id or not self.client_secret:
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.app_id = data.get("app_id", self.app_id)
                self.client_secret = data.get("client_secret", self.client_secret)
                self.access_token = data.get("access_token", self.access_token)
                self.refresh_token = data.get("refresh_token", self.refresh_token)
            except Exception:
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
        """Refresca el access_token usando el refresh_token. Persiste el nuevo refresh_token si viene."""
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

_token_store = MeliTokenStore()

def _meli_get_access_token() -> Optional[str]:
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
        if _token_store.refresh():
            base_headers = _meli_headers(_token_store.access_token or "")
            if headers:
                base_headers.update(headers)
            r = requests.request(method, url, headers=base_headers, params=params, data=data, json=json_body, timeout=timeout)

    return r

# ===================== Packs / Orders / Shipments =====================

def _meli_get_user_id() -> Optional[int]:
    try:
        r = _meli_request("GET", "/users/me")
        if r.status_code == 200:
            return (r.json() or {}).get("id")
    except Exception:
        pass
    return None

def _meli_get_shipment_id_from_pack(pack_id: str, seller_id: Optional[int] = None) -> Optional[str]:
    try:
        r = _meli_request("GET", f"/packs/{pack_id}")
        if r.status_code == 200:
            d = r.json() or {}
            sh = (d.get("shipment") or {}).get("id")
            if sh:
                return str(sh)
    except Exception:
        pass

    # Fallback /shipments/search
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
    try:
        r = _meli_request("GET", f"/orders/{order_id}")
        if r.status_code == 200:
            data = r.json() or {}
            shipping = data.get("shipping") or {}
            if shipping.get("id"):
                return str(shipping["id"])
            pack_id = data.get("pack_id") or (data.get("pack") or {}).get("id")
            if pack_id:
                seller_id = _meli_get_user_id()
                return _meli_get_shipment_id_from_pack(str(pack_id), seller_id=seller_id)
    except Exception:
        pass

    # Fallback search
    try:
        seller_id = _meli_get_user_id()
        params = {"order": order_id}
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
    logistic = sh_json.get("logistic") or {}
    mode = logistic.get("mode")
    ltype = logistic.get("type") or logistic.get("logistic_type")

    if mode != "me2":
        return "El envío no es ME2 (mode != 'me2')."
    if ltype is None:
        return "No se encontró logistic.type."
    if ltype == "fulfillment":
        return "Fulfillment: la etiqueta la gestiona Mercado Libre."
    if ltype not in LABEL_ALLOWED_TYPES:
        return f"Tipo de logística no permitido para etiqueta: {ltype}."

    status = sh_json.get("status")
    substatus = sh_json.get("substatus")

    if status == "pending" and substatus == "buffered":
        lead_time = sh_json.get("lead_time") or {}
        buffering = lead_time.get("buffering") or {}
        dt = buffering.get("date")
        if dt:
            return f"Etiqueta no disponible aún (buffering). Disponible desde: {dt}."
        return "Etiqueta no disponible aún (buffering)."

    if status != "ready_to_ship":
        return f"Estado no permitido: status={status}."
    if substatus not in {"ready_to_print", "printed"}:
        return f"Subestado no permitido: substatus={substatus}."

    return None

def _meli_download_label_pdf(shipment_id: str) -> Optional[bytes]:
    sh = _meli_get_shipment(shipment_id)
    if not sh:
        return None

    reason = _explicacion_estado_label(sh)
    if reason is not None:
        return None

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
            if content[:4] == b"%PDF":
                return content
    except Exception:
        pass
    return None

# ===================== Alto nivel =====================

def descargar_etiqueta_por_order_o_pack(order_id: Optional[str] = None,
                                        pack_id: Optional[str] = None,
                                        archivo_adjunto_url: Optional[str] = None) -> Optional[bytes]:
    if pack_id:
        sid = _meli_get_shipment_id_from_pack(str(pack_id), seller_id=_meli_get_user_id())
        if sid:
            pdf = _meli_download_label_pdf(sid)
            if pdf:
                return pdf

    if order_id:
        sid = _meli_get_shipment_id_from_order(str(order_id))
        if sid:
            pdf = _meli_download_label_pdf(sid)
            if pdf:
                return pdf

    if archivo_adjunto_url:
        try:
            h = requests.head(archivo_adjunto_url, allow_redirects=True, timeout=10)
            if h.status_code < 400:
                b = requests.get(archivo_adjunto_url, timeout=10).content
                if b[:4] == b"%PDF":
                    return b
        except Exception:
            pass
    return None

# ===================== Utilidad para guardar token manual =====================

def save_manual_token(token: str):
    """
    Guarda un Access Token manual en st.session_state para que la app lo use.
    (No se persiste en DB; solo sesión.)
    """
    try:
        import streamlit as st  # type: ignore
        st.session_state.meli_manual_token = (token or "").strip()
    except Exception:
        pass

# ===================== CLI (opcional) =====================

if __name__ == "__main__":
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
