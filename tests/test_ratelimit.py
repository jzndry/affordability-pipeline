from starlette.requests import Request

from app.ratelimit import client_ip


def _request(headers: dict[str, str], client: tuple[str, int] | None = ("9.9.9.9", 0)) -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope = {"type": "http", "headers": raw, "client": client}
    return Request(scope)


def test_client_ip_prefers_cf_connecting_ip():
    req = _request({"cf-connecting-ip": "3.3.3.3", "x-forwarded-for": "1.2.3.4"})
    assert client_ip(req) == "3.3.3.3"


def test_client_ip_uses_first_x_forwarded_for_entry():
    req = _request({"x-forwarded-for": "1.2.3.4, 5.6.7.8"})
    assert client_ip(req) == "1.2.3.4"


def test_client_ip_falls_back_to_remote_address():
    req = _request({})
    assert client_ip(req) == "9.9.9.9"
