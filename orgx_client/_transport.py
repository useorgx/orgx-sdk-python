"""HTTP transport that keeps credentials on the configured origin."""

from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, build_opener


def _origin(url: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(url)
    return (parsed.scheme.lower(), (parsed.hostname or "").lower(),
            parsed.port or {"https": 443, "http": 80}.get(parsed.scheme.lower()))


class SameOriginRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urlsplit(newurl)
        if target.username or target.password or _origin(req.full_url) != _origin(newurl):
            raise HTTPError(req.full_url, code, "Cross-origin redirect refused", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


urlopen = build_opener(SameOriginRedirectHandler()).open
