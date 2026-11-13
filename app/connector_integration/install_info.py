"""What the install screen needs to show the client the right container settings.

The download site, the image, the release version and any extra environment variables are operator
settings (CONNECTOR_DOWNLOAD_URL, CONNECTOR_IMAGE, CONNECTOR_RELEASE_VERSION, CONNECTOR_INSTALL_ENV).
The frontend builds copyable configuration from them, so they are validated here: only characters that
cannot change the meaning of that text are allowed, and an environment entry may only set a VIZAI_*
variable.
"""

from __future__ import annotations

import re

_URL = re.compile(r"^https?://[A-Za-z0-9._~:/-]+$")
_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$")
_IMAGE = re.compile(r"^[a-z0-9][a-z0-9._-]*(:[0-9]+)?(/[a-z0-9][a-z0-9._-]*)*$")
_ENTRY = re.compile(r"^(VIZAI_[A-Z0-9_]+)=([A-Za-z0-9_./:@+,=~-]+)$")


class InvalidInstallSettings(ValueError):
    """An operator setting would be unsafe to put in a command."""


def build_install_info(download_url: str, version: str, env_text: str | None, image: str = "vizai/connector") -> dict:
    url = (download_url or "").strip().rstrip("/")
    if not _URL.match(url):
        raise InvalidInstallSettings("CONNECTOR_DOWNLOAD_URL must be an http(s) URL without spaces or quotes")
    if not _VERSION.match((version or "").strip()):
        raise InvalidInstallSettings("CONNECTOR_RELEASE_VERSION must look like 0.6.0")
    image = (image or "").strip()
    if not _IMAGE.match(image):
        raise InvalidInstallSettings("CONNECTOR_IMAGE must be an image name without a tag, e.g. registry.example.com/vizai/connector")
    env: list[str] = []
    for entry in (env_text or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        if not _ENTRY.match(entry):
            raise InvalidInstallSettings("CONNECTOR_INSTALL_ENV entries must look like VIZAI_NAME=value (safe characters only)")
        env.append(entry)
    return {"downloadUrl": url, "version": version.strip(), "image": f"{image}:{version.strip()}", "env": env}
