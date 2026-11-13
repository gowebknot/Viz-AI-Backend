"""The install screen builds shell commands from these settings, so unsafe values must be refused."""

import pytest

from app.connector_integration.install_info import InvalidInstallSettings, build_install_info


def test_normal_settings():
    info = build_install_info("https://downloads.vizai.ai/connector/", "0.6.0", None)
    assert info == {"downloadUrl": "https://downloads.vizai.ai/connector", "version": "0.6.0", "image": "vizai/connector:0.6.0", "env": []}


def test_local_testing_settings():
    info = build_install_info("http://127.0.0.1:8088", "0.6.0", "VIZAI_TLS_CA_FILE=/home/me/ca.pem, VIZAI_TLS=true")
    assert info["env"] == ["VIZAI_TLS_CA_FILE=/home/me/ca.pem", "VIZAI_TLS=true"]


@pytest.mark.parametrize("url", ["", "ftp://x", "https://a b", "https://x/'; rm -rf /", "https://x/$(id)", "https://x/`id`", "javascript:alert(1)"])
def test_unsafe_urls_are_refused(url):
    with pytest.raises(InvalidInstallSettings):
        build_install_info(url, "0.6.0", None)


@pytest.mark.parametrize("version", ["", "latest", "0.6", "0.6.0; id", "$(id)"])
def test_unsafe_versions_are_refused(version):
    with pytest.raises(InvalidInstallSettings):
        build_install_info("https://x.example", version, None)


@pytest.mark.parametrize("env", ["PATH=/tmp", "VIZAI_X=a b", "VIZAI_X=$(id)", "VIZAI_X=`id`", "VIZAI_X=a;b", "VIZAI_X=", "vizai_x=1", "VIZAI_X='q'"])
def test_unsafe_environment_entries_are_refused(env):
    with pytest.raises(InvalidInstallSettings):
        build_install_info("https://x.example", "0.6.0", env)


def test_the_image_can_come_from_a_private_registry():
    info = build_install_info("https://x.example", "0.7.0", None, "registry.example.com:5000/vizai/connector")
    assert info["image"] == "registry.example.com:5000/vizai/connector:0.7.0"


@pytest.mark.parametrize("image", ["", "vizai/connector:latest", "Vizai/Connector", "a b", "$(id)", "a;b", "a/", "https://x/y"])
def test_unsafe_images_are_refused(image):
    with pytest.raises(InvalidInstallSettings):
        build_install_info("https://x.example", "0.7.0", None, image)
