import json
import subprocess
import time
from uuid import uuid4

import httpx
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url

from alembic import command
from backend.tests.infrastructure import COMPOSE, PROJECT, ROOT
from backend.tests.test_payment_value_migration import INSERT_PAYMENT, query

pytestmark = [pytest.mark.integration, pytest.mark.runtime]


def compose(*args, check=True):
    return subprocess.run(
        [*COMPOSE, "--profile", "runtime", *args],
        capture_output=True,
        text=True,
        check=check,
        timeout=90,
    )


def inspect_service(service):
    identifier = compose("ps", "-a", "-q", service).stdout.strip()
    if not identifier:
        pytest.fail("Start compose.test.yaml --profile runtime before runtime tests")
    result = subprocess.run(
        ["docker", "inspect", identifier],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    info = json.loads(result.stdout)[0]
    assert info["Config"]["Labels"]["com.docker.compose.project"] == PROJECT
    return info


def wait_health(timeout=30):
    deadline = time.monotonic() + timeout
    with httpx.Client(trust_env=False, timeout=2) as client:
        while time.monotonic() < deadline:
            try:
                response = client.get("http://127.0.0.1:58000/healthz")
                if response.status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.1)
    pytest.fail("Test API did not become healthy")


def test_runtime_migrator_non_root_and_health(isolated_postgres, isolated_rabbitmq_url):
    migration = inspect_service("migrator")
    assert migration["State"]["ExitCode"] == 0
    assert migration["Config"]["User"] == "10001:10001"
    for service in ("api", "payment-worker", "publisher"):
        info = inspect_service(service)
        assert info["State"]["Running"]
        assert info["Config"]["User"] == "10001:10001"
        assert not info["Mounts"]
        assert compose("exec", "-T", service, "id", "-u").stdout.strip() == "10001"
    wait_health()
    with httpx.Client(trust_env=False, base_url="http://127.0.0.1:58000") as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/health").status_code == 401
    assert inspect_service("api")["State"]["Health"]["Status"] == "healthy"


@pytest.mark.parametrize("existing_schema", [False, True])
def test_container_migrator_clean_and_existing_schema(
    test_database_url, existing_schema
):
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["database_url"] = test_database_url
    if existing_schema:
        command.upgrade(config, "ff31925d1df3")
        query(test_database_url, INSERT_PAYMENT, uuid4(), 123, "USD", "retained")
    internal_url = (
        make_url(test_database_url)
        .set(host="db", port=5432)
        .render_as_string(hide_password=False)
    )
    for _ in range(2):
        compose(
            "run", "--rm", "--no-deps", "-e", f"DATABASE_URL={internal_url}", "migrator"
        )
    assert query(test_database_url, "SELECT version_num FROM alembic_version") == [
        {"version_num": ScriptDirectory.from_config(config).get_current_head()}
    ]
    assert query(test_database_url, "SELECT count(*) AS n FROM payments") == [
        {"n": int(existing_schema)}
    ]


def test_api_restart_policy_after_process_exit(isolated_postgres):
    before = inspect_service("api")
    assert before["HostConfig"]["RestartPolicy"]["Name"] == "unless-stopped"
    compose(
        "exec",
        "-T",
        "api",
        "python",
        "-c",
        "import os, signal; os.kill(1, signal.SIGTERM)",
        check=False,
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        after = inspect_service("api")
        if after["RestartCount"] > before["RestartCount"]:
            break
        time.sleep(0.1)
    else:
        pytest.fail("API was not automatically restarted")
    wait_health()


def test_failed_migrator_blocks_dependent_container(isolated_postgres, tmp_path):
    project = f"payment-service-async-tests-failure-{uuid4().hex[:8]}"
    path = tmp_path / "compose.json"
    path.write_text(
        json.dumps(
            {
                "services": {
                    "migrator": {
                        "image": "payment-service:test",
                        "command": ["python", "-c", "raise SystemExit(7)"],
                    },
                    "blocked": {
                        "image": "payment-service:test",
                        "command": ["python", "-c", "import time; time.sleep(60)"],
                        "depends_on": {
                            "migrator": {"condition": "service_completed_successfully"}
                        },
                    },
                }
            }
        )
    )
    args = ["docker", "compose", "-f", str(path), "-p", project]
    try:
        result = subprocess.run(
            [*args, "up", "-d"], capture_output=True, text=True, check=False, timeout=30
        )
        assert result.returncode != 0
        result = subprocess.run(
            [*args, "ps", "--status", "running", "-q", "blocked"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        assert not result.stdout.strip()
    finally:
        # Only this test's newly created, volume-free containers/network.
        subprocess.run([*args, "down"], capture_output=True, check=True, timeout=30)
