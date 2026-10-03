from fastapi.testclient import TestClient

from app import main
from app.main import app

client = TestClient(app)


def test_unknown_alias():
    r = client.get("/databases/nope/version")
    assert r.status_code == 404


def test_invalid_manifest_chain():
    payload = {
        "expected_version": 0,
        "scripts": [
            {"version": 2, "description": "d", "sql": "CREATE TABLE x(a);"}
        ],
    }
    r = client.post("/databases/demo/migrate", json=payload)
    assert r.status_code == 422


def test_blank_script_rejected():
    payload = {
        "expected_version": 0,
        "scripts": [{"version": 1, "description": "d", "sql": "   "}],
    }
    r = client.post("/databases/demo/migrate", json=payload)
    assert r.status_code == 422


def test_body_size_limit():
    huge = "x" * (3 * 1024 * 1024)
    r = client.post(
        "/databases/demo/migrate",
        content=huge,
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 413


def test_checkpoint_http_flow():
    created = client.post("/databases/demo/checkpoints")
    assert created.status_code == 201
    meta = created.json()
    assert meta["alias"] == "demo"
    assert meta["id"].startswith("cp-")
    try:
        listed = client.get("/databases/demo/checkpoints")
        assert listed.status_code == 200
        ids = [c["id"] for c in listed.json()["checkpoints"]]
        assert meta["id"] in ids

        unknown = client.post(
            "/databases/demo/restore",
            json={"checkpoint_id": "cp-20000101T000000Z-nope0000", "expected_version": 0},
        )
        assert unknown.status_code == 404
        assert unknown.json()["code"] == "checkpoint_not_found"

        conflict = client.post(
            "/databases/demo/restore",
            json={"checkpoint_id": meta["id"], "expected_version": 999},
        )
        assert conflict.status_code == 409

        wrong_alias = client.post(
            "/databases/billing/restore",
            json={"checkpoint_id": meta["id"], "expected_version": 0},
        )
        assert wrong_alias.status_code == 409
        assert wrong_alias.json()["code"] == "checkpoint_alias_mismatch"

        version = client.get("/databases/demo/version").json()["current_version"]
        ok = client.post(
            "/databases/demo/restore",
            json={"checkpoint_id": meta["id"], "expected_version": version},
        )
        assert ok.status_code == 200
        assert ok.json()["restored"]["id"] == meta["id"]
    finally:
        (main.checkpoints.root / f"{meta['id']}.json").unlink(missing_ok=True)
        (main.checkpoints.root / f"{meta['id']}.db").unlink(missing_ok=True)


def test_forbidden_sql_returns_business_error_not_500(tmp_path):
    db = tmp_path / "api.db"
    db.touch()
    main.settings.aliases["pytest-tmp"] = db
    try:
        payload = {
            "expected_version": 0,
            "scripts": [{"version": 1, "description": "d", "sql": "VACUUM;"}],
        }
        r = client.post("/databases/pytest-tmp/migrate", json=payload)
    finally:
        main.settings.aliases.pop("pytest-tmp", None)
    assert r.status_code == 422
    body = r.json()
    assert body["code"] == "migration_failed"
    assert body["failed_version"] == 1
