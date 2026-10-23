"""Steps used by the example workflow. Importable so the demo runs for real."""
import time


def checkout(ctx):
    return {"ref": ctx.get("ref", "HEAD"), "files": 214}


def install(ctx):
    return {"packages": 96, "cache": "warm"}


def test_unit(ctx):
    return {"passed": 118, "failed": 0}


def test_integration(ctx):
    return {"passed": 24, "failed": 0}


def build(ctx):
    # Outputs are keyed by *step id*, not by function name: that is the only
    # identifier a workflow author controls, so it is the stable key.
    unit = ctx["outputs"]["unit"]
    integration = ctx["outputs"]["integration"]
    if unit["failed"] or integration["failed"]:
        raise RuntimeError("tests failed; refusing to build")
    return {"artifact": "app-1.4.0.tar.gz", "bytes": 8_412_233}


def deploy(ctx):
    time.sleep(0.01)
    return {"url": "https://staging.internal", "ref": ctx["outputs"]["checkout"]["ref"]}


def notify(ctx):
    return {"sent_to": "#releases"}
