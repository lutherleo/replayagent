"""The README quickstart must keep working exactly as printed."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

QUICKSTART = Path(__file__).resolve().parent.parent / "examples" / "quickstart.py"


def test_quickstart_runs_and_prints_the_receipt(tmp_path: Path) -> None:
    # Run from an empty directory, unmeasured: pytest-cov's subprocess hook would look
    # for the coverage config relative to the child's cwd and record statement-only data.
    env = {k: v for k, v in os.environ.items() if not k.startswith(("COV_CORE_", "COVERAGE_"))}
    proc = subprocess.run(
        [sys.executable, str(QUICKSTART)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
        check=False,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "{'receipt': 'invoice order-1/4: 7', 'status': 'sent'}"
    assert (tmp_path / "quickstart.db").exists()


def test_readme_embeds_the_quickstart_verbatim() -> None:
    readme = (QUICKSTART.parent.parent / "README.md").read_text()
    body = QUICKSTART.read_text()
    start = body.index("@activity(")
    end = body.index("def main()")
    assert body[start:end].strip() in readme
