"""Shared helper: run a Hadoop Streaming script the way Hadoop runs it — text on stdin, text out."""
import os, subprocess, sys, pathlib

KIT = pathlib.Path(__file__).resolve().parent.parent


def run_script(rel_path, stdin_text, env_extra=None, cwd=None):
    env = dict(os.environ, LC_ALL="C", **(env_extra or {}))
    p = subprocess.run(
        [sys.executable, str(KIT / rel_path)],
        input=stdin_text, capture_output=True, text=True, env=env, cwd=cwd,
    )
    assert p.returncode == 0, f"{rel_path} failed:\n{p.stderr}"
    return p.stdout


def shuffle_sort(mapper_out):
    """Stand in for Hadoop's sort between map and reduce: byte order on the whole line."""
    return "".join(sorted(mapper_out.splitlines(keepends=True)))
