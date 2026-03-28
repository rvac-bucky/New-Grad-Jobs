#!/usr/bin/env python3
"""Tests for the README.md / docs/ staging contract (issue #156).

The scraper must NOT write README.md. README.md is a static, human-maintained
document. Only docs/ artefacts (jobs.json, market-history.json, health.json,
feed.xml) may be written by the automated pipeline.

These tests act as a persistent regression guard so the contract cannot drift
again without a test failure surfacing the violation.
"""
import ast
import re
import pathlib

ROOT = pathlib.Path(__file__).parent.parent
SCRAPER = ROOT / "scripts" / "update_jobs.py"
WORKFLOW = ROOT / ".github" / "workflows" / "update-jobs.yml"


# ---------------------------------------------------------------------------
# Guard A: scraper must not define or call generate_readme
# ---------------------------------------------------------------------------

def _scraper_ast() -> ast.Module:
    return ast.parse(SCRAPER.read_text(encoding="utf-8"))


def test_scraper_does_not_define_generate_readme() -> None:
    """generate_readme must not be defined or called in update_jobs.py."""
    tree = _scraper_ast()
    # Check: no function definition
    defined = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    ]
    assert "generate_readme" not in defined, (
        "generate_readme is defined in update_jobs.py — README.md "
        "generation was re-introduced.  See issue #156."
    )
    # Check: no call site (ast.Name or ast.Attribute)
    called = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and (
            (isinstance(node.func, ast.Name) and node.func.id == "generate_readme")
            or (isinstance(node.func, ast.Attribute) and node.func.attr == "generate_readme")
        )
    ]
    assert not called, (
        f"generate_readme is called at line(s) "
        f"{[n.lineno for n in called]} in update_jobs.py — README.md "
        "generation was re-introduced.  See issue #156."
    )


def _string_constant_bindings(tree: ast.Module) -> dict[str, str]:
    """Return simple top-level name -> string bindings for constant paths."""
    bindings: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        value = node.value
        if isinstance(target, ast.Name) and isinstance(value, ast.Constant) and isinstance(value.value, str):
            bindings[target.id] = value.value
    return bindings


def _resolve_string(expr: ast.AST, bindings: dict[str, str]) -> str:
    """Resolve an AST expression to a comparable string when straightforward."""
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return expr.value
    if isinstance(expr, ast.Name):
        return bindings.get(expr.id, expr.id)
    if isinstance(expr, ast.Call) and expr.args:
        fn = expr.func
        if (
            isinstance(fn, ast.Name)
            and fn.id == "Path"
            or isinstance(fn, ast.Attribute)
            and fn.attr == "Path"
        ):
            return _resolve_string(expr.args[0], bindings)
    return ast.unparse(expr)


def _readme_open_violations(tree: ast.Module) -> list[str]:
    """Return AST violation strings for any open() call that targets README.

    Handles:
    - positional arg:   open("README.md", ...)
    - keyword arg:      open(file="README.md", ...)
    - simple binding:   p = "README.md"; open(p)
    - attribute open:   Path("README.md").open(...) / pathlib.Path(...).open(...)
    - simple binding:   p = "README.md"; Path(p).open(...)
    """
    violations = []
    bindings = _string_constant_bindings(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func

        # open(...) as a plain name or attribute (e.g. builtins.open)
        if isinstance(fn, ast.Name) and fn.id == "open":
            _check_open_args(node, violations, bindings)

        elif isinstance(fn, ast.Attribute) and fn.attr == "open":
            # Path("README.md").open() — check the receiver, not the call args
            receiver_str = _resolve_string(fn.value, bindings)
            if "readme" in receiver_str.lower():
                violations.append(
                    f"line {node.lineno}: <expr>.open() on {receiver_str!r}"
                )
            # Also check any args passed to .open() itself
            _check_open_args(node, violations, bindings)

    return violations


def _check_open_args(
    node: ast.Call,
    violations: list[str],
    bindings: dict[str, str],
) -> None:
    """Append a violation string if any open() argument names a README path."""
    candidates: list[str] = []
    # Positional arg[0] is the file path
    if node.args:
        candidates.append(_resolve_string(node.args[0], bindings))
    # Keyword arg named "file"
    candidates.extend(
        _resolve_string(kw.value, bindings) for kw in node.keywords if kw.arg == "file"
    )
    for arg_str in candidates:
        if "readme" in arg_str.lower():
            violations.append(f"line {node.lineno}: open({arg_str!r})")


def test_scraper_does_not_open_readme() -> None:
    """update_jobs.py must not open() a path containing 'README' (any form)."""
    tree = _scraper_ast()
    violations = _readme_open_violations(tree)
    assert not violations, (
        "update_jobs.py opens a README path — README.md generation "
        "was re-introduced.  See issue #156.\n" + "\n".join(violations)
    )


def test_scraper_docstring_does_not_mention_readme() -> None:
    """Module-level docstring must not claim the scraper updates README.md."""
    source = SCRAPER.read_text(encoding="utf-8")
    # Only check the first 20 lines (module docstring area)
    header = "\n".join(source.splitlines()[:20])
    assert "README.md" not in header, (
        "Module docstring still mentions README.md — update it to reflect "
        "the current contract.  See issue #156."
    )


# ---------------------------------------------------------------------------
# Guard B: update-jobs.yml must not stage README.md
# ---------------------------------------------------------------------------

def _git_add_lines() -> list[str]:
    """Extract only the workflow lines that execute git add commands."""
    content = WORKFLOW.read_text(encoding="utf-8")
    return re.findall(r"(?m)^\s*git add\s+([^\n]+)$", content)


def test_workflow_does_not_stage_readme() -> None:
    """update-jobs.yml git-add step must not include README.md."""
    git_add_lines = _git_add_lines()
    match = next((line for line in git_add_lines if "README.md" in line), None)
    assert match is None, (
        f"update-jobs.yml stages README.md: {match!r}\n"
        "README.md must not be committed by the automated scraper workflow.  "
        "See issue #156."
    )


def test_workflow_stages_docs_files() -> None:
    """update-jobs.yml must stage the required docs/ artifacts."""
    git_add_blob = "\n".join(_git_add_lines())
    required = [
        "docs/jobs.json",
        "docs/market-history.json",
        "docs/health.json",
        "docs/feed.xml",
    ]
    missing = [f for f in required if f not in git_add_blob]
    assert not missing, (
        "update-jobs.yml is missing required docs/ artifacts in git add: "
        + ", ".join(missing)
        + "  See issue #156."
    )
