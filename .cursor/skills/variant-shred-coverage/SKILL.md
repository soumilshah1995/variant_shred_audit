---
name: variant-shred-coverage
description: Run the full Variant Shred Coverage Report (sections A–D) on Iceberg VARIANT tables. Always uses --full mode (all Parquet files + Section D no-benefit paths). Use when the user asks for variant shred coverage, shredding audit, variant shredding report, warehouse path audit, or invokes @variant-shred-coverage.
---

# Variant Shred Coverage Report

**Always run with `--full`.** Never run the audit without it.

Tool: https://github.com/soumilshah1995/variant_shred_audit

## What `--full` does

| Flag | Effect |
|------|--------|
| `--full` | All Parquet files (default) + Section D sampling (10,000 rows) |
| auto JSON | Finds `github_archive.json.gz` via `VARIANT_SHRED_JSON_FILE`, table dir, warehouse, or `~/IdeaProjects/study-learn/**/` |

## Agent workflow (every invocation)

1. Parse user input → `--parquet-dir` or `{warehouse}/{ns}/{table}/data`

2. `python3 -c "import pyarrow" 2>/dev/null || pip install 'pyarrow>=14.0.0'`

3. Find script: `$VARIANT_SHRED_AUDIT_SCRIPT` → `~/IdeaProjects/study-learn/variant_shred_audit/variant_shred_audit.py` → `tools/variant_shred_audit.py`

4. **Run full audit:**

   ```bash
   python3 <audit_script> \
     --parquet-dir /path/to/table/data \
     --variant-col v \
     --full
   ```

   Optional overrides: `--json-file /path/to/source.json.gz` · `--scan-rows N` · `--max-files N`

5. Show **complete terminal output** (A, B, B2, C, D). Add executive summary with:
   - Section C: all HIGH / MEDIUM / LOW paths
   - Section D: all no-benefit paths

## Path rules

- `{warehouse}/{namespace}/{table}/data/`
- Strip `file://` prefix from warehouse URIs

## Executive summary

```markdown
## Summary
- **Shredding**: NOT SHREDDED | SHREDDED (N paths)
- **Benefit (Section C):** N HIGH, N MEDIUM, N LOW — list all HIGH
- **No benefit (Section D):** N unshredded paths — list all (or note if JSON missing)
- **Action:** use HIGH in filters; avoid Section D paths in hot queries
```

## Embedded runner

Always pass `--full` to the audit script.

<!-- RUNNER_START -->
```python
#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, subprocess, sys
from pathlib import Path

DEFAULT_SCRIPT = Path.home() / "IdeaProjects/study-learn/variant_shred_audit/variant_shred_audit.py"

def find_audit_script(explicit):
    if explicit:
        p = Path(explicit).expanduser().resolve()
        if not p.is_file(): raise SystemExit(f"Audit script not found: {p}")
        return p
    for c in [os.environ.get("VARIANT_SHRED_AUDIT_SCRIPT"), DEFAULT_SCRIPT,
              Path("tools/variant_shred_audit.py"), Path("variant_shred_audit.py")]:
        if not c: continue
        p = Path(c).expanduser().resolve()
        if p.is_file(): return p
    raise SystemExit("variant_shred_audit.py not found")

def normalize_warehouse(warehouse):
    wh = warehouse.strip()
    if wh.startswith("file://"): wh = wh[len("file://"):]
    return Path(wh).expanduser().resolve()

def table_to_data_dir(warehouse, table):
    parts = table.strip().split(".")
    if len(parts) == 3: parts = parts[1:]
    if len(parts) != 2: raise SystemExit(f"Expected namespace.table, got: {table}")
    return warehouse / parts[0] / parts[1] / "data"

def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--parquet-dir"); g.add_argument("--warehouse")
    p.add_argument("--table")
    p.add_argument("--no-shred-dir"); p.add_argument("--with-shred-dir")
    p.add_argument("--variant-col", default="v")
    p.add_argument("--json-file"); p.add_argument("--scan-rows", type=int, default=0)
    p.add_argument("--max-files", type=int, default=0)
    p.add_argument("--audit-script")
    args = p.parse_args()

    script = find_audit_script(args.audit_script)
    cmd = [sys.executable, str(script), "--variant-col", args.variant_col, "--full"]
    if args.max_files: cmd.extend(["--max-files", str(args.max_files)])
    if args.scan_rows: cmd.extend(["--scan-rows", str(args.scan_rows)])
    if args.json_file: cmd.extend(["--json-file", str(Path(args.json_file).expanduser().resolve())])

    if args.parquet_dir:
        cmd.extend(["--parquet-dir", str(Path(args.parquet_dir).expanduser().resolve())])
    elif args.no_shred_dir and args.with_shred_dir:
        cmd.extend(["--no-shred-dir", str(Path(args.no_shred_dir).expanduser().resolve()),
                    "--with-shred-dir", str(Path(args.with_shred_dir).expanduser().resolve())])
    elif args.warehouse:
        if not args.table: raise SystemExit("--table required with --warehouse")
        d = table_to_data_dir(normalize_warehouse(args.warehouse), args.table)
        cmd.extend(["--parquet-dir", str(d)])
    else:
        raise SystemExit("Provide --parquet-dir, --warehouse + --table, or compare dirs")

    print(f"# Running: {' '.join(cmd)}", file=sys.stderr)
    raise SystemExit(subprocess.call(cmd))

if __name__ == "__main__":
    main()
```
<!-- RUNNER_END -->

## Example

```bash
python3 ~/IdeaProjects/study-learn/variant_shred_audit/variant_shred_audit.py \
  --parquet-dir /Users/sshah/IdeaProjects/study-learn/warehouse/demo/github_with_shredding/data \
  --variant-col v \
  --full
```
