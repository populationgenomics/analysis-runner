#!/usr/bin/env python3
"""Assert the analysis-runner wheel built from two git refs is equivalent.

Equivalence (SET-1223 DoD, per Dan on Jira):
- identical set of installed module paths (no deep-import breakage)
- byte-identical module files, except a whitelist of deliberate changes
- identical console entry points
- identical Requires-Dist dependency requirements
- METADATA printed as a unified diff for human review (not asserted)

Usage: check_package_equivalence.py BASE_REF HEAD_REF
Builds each ref in a temporary git worktree with `python -m build --wheel`,
auto-detecting the package directory (packages/analysis-runner if present,
else the repo root for the legacy setup.py layout).
"""

import difflib
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

# Deliberate, hand-reviewed changes only.
BYTE_DIFF_WHITELIST = {'analysis_runner/_version.py'}


def build_wheel(ref: str) -> Path:
    tree = Path(tempfile.mkdtemp(prefix='ar-equiv-'))
    git_path = shutil.which('git')
    if not git_path:
        raise RuntimeError('git not found in PATH')
    subprocess.run(  # noqa: S603
        [git_path, 'worktree', 'add', '--detach', str(tree), ref],
        check=True,
    )
    pkg_dir = tree / 'packages' / 'analysis-runner'
    if not pkg_dir.exists():
        pkg_dir = tree
    out_dir = tree / 'equiv-dist'
    subprocess.run(  # noqa: S603
        [sys.executable, '-m', 'build', '--wheel', '-o', str(out_dir), str(pkg_dir)],
        check=True,
    )
    return next(out_dir.glob('*.whl'))


def wheel_contents(whl: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(whl) as z:
        return {n: z.read(n) for n in z.namelist()}


def metadata_file(contents: dict[str, bytes], suffix: str) -> str:
    for name, data in contents.items():
        if name.endswith(suffix):
            return data.decode()
    return ''


def main() -> int:
    base_ref, head_ref = sys.argv[1], sys.argv[2]
    base = wheel_contents(build_wheel(base_ref))
    head = wheel_contents(build_wheel(head_ref))
    failures = []

    base_modules = {n for n in base if n.startswith('analysis_runner/')}
    head_modules = {n for n in head if n.startswith('analysis_runner/')}
    if base_modules != head_modules:
        failures.append(
            f'module path set changed: only-in-base={sorted(base_modules - head_modules)} '
            f'only-in-head={sorted(head_modules - base_modules)}'
        )

    for name in sorted(base_modules & head_modules):
        if name in BYTE_DIFF_WHITELIST:
            print(f'--- whitelisted diff in {name} (review by hand) ---')
            print(
                '\n'.join(
                    difflib.unified_diff(
                        base[name].decode().splitlines(),
                        head[name].decode().splitlines(),
                        f'{base_ref}:{name}',
                        f'{head_ref}:{name}',
                        lineterm='',
                    )
                )
            )
            continue
        if base[name] != head[name]:
            failures.append(f'module file changed: {name}')

    if metadata_file(base, 'entry_points.txt') != metadata_file(
        head, 'entry_points.txt'
    ):
        failures.append('entry_points.txt changed')

    def requires_dist(contents: dict[str, bytes]) -> set[str]:
        meta = metadata_file(contents, '.dist-info/METADATA')
        return {l for l in meta.splitlines() if l.startswith('Requires-Dist:')}

    if requires_dist(base) != requires_dist(head):
        failures.append(
            f'Requires-Dist changed: only-in-base={sorted(requires_dist(base) - requires_dist(head))} '
            f'only-in-head={sorted(requires_dist(head) - requires_dist(base))}'
        )

    print('--- METADATA diff (informational, human-reviewed) ---')
    print(
        '\n'.join(
            difflib.unified_diff(
                metadata_file(base, '.dist-info/METADATA').splitlines(),
                metadata_file(head, '.dist-info/METADATA').splitlines(),
                f'{base_ref}:METADATA',
                f'{head_ref}:METADATA',
                lineterm='',
            )
        )
    )

    if failures:
        print('\nEQUIVALENCE FAILURES:')
        for f in failures:
            print(f'  - {f}')
        return 1
    print('\npackage equivalence: OK')
    return 0


if __name__ == '__main__':
    sys.exit(main())
