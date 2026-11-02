"""
Compare the results run_compat_check.sh collected for two Python versions.

Each result directory holds:

- offline.xml / integration.xml / warnings.xml : junit reports of the pytest runs
- offline.exit / integration.exit / warnings.exit : the exit codes of those runs
- snapshot.json : the output of snapshot.py
- freeze.txt : the installed packages

The report lists every test whose outcome differs, every test that failed in either
version, every test that a deprecation or future warning broke, every snapshot value
that differs and the package versions that differ.

usage:
    python scripts/compat/compare.py <base result dir> <new result dir>
returns (exit code):
    0 if both versions behave the same and nothing failed, 1 otherwise
"""

import json
import math
import pathlib
import sys
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterator, List, Optional, Tuple

# relative tolerance for float values, so that last bit differences of a mean are not
# reported as a change of behaviour
FLOAT_RTOL = 1e-9

# how many places are listed under one kind of snapshot difference
SHOWN_PATHS = 5


def read_junit(path: pathlib.Path) -> Optional[Dict[str, str]]:
    """
    Read the outcome of every test in a junit report.

    args:
        - path : the junit xml file
    returns:
        - test id -> passed / failed / error / skipped, or None if the file is missing
    """
    if not path.is_file():
        return None
    outcomes = {}
    for case in ET.parse(path).getroot().iter('testcase'):
        test_id = f"{case.get('classname')}::{case.get('name')}"
        outcome = 'passed'
        for tag in ('failure', 'error', 'skipped'):
            if case.find(tag) is not None:
                outcome = 'failed' if tag == 'failure' else tag
        outcomes[test_id] = outcome
    return outcomes


def junit_messages(path: pathlib.Path) -> Dict[str, str]:
    """
    Read the first line of the failure or error message of every broken test.

    args:
        - path : the junit xml file
    returns:
        - test id -> message
    """
    messages = {}
    if not path.is_file():
        return messages
    for case in ET.parse(path).getroot().iter('testcase'):
        for tag in ('failure', 'error'):
            node = case.find(tag)
            if node is not None:
                message = (node.get('message') or node.text or '').strip().splitlines()
                messages[f"{case.get('classname')}::{case.get('name')}"] = message[0] if message else ''
    return messages


def check_exit_code(name: str, label: str, result_dir: pathlib.Path) -> int:
    """
    Check the exit code run_compat_check.sh recorded for one pytest run.

    pytest exits with 0 when every test passed and 1 when some failed, which the junit
    report itself lists. Any other code (interrupted, internal error, usage error, no
    tests collected) means the report cannot be trusted to cover the run.

    args:
        - name : the run, e.g. offline
        - label : the version label
        - result_dir : the result directory of that version
    returns:
        - the number of problems found, 0 or 1
    """
    path = result_dir / f'{name}.exit'
    if not path.is_file():
        print(f'  MISSING[{label}] {name}.exit was not written')
        return 1
    code = int(path.read_text().strip())
    if code == 0:
        return 0
    if code == 1 and junit_messages(result_dir / f'{name}.xml'):
        # the failures are reported test by test from the junit report
        return 0
    print(f'  EXIT[{label}] pytest exited with {code}, see {name}.log')
    return 1


def report_junit(name: str, base_dir: pathlib.Path, new_dir: pathlib.Path,
                 base_label: str, new_label: str) -> int:
    """
    Print the outcome differences and failures of one pytest run.

    args:
        - name : the run, e.g. offline
        - base_dir / new_dir : the result directories
        - base_label / new_label : the version labels
    returns:
        - the number of problems found
    """
    base = read_junit(base_dir / f'{name}.xml')
    new = read_junit(new_dir / f'{name}.xml')
    print(f'\n## pytest: {name}')
    problems = 0
    for label, result_dir in ((base_label, base_dir), (new_label, new_dir)):
        problems += check_exit_code(name, label, result_dir)
    if base is None or new is None:
        # pytest crashed before writing its report, or was never run, so the tests of
        # this run were not checked at all
        for label, outcomes in ((base_label, base), (new_label, new)):
            if outcomes is None:
                problems += 1
                print(f'  MISSING[{label}] {name}.xml was not written')
        return problems
    for label, outcomes in ((base_label, base), (new_label, new)):
        counts: Dict[str, int] = {}
        for outcome in outcomes.values():
            counts[outcome] = counts.get(outcome, 0) + 1
        print(f'  {label}: {dict(sorted(counts.items()))}')
    for test_id in sorted(set(base) | set(new)):
        before, after = base.get(test_id, 'absent'), new.get(test_id, 'absent')
        if before != after:
            problems += 1
            print(f'  DIFF {test_id}: {before} -> {after}')
    for label, result_dir in ((base_label, base_dir), (new_label, new_dir)):
        for test_id, message in junit_messages(result_dir / f'{name}.xml').items():
            problems += 1
            print(f'  FAIL[{label}] {test_id}: {message}')
    if name == 'integration':
        for label, outcomes in ((base_label, base), (new_label, new)):
            skipped = sum(1 for outcome in outcomes.values() if outcome == 'skipped')
            if skipped:
                # skipped tests compare equal, so say so instead of letting them look checked
                print(f'  WARNING[{label}] {skipped} tests skipped, see integration.log for the reason')
    return problems


def diff_values(base: Any, new: Any, path: str = '') -> Iterator[Tuple[str, Any, Any]]:
    """
    Walk two JSON values and yield every place where they differ.

    args:
        - base / new : the values to compare
        - path : the location of the values, used in the output
    returns:
        - an iterator of (path, base value, new value)
    """
    if isinstance(base, dict) and isinstance(new, dict):
        for key in sorted(set(base) | set(new)):
            yield from diff_values(base.get(key, '<absent>'), new.get(key, '<absent>'), f'{path}/{key}')
    elif isinstance(base, list) and isinstance(new, list) and len(base) == len(new):
        for i, (b, n) in enumerate(zip(base, new)):
            yield from diff_values(b, n, f'{path}[{i}]')
    elif (isinstance(base, float) or isinstance(new, float)) and \
            isinstance(base, (int, float)) and isinstance(new, (int, float)) and \
            not isinstance(base, bool) and not isinstance(new, bool):
        if type(base) is not type(new) or not math.isclose(base, new, rel_tol=FLOAT_RTOL):
            yield path, base, new
    elif base != new:
        yield path, base, new


def report_snapshot(base_dir: pathlib.Path, new_dir: pathlib.Path) -> int:
    """
    Print the differences between the two snapshots.

    args:
        - base_dir / new_dir : the result directories
    returns:
        - the number of differences
    """
    print('\n## snapshot')
    base_path, new_path = base_dir / 'snapshot.json', new_dir / 'snapshot.json'
    if not base_path.is_file() or not new_path.is_file():
        print('  (snapshot missing)')
        return 1
    base, new = json.loads(base_path.read_text()), json.loads(new_path.read_text())
    print(f"  base: {base['meta']}")
    print(f"  new : {new['meta']}")
    problems = 0
    if base['meta']['live'] != new['meta']['live']:
        problems += 1
        print('  WARNING: only one of the snapshots holds the live cases')
    # a live case that failed the same way on both versions shows no difference, so a
    # wrong url or token would otherwise pass for a successful comparison
    for label, snapshot in (('base', base), ('new', new)):
        for name, case in snapshot['cases'].items():
            if name.startswith('live/') and ('error' in case or case['stdout']):
                problems += 1
                detail = case['error']['message'] if 'error' in case else case['stdout']
                print(f'  LIVE FAILED[{label}] {name}: {detail.splitlines()[0][:200]}')
    diffs: List[Tuple[str, Any, Any]] = list(diff_values(base['cases'], new['cases']))
    # the same change (e.g. a dtype object -> str) shows up in many cases, so group the
    # differences by what changed and list the places under it
    groups: Dict[Tuple[str, str], List[str]] = {}
    for path, before, after in diffs:
        key = (json.dumps(before, ensure_ascii=False)[:300], json.dumps(after, ensure_ascii=False)[:300])
        groups.setdefault(key, []).append(path)
    for (before, after), paths in groups.items():
        print(f'  DIFF x{len(paths)}\n      base: {before}\n      new : {after}')
        for path in paths[:SHOWN_PATHS]:
            print(f'      at {path}')
        if len(paths) > SHOWN_PATHS:
            print(f'      ... and {len(paths) - SHOWN_PATHS} more')
    print(f"  {len(base['cases'])} cases, {len(diffs)} differences in {len(groups)} kinds")
    return problems + len(diffs)


def report_freeze(base_dir: pathlib.Path, new_dir: pathlib.Path) -> None:
    """
    Print the packages whose installed version differs.

    args:
        - base_dir / new_dir : the result directories
    """
    def read(path: pathlib.Path) -> Dict[str, str]:
        packages = {}
        if path.is_file():
            for line in path.read_text().splitlines():
                if '==' in line:
                    name, version = line.split('==', 1)
                    packages[name.lower()] = version
        return packages

    base, new = read(base_dir / 'freeze.txt'), read(new_dir / 'freeze.txt')
    print('\n## installed packages that differ')
    for name in sorted(set(base) | set(new)):
        if base.get(name) != new.get(name):
            print(f"  {name}: {base.get(name, '-')} -> {new.get(name, '-')}")


def main() -> int:
    """
    Compare the two result directories given on the command line.

    returns:
        - the process exit code
    """
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    base_dir, new_dir = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    base_label, new_label = base_dir.name, new_dir.name
    print(f'# compatibility report: python {base_label} -> {new_label}')
    problems = 0
    for name in ('offline', 'integration', 'warnings'):
        problems += report_junit(name, base_dir, new_dir, base_label, new_label)
    problems += report_snapshot(base_dir, new_dir)
    report_freeze(base_dir, new_dir)
    print(f'\nresult: {"SAME" if problems == 0 else f"{problems} problems found"}')
    return 0 if problems == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
