#!/usr/bin/env python3
"""Build a pinned, minimal master hotfix from an installed 3.0.0-alpha baseline.

No server access or deployment is performed. All inputs must be local copies.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import zipfile

BASE_SHA256 = 'e5395804de6a7823e54f332f08e58d88092dc7c9ade28a8dca4dd8682280ab7b'
CLASSES = [
    'org/apache/dolphinscheduler/server/master/registry/MasterRegistryClient',
    'org/apache/dolphinscheduler/server/master/runner/StateWheelExecuteThread',
    'org/apache/dolphinscheduler/server/master/runner/WorkflowExecuteThread',
    'org/apache/dolphinscheduler/server/master/runner/task/CommonTaskProcessor',
]

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--libs', required=True, type=Path)
    parser.add_argument('--javac', required=True, type=Path)
    parser.add_argument('--annotations', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    baseline = args.libs / 'dolphinscheduler-master-3.0.0-alpha.jar'
    if digest(baseline) != BASE_SHA256:
        raise SystemExit('Unexpected baseline master JAR; refusing to build')
    version = subprocess.check_output([str(args.javac), '-version'], stderr=subprocess.STDOUT).decode().strip()
    if not version.startswith('javac 1.8.'):
        raise SystemExit('This pinned build requires a JDK 8 compiler')
    sources = [root / 'dolphinscheduler-master/src/main/java' / (name + '.java') for name in CLASSES]
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root).decode().strip()
    metadata = {
        'source_commit': revision,
        'compiler': version,
        'baseline_master_sha256': BASE_SHA256,
        'sources': {str(p.relative_to(root)): digest(p) for p in sources},
        'dependencies': {p.name: digest(p) for p in sorted(args.libs.glob('*.jar'))},
        'annotation_jar_sha256': digest(args.annotations),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        raise SystemExit('Output already exists; choose a new build output')
    with tempfile.TemporaryDirectory(prefix='ds-master-build-') as tmp:
        dest = Path(tmp)
        classpath = str(args.libs.resolve() / '*') + ':' + str(args.annotations.resolve())
        subprocess.run([str(args.javac), '-proc:none', '-g', '-source', '8', '-target', '8',
                        '-encoding', 'UTF-8', '-cp', classpath, '-d', str(dest)] + list(map(str, sources)), check=True)
        generated = {str(p.relative_to(dest)): p.read_bytes() for p in dest.rglob('*.class')}
        for name in generated:
            if not any(name == c + '.class' or name.startswith(c + '$') for c in CLASSES):
                raise SystemExit('Unexpected generated class: ' + name)
        with zipfile.ZipFile(baseline) as old:
            if not set(generated).issubset(old.namelist()):
                raise SystemExit('Build introduced unexpected class names')
            metadata['replaced_entries'] = sorted(generated)
            with zipfile.ZipFile(args.output, 'w') as new:
                for entry in old.infolist():
                    new.writestr(entry, generated.get(entry.filename, old.read(entry.filename)))
                info = zipfile.ZipInfo('META-INF/xcf-hotfix.json', (2026, 10, 27, 0, 0, 0))
                new.writestr(info, json.dumps(metadata, indent=2, sort_keys=True) + '\n')
            with zipfile.ZipFile(args.output) as new:
                assert new.testzip() is None
                for name in old.namelist():
                    if name not in generated:
                        assert old.read(name) == new.read(name), name
    summary = {'artifact': str(args.output), 'sha256': digest(args.output), **metadata}
    args.output.with_suffix('.build.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    print(json.dumps({k: summary[k] for k in ['artifact', 'sha256', 'source_commit', 'replaced_entries']}, indent=2))

if __name__ == '__main__':
    main()
