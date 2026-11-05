#!/usr/bin/env python3
"""Pin the agent skills in skills/ to an Aquifer release.

  pin-skills.py v0.16.1            update the "Pinned to" line in skills/*/SKILL.md
                                   (run it and commit before tagging a release)
  pin-skills.py v0.16.1 --check    exit non-zero unless every skill is pinned to v0.16.1
  pin-skills.py v0.16.1 --out DIR  also write a copy whose repo links point at
                                   that tag instead of main (the release asset).
                                   DIR must not exist yet, or be under dist/.
"""
import argparse, pathlib, re, shutil, sys

REPO = "https://github.com/rjpruitt16/aquifer"
PIN = re.compile(r"^Pinned to Aquifer (\S+)\.(?= )", re.M)
# SemVer with a leading v: prerelease and build metadata allowed.
TAG = re.compile(r"v\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?")
STALE = " Links to `main` here may describe features newer than their release; check before relying on one."


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("version")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--out")
    a = ap.parse_args()
    if not TAG.fullmatch(a.version):
        sys.exit(f"not a release tag: {a.version}")
    root = pathlib.Path(__file__).resolve().parent.parent / "skills"
    repo = root.parent
    skills = sorted(root.glob("*/SKILL.md"))
    if not skills:
        sys.exit("no skills found")

    # Validate everything before writing anything.
    texts = {}
    for p in skills:
        s = p.read_text()
        pins = PIN.findall(s)
        if len(pins) != 1:
            sys.exit(f"{p}: expected exactly one 'Pinned to Aquifer vX.' line, found {len(pins)}")
        texts[p] = (s, pins[0])
    if a.check:
        wrong = [f"{p.relative_to(repo)} is pinned to {pin}" for p, (_, pin) in texts.items() if pin != a.version]
        if wrong:
            sys.exit(f"not pinned to {a.version}: " + "; ".join(wrong))
        print(f"all {len(skills)} skills pinned to {a.version}")
        return
    out = None
    if a.out:
        out = pathlib.Path(a.out).resolve()
        dist = repo / "dist"
        if out.exists() and dist not in out.parents:
            sys.exit(f"--out {a.out} already exists; give a new directory (or one under dist/)")
        if out == repo or root in out.parents or out == root or out in repo.parents:
            sys.exit(f"--out must be a separate directory, not {a.out}")

    for p, (s, _) in texts.items():
        p.write_text(PIN.sub(f"Pinned to Aquifer {a.version}.", s))
    if out:
        if out.exists():
            shutil.rmtree(out)
        shutil.copytree(root, out)
        for p in out.rglob("*.md"):
            s = p.read_text()
            s = s.replace(f"{REPO}/blob/main/", f"{REPO}/blob/{a.version}/")
            s = s.replace(f"{REPO}/tree/main/", f"{REPO}/tree/{a.version}/")
            s = s.replace(STALE, "")  # in the pinned copy the links already point at the tag
            p.write_text(s)
    print(f"pinned {len(skills)} skills to {a.version}")


if __name__ == "__main__":
    main()
