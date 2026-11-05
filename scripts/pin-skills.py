#!/usr/bin/env python3
"""Pin the agent skills in skills/ to an Aquifer release.

  pin-skills.py v0.16.1            update the "Pinned to" line in skills/*/SKILL.md
                                   (run it and commit before tagging a release)
  pin-skills.py v0.16.1 --out DIR  also write a copy whose repo links point at
                                   that tag instead of main (the release asset)
"""
import argparse, pathlib, re, shutil, sys

REPO = "https://github.com/rjpruitt16/aquifer"
PIN = re.compile(r"^Pinned to Aquifer \S+\.", re.M)

def pin_line(v):
    return f"Pinned to Aquifer {v}."

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("version")
    ap.add_argument("--out")
    a = ap.parse_args()
    if not re.fullmatch(r"v\d+\.\d+\.\d+(-[\w.]+)?", a.version):
        sys.exit(f"not a release tag: {a.version}")
    root = pathlib.Path(__file__).resolve().parent.parent / "skills"
    skills = sorted(root.glob("*/SKILL.md"))
    if not skills:
        sys.exit("no skills found")
    for p in skills:
        s = p.read_text()
        if len(PIN.findall(s)) != 1:
            sys.exit(f"{p}: expected exactly one 'Pinned to Aquifer vX.' line")
        p.write_text(PIN.sub(pin_line(a.version), s))
    if a.out:
        out = pathlib.Path(a.out).resolve()
        repo = root.parent
        if out == repo or root == out or root in out.parents or out in repo.parents:
            sys.exit(f"--out must be a separate directory, not {a.out}")
        if out.exists():
            shutil.rmtree(out)
        shutil.copytree(root, out)
        for p in out.rglob("*.md"):
            s = p.read_text()
            s = s.replace(f"{REPO}/blob/main/", f"{REPO}/blob/{a.version}/")
            s = s.replace(f"{REPO}/tree/main/", f"{REPO}/tree/{a.version}/")
            # In the pinned copy the links already point at the tag.
            s = s.replace(" Links to `main` here may describe features newer than their release; check before relying on one.", "")
            p.write_text(s)
    print(f"pinned {len(skills)} skills to {a.version}")

if __name__ == "__main__":
    main()
