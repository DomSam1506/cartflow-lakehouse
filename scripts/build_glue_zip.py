"""Package the glue_jobs/ package for Glue's --extra-py-files. Run from anywhere."""
import pathlib
import zipfile

root = pathlib.Path(__file__).resolve().parents[1]
out = root / "build" / "glue_jobs.zip"
out.parent.mkdir(exist_ok=True)

with zipfile.ZipFile(out, "w") as z:
    for p in sorted((root / "glue_jobs").rglob("*.py")):
        # fixed timestamp: same input gives the same bytes, so Terraform only re-uploads on real changes
        info = zipfile.ZipInfo(p.relative_to(root).as_posix(), date_time=(2020, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o644 << 16
        z.writestr(info, p.read_bytes())

print(f"{out} ({out.stat().st_size} bytes)")
for name in zipfile.ZipFile(out).namelist():
    print("  ", name)