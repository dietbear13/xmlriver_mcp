"""Fail builds containing local paths, project data, credential files or private keys."""

import re
import tarfile
import zipfile
from pathlib import Path


def main():
    for path in Path("dist").iterdir():
        if path.suffix == ".whl":
            with zipfile.ZipFile(path) as archive:
                entries = [(name, archive.read(name)) for name in archive.namelist()]
        elif path.name.endswith(".tar.gz"):
            with tarfile.open(path) as archive:
                entries = [
                    (member.name, archive.extractfile(member).read())
                    for member in archive.getmembers()
                    if member.isfile()
                ]
        else:
            continue
        for name, content in entries:
            assert not any(p in name.split("/") for p in (".env", ".mcp.json", ".git", ".venv")), (
                name
            )
            assert not re.search(
                rb"(?i)(ticketland|[A-Z]:[\\/]Users[\\/]|BEGIN (?:RSA |OPENSSH )?PRIVATE KEY)",
                content,
            ), name
            assert not re.search(rb"(?i)[?&]key=[a-z0-9]{20,}", content), name
        print(f"{path.name}: inspected {len(entries)} files")


if __name__ == "__main__":
    main()
