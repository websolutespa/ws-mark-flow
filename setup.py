from pathlib import Path
from setuptools import setup, find_packages

_here = Path(__file__).parent.resolve()


def _read_requirements():
    req_file = _here / "requirements.txt"
    if not req_file.exists():
        return []
    reqs = []
    for raw in req_file.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            reqs.append(line)
    return reqs


setup(
    name="ws_mark_flow",
    version="0.0.7",
    description="Convert files from various sources (SharePoint, S3, Azure Blob, etc.) to Markdown and upload to destinations (Google Drive, SharePoint, etc.).",
    long_description=(_here / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    author="Websolute Spa",
    author_email="dev@websolute.it",
    url="https://github.com/websolutespa/ws-mark-flow",
    package_dir={"ws_mark_flow": "src"},
    # Auto-discover all subpackages under src/ so new ones (e.g. vectorstore)
    # don't need manual wiring. Top-level package is mapped above.
    packages=["ws_mark_flow", *(
        f"ws_mark_flow.{p}" for p in find_packages(where="src")
    )],
    include_package_data=True,
    install_requires=_read_requirements(),
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.12",
)