"""
UI loader for the Ws-Mark-Flow AI Converter.
Assembles the HTML template, CSS and JavaScript from separate source files.
"""
from pathlib import Path

_BASE = Path(__file__).parent


def get_ui_html() -> str:
    """Return the assembled HTML for the converter UI."""
    html = (_BASE / "ui.html").read_text(encoding="utf-8")
    css = (_BASE / "ui.css").read_text(encoding="utf-8")
    js = (_BASE / "ui.js").read_text(encoding="utf-8")
    return (
        html
        .replace("/* APP_CSS */", css)
        .replace("<!-- APP_JS -->", js)
    )
