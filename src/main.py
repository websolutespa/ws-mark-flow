"""
Ws-Mark-Flow AI Converter - Main entry point.
Converts files from various sources to Markdown and uploads to destinations.
"""
import uvicorn

from .app import app


def main():
    """Run the FastAPI application."""
    uvicorn.run(
        "src.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )


if __name__ == "__main__":
    main()
