"""
Application configuration loaded from environment variables.
"""
import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings from environment."""
    
    # App settings
    app_name: str = "Ws-Mark-Flow"
    app_version: str = "0.1.0"
    debug: bool = False
    
    # MongoDB settings
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "ws-mark-flow"

    # LLM settings (for AI-assisted conversion)
    llm_provider: str = "openai"  # openai, anthropic, google, ollama
    llm_api_key: str = ""  # Should be set in .env for security
    llm_model: str = "gpt-5.4"  # vision capable model for file analysis
    llm_base_url: str = ""  # Custom base URL (required for ollama, e.g. http://localhost:11434/v1)
    
    # PDF complexity analysis (ACCURATE strategy)
    llm_max_pages: int = 50  # PDFs with more pages skip LLM, use Docling directly
    pdf_complexity_threshold: float = 0.1  # 0-1 score; below this, downscale to Docling

    # Batch processing
    batch_size: int = 4  # Number of files to process concurrently
    
    # Temp directory for conversions
    temp_dir: str = "./.data/tmp"

    # Basic auth credentials (set AUTH_USERNAME / AUTH_PASSWORD in .env)
    auth_username: str = "admin"
    auth_password: str = ""  # Empty string disables auth
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
