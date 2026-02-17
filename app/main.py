"""Entry point for the FastAPI application."""
from fastapi import FastAPI

from app.api.routes import router as api_router

app = FastAPI(title="Mushroom Nowcast Service", version="0.1.0")

app.include_router(api_router)
