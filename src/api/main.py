"""FastAPI REST server — serves the frontend at port 8000."""
from __future__ import annotations

from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ..store.db import init_db
from ..hyper.manager import HyperManager
from .routers import connections, flows
from .routers import hyper as hyper_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    HyperManager.get().start()
    yield
    HyperManager.get().stop()


app = FastAPI(title="SQL Compiler API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(connections.router)
app.include_router(flows.router)
app.include_router(hyper_router.router)


@app.get("/health")
def health():
    return {"status": "ok"}
