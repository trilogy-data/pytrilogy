"""Serve command for Trilogy CLI."""

import sys
from pathlib import Path as PathlibPath

from click import Path, argument, option, pass_context


def check_fastapi_available() -> bool:
    """Check if FastAPI and uvicorn are available."""
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401

        return True
    except ImportError:
        return False


@argument("directory", type=Path(exists=True, file_okay=False, dir_okay=True))
@option("--port", "-p", default=8100, help="Port to run the server on")
@option("--host", "-h", default="0.0.0.0", help="Host to bind the server to")
@pass_context
def serve(ctx, directory: str, port: int, host: str):
    """Start a FastAPI server to expose Trilogy models from a directory."""
    if not check_fastapi_available():
        print(
            "Error: FastAPI and uvicorn are required for the serve command.\n"
            "Please install with: pip install trilogy[serve]",
            file=sys.stderr,
        )
        sys.exit(1)

    import uvicorn
    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import PlainTextResponse

    from trilogy.scripts.serve_helpers import (
        ModelImport,
        StoreIndex,
        find_file_content_by_name,
        find_model_by_name,
        find_preql_files,
        generate_model_index,
    )

    directory_path = PathlibPath(directory).resolve()
    base_url = f"http://{host}:{port}"

    app = FastAPI(title="Trilogy Model Server", version="1.0.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/")
    async def root():
        """Root endpoint with server information."""
        preql_count = len(find_preql_files(directory_path))
        return {
            "message": "Trilogy Model Server",
            "description": f"Serving {preql_count} Trilogy models from {directory_path}",
            "endpoints": {
                "/index.json": "Get list of available models",
                "/models/<model-name>.json": "Get specific model details",
            },
        }

    @app.get("/index.json", response_model=StoreIndex)
    async def get_index() -> StoreIndex:
        """Return the store index with list of available models."""
        return StoreIndex(
            name=f"Trilogy Models - {directory_path.name}",
            models=generate_model_index(directory_path, base_url),
        )

    @app.get("/models/{model_name}.json", response_model=ModelImport)
    async def get_model(model_name: str) -> ModelImport:
        """Return a specific model by name."""
        model = find_model_by_name(model_name, directory_path, base_url)
        if model is None:
            raise HTTPException(status_code=404, detail="Model not found")
        return model

    @app.get("/files/{file_name}.preql")
    async def get_file(file_name: str):
        """Return the raw .preql file content."""
        content = find_file_content_by_name(file_name, directory_path)
        if content is None:
            raise HTTPException(status_code=404, detail="File not found")
        return PlainTextResponse(content=content)

    print(f"Starting Trilogy Model Server on http://{host}:{port}")
    print(f"Serving models from: {directory_path}")
    print(f"Access the index at: http://{host}:{port}/index.json")
    print(f"Found {len(find_preql_files(directory_path))} .preql files")

    uvicorn.run(app, host=host, port=port)
