from backend.api.main import create_app

# Expose main FastAPI app for ASGI servers
app = create_app()
