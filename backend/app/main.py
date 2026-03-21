from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.config import settings
from app.routers import analyze, suggest, generate

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    description="AI-powered data cleaning assistant"
)

# Handler pour les erreurs 422 - très utile pour le debug
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # CORRECTION: Gérer le body qui peut être bytes ou autre
    body = exc.body
    if isinstance(body, bytes):
        try:
            body = body.decode('utf-8')
        except:
            body = str(body)
    
    print(f"\n{'='*50}")
    print(f"VALIDATION ERROR on {request.method} {request.url.path}")
    print(f"Errors: {exc.errors()}")
    print(f"Body: {body}")
    print(f"{'='*50}\n")
    
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors(),
            "message": "Validation failed",
            "path": str(request.url.path),
            "body_preview": str(body)[:500] if body else None  # Limiter la taille
        }
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(analyze.router)
app.include_router(suggest.router)
app.include_router(generate.router)

@app.get("/")
async def root():
    return {
        "message": settings.APP_NAME,
        "version": settings.VERSION,
        "docs": "/docs"
    }