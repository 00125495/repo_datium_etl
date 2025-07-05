from fastapi import APIRouter
from app.api.v1.endpoints import projects, requirements, user_stories, ai_services

api_router = APIRouter()

api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(requirements.router, prefix="/requirements", tags=["requirements"])
api_router.include_router(user_stories.router, prefix="/user-stories", tags=["user_stories"])
api_router.include_router(ai_services.router, prefix="/ai", tags=["ai_services"])