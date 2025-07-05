from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from app.models.requirements import RequirementType, RequirementStatus, Priority


# Base schemas
class ProjectBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None


class RequirementBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., min_length=1)
    type: RequirementType
    priority: Priority = Priority.MEDIUM
    business_value: Optional[str] = None
    acceptance_criteria: Optional[str] = None
    technical_requirements: Optional[Dict[str, Any]] = None
    dependencies: Optional[List[str]] = None
    estimated_effort: Optional[float] = None


class UserStoryBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., min_length=1)
    acceptance_criteria: Optional[str] = None
    story_points: Optional[int] = Field(None, ge=1, le=100)
    priority: Priority = Priority.MEDIUM
    sprint_number: Optional[int] = Field(None, ge=1)
    epic: Optional[str] = None


# Create schemas
class ProjectCreate(ProjectBase):
    pass


class RequirementCreate(RequirementBase):
    project_id: int


class UserStoryCreate(UserStoryBase):
    project_id: int
    requirement_id: Optional[int] = None


# Response schemas
class Project(ProjectBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime]
    is_active: bool

    class Config:
        from_attributes = True


class AIFeedbackResponse(BaseModel):
    id: int
    feedback_type: str
    message: str
    severity: str
    score: Optional[float]
    model_used: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class Requirement(RequirementBase):
    id: int
    status: RequirementStatus
    project_id: int
    ai_feedback: Optional[Dict[str, Any]] = None
    ai_score: Optional[float] = None
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True


class UserStory(UserStoryBase):
    id: int
    project_id: int
    requirement_id: Optional[int] = None
    jira_ticket_id: Optional[str] = None
    github_issue_id: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True


# Extended response schemas with relationships
class ProjectWithRequirements(Project):
    requirements: List[Requirement] = []
    user_stories: List[UserStory] = []


class RequirementWithUserStories(Requirement):
    user_stories: List[UserStory] = []
    project: Project


# AI validation schemas
class AIValidationRequest(BaseModel):
    requirement_id: int
    force_revalidation: bool = False


class AIValidationResponse(BaseModel):
    requirement_id: int
    overall_score: float
    feedback: List[AIFeedbackResponse]
    suggestions: List[str]
    is_valid: bool


# User story generation schemas
class UserStoryGenerationRequest(BaseModel):
    requirement_id: int
    sprint_number: int
    max_stories: int = Field(default=5, ge=1, le=20)
    include_technical_tasks: bool = True


class UserStoryGenerationResponse(BaseModel):
    requirement_id: int
    generated_stories: List[UserStoryCreate]
    total_estimated_points: int
    recommendations: List[str]


# Bulk operations
class BulkRequirementCreate(BaseModel):
    project_id: int
    requirements: List[RequirementCreate]


class BulkUserStoryCreate(BaseModel):
    project_id: int
    user_stories: List[UserStoryCreate]


# Search and filtering
class RequirementFilter(BaseModel):
    project_id: Optional[int] = None
    type: Optional[RequirementType] = None
    status: Optional[RequirementStatus] = None
    priority: Optional[Priority] = None
    min_ai_score: Optional[float] = Field(None, ge=0, le=1)
    search_query: Optional[str] = None


class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    per_page: int
    total_pages: int