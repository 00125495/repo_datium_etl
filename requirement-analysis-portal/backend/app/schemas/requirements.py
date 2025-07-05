from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from app.models.requirements import RequirementType, RequirementStatus, Priority


# Base schemas
class ProjectBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    azure_devops_organization: Optional[str] = None
    azure_devops_project: Optional[str] = None
    azure_devops_area_path: Optional[str] = None
    azure_devops_iteration_path: Optional[str] = None


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


# Azure DevOps schemas
class AzureDevOpsIntegrationBase(BaseModel):
    organization_url: str = Field(..., min_length=1)
    project_name: str = Field(..., min_length=1)
    personal_access_token: Optional[str] = None
    default_work_item_type: str = "User Story"
    default_area_path: Optional[str] = None
    default_iteration_path: Optional[str] = None
    auto_sync_enabled: bool = False


class AzureDevOpsWorkItemCreate(BaseModel):
    title: str
    description: str
    work_item_type: str = "User Story"
    assigned_to: Optional[str] = None
    area_path: Optional[str] = None
    iteration_path: Optional[str] = None
    story_points: Optional[int] = None
    priority: Optional[str] = None
    tags: Optional[List[str]] = None


class AzureDevOpsWorkItemUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    state: Optional[str] = None
    assigned_to: Optional[str] = None
    story_points: Optional[int] = None
    priority: Optional[str] = None
    tags: Optional[List[str]] = None


# Create schemas
class ProjectCreate(ProjectBase):
    pass


class RequirementCreate(RequirementBase):
    project_id: int


class UserStoryCreate(UserStoryBase):
    project_id: int
    requirement_id: Optional[int] = None


class AzureDevOpsIntegrationCreate(AzureDevOpsIntegrationBase):
    project_id: int


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
    azure_devops_work_item_id: Optional[int] = None
    azure_devops_work_item_type: Optional[str] = None
    azure_devops_state: Optional[str] = None
    azure_devops_assigned_to: Optional[str] = None
    azure_devops_url: Optional[str] = None
    azure_devops_sync_status: Optional[str] = None
    azure_devops_last_sync: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True


class AzureDevOpsIntegration(AzureDevOpsIntegrationBase):
    id: int
    project_id: int
    is_active: bool
    last_sync: Optional[datetime]
    sync_status: str
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


# Azure DevOps specific schemas
class AzureDevOpsSyncRequest(BaseModel):
    user_story_id: int
    create_work_item: bool = True
    work_item_type: str = "User Story"
    assigned_to: Optional[str] = None


class AzureDevOpsSyncResponse(BaseModel):
    user_story_id: int
    work_item_id: Optional[int]
    work_item_url: Optional[str]
    sync_status: str
    sync_message: str


class AzureDevOpsProjectSyncRequest(BaseModel):
    project_id: int
    sync_all_stories: bool = False
    work_item_type: str = "User Story"


class AzureDevOpsProjectSyncResponse(BaseModel):
    project_id: int
    synced_stories: int
    failed_stories: int
    sync_details: List[AzureDevOpsSyncResponse]


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


class UserStoryFilter(BaseModel):
    project_id: Optional[int] = None
    requirement_id: Optional[int] = None
    sprint_number: Optional[int] = None
    azure_devops_sync_status: Optional[str] = None


class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    per_page: int
    total_pages: int


# Azure DevOps connection test
class AzureDevOpsConnectionTest(BaseModel):
    organization_url: str
    project_name: str
    personal_access_token: str


class AzureDevOpsConnectionTestResponse(BaseModel):
    success: bool
    message: str
    project_info: Optional[Dict[str, Any]] = None
    available_work_item_types: Optional[List[str]] = None
    available_iterations: Optional[List[str]] = None