from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.requirements import (
    AzureDevOpsIntegration as AzureDevOpsIntegrationModel,
    UserStory as UserStoryModel,
    Project as ProjectModel
)
from app.schemas.requirements import (
    AzureDevOpsIntegration,
    AzureDevOpsIntegrationCreate,
    AzureDevOpsConnectionTest,
    AzureDevOpsConnectionTestResponse,
    AzureDevOpsSyncRequest,
    AzureDevOpsSyncResponse,
    AzureDevOpsProjectSyncRequest,
    AzureDevOpsProjectSyncResponse
)
from app.services.azure_devops_service import azure_devops_service

router = APIRouter()

@router.post("/test-connection", response_model=AzureDevOpsConnectionTestResponse)
async def test_azure_devops_connection(
    connection_test: AzureDevOpsConnectionTest
):
    """Test connection to Azure DevOps"""
    return await azure_devops_service.test_connection(
        connection_test.organization_url,
        connection_test.project_name,
        connection_test.personal_access_token
    )

@router.post("/integrations", response_model=AzureDevOpsIntegration)
def create_azure_devops_integration(
    integration: AzureDevOpsIntegrationCreate,
    db: Session = Depends(get_db)
):
    """Create Azure DevOps integration for a project"""
    # Check if project exists
    project = db.query(ProjectModel).filter(ProjectModel.id == integration.project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Check if integration already exists for this project
    existing = db.query(AzureDevOpsIntegrationModel).filter(
        AzureDevOpsIntegrationModel.project_id == integration.project_id,
        AzureDevOpsIntegrationModel.is_active == True
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Azure DevOps integration already exists for this project"
        )
    
    # Create new integration
    db_integration = AzureDevOpsIntegrationModel(**integration.dict())
    db.add(db_integration)
    db.commit()
    db.refresh(db_integration)
    return db_integration

@router.get("/integrations/{project_id}", response_model=AzureDevOpsIntegration)
def get_azure_devops_integration(
    project_id: int,
    db: Session = Depends(get_db)
):
    """Get Azure DevOps integration for a project"""
    integration = db.query(AzureDevOpsIntegrationModel).filter(
        AzureDevOpsIntegrationModel.project_id == project_id,
        AzureDevOpsIntegrationModel.is_active == True
    ).first()
    
    if not integration:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Azure DevOps integration not found for this project"
        )
    
    return integration

@router.put("/integrations/{integration_id}", response_model=AzureDevOpsIntegration)
def update_azure_devops_integration(
    integration_id: int,
    integration_update: AzureDevOpsIntegrationCreate,
    db: Session = Depends(get_db)
):
    """Update Azure DevOps integration"""
    db_integration = db.query(AzureDevOpsIntegrationModel).filter(
        AzureDevOpsIntegrationModel.id == integration_id
    ).first()
    
    if not db_integration:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Azure DevOps integration not found"
        )
    
    # Update fields (excluding project_id)
    update_data = integration_update.dict(exclude={"project_id"})
    for key, value in update_data.items():
        setattr(db_integration, key, value)
    
    db.commit()
    db.refresh(db_integration)
    return db_integration

@router.delete("/integrations/{integration_id}")
def delete_azure_devops_integration(
    integration_id: int,
    db: Session = Depends(get_db)
):
    """Delete (deactivate) Azure DevOps integration"""
    db_integration = db.query(AzureDevOpsIntegrationModel).filter(
        AzureDevOpsIntegrationModel.id == integration_id
    ).first()
    
    if not db_integration:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Azure DevOps integration not found"
        )
    
    db_integration.is_active = False
    db.commit()
    return {"message": "Azure DevOps integration deactivated successfully"}

@router.post("/sync/user-story", response_model=AzureDevOpsSyncResponse)
async def sync_user_story_to_azure_devops(
    sync_request: AzureDevOpsSyncRequest,
    db: Session = Depends(get_db)
):
    """Sync a single user story to Azure DevOps"""
    # Get user story
    user_story = db.query(UserStoryModel).filter(
        UserStoryModel.id == sync_request.user_story_id
    ).first()
    
    if not user_story:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User story not found"
        )
    
    # Get Azure DevOps integration
    integration = db.query(AzureDevOpsIntegrationModel).filter(
        AzureDevOpsIntegrationModel.project_id == user_story.project_id,
        AzureDevOpsIntegrationModel.is_active == True
    ).first()
    
    if not integration:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Azure DevOps integration not found for this project"
        )
    
    # Sync user story
    result = await azure_devops_service.sync_user_story_to_azure_devops(
        db, user_story, integration, sync_request.create_work_item
    )
    
    return result

@router.post("/sync/project", response_model=AzureDevOpsProjectSyncResponse)
async def sync_project_to_azure_devops(
    sync_request: AzureDevOpsProjectSyncRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Sync all user stories in a project to Azure DevOps"""
    # Get project
    project = db.query(ProjectModel).filter(ProjectModel.id == sync_request.project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Get Azure DevOps integration
    integration = db.query(AzureDevOpsIntegrationModel).filter(
        AzureDevOpsIntegrationModel.project_id == sync_request.project_id,
        AzureDevOpsIntegrationModel.is_active == True
    ).first()
    
    if not integration:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Azure DevOps integration not found for this project"
        )
    
    # Get user stories to sync
    query = db.query(UserStoryModel).filter(UserStoryModel.project_id == sync_request.project_id)
    
    if not sync_request.sync_all_stories:
        # Only sync stories that haven't been synced yet
        query = query.filter(
            (UserStoryModel.azure_devops_work_item_id.is_(None)) |
            (UserStoryModel.azure_devops_sync_status == "sync_failed")
        )
    
    user_stories = query.all()
    
    if not user_stories:
        return AzureDevOpsProjectSyncResponse(
            project_id=sync_request.project_id,
            synced_stories=0,
            failed_stories=0,
            sync_details=[]
        )
    
    # Sync stories (use background task for large numbers)
    if len(user_stories) > 10:
        background_tasks.add_task(
            sync_project_stories_background,
            db,
            user_stories,
            integration,
            sync_request.sync_all_stories
        )
        
        return AzureDevOpsProjectSyncResponse(
            project_id=sync_request.project_id,
            synced_stories=0,
            failed_stories=0,
            sync_details=[],
            message=f"Queued {len(user_stories)} stories for background sync"
        )
    else:
        # Sync immediately for small numbers
        sync_details = []
        synced_count = 0
        failed_count = 0
        
        for story in user_stories:
            result = await azure_devops_service.sync_user_story_to_azure_devops(
                db, story, integration, sync_request.sync_all_stories
            )
            sync_details.append(result)
            
            if result.sync_status == "synced":
                synced_count += 1
            else:
                failed_count += 1
        
        return AzureDevOpsProjectSyncResponse(
            project_id=sync_request.project_id,
            synced_stories=synced_count,
            failed_stories=failed_count,
            sync_details=sync_details
        )

@router.patch("/user-stories/{story_id}/azure-devops")
def update_user_story_azure_devops_link(
    story_id: int,
    work_item_id: int,
    work_item_url: str = None,
    db: Session = Depends(get_db)
):
    """Manually link a user story to an Azure DevOps work item"""
    user_story = db.query(UserStoryModel).filter(UserStoryModel.id == story_id).first()
    
    if not user_story:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User story not found"
        )
    
    user_story.azure_devops_work_item_id = work_item_id
    user_story.azure_devops_url = work_item_url
    user_story.azure_devops_sync_status = "manually_linked"
    
    db.commit()
    db.refresh(user_story)
    
    return {"message": "Azure DevOps work item linked successfully"}

@router.post("/sync/pull-status/{story_id}")
async def pull_azure_devops_status(
    story_id: int,
    db: Session = Depends(get_db)
):
    """Pull status updates from Azure DevOps for a user story"""
    user_story = db.query(UserStoryModel).filter(UserStoryModel.id == story_id).first()
    
    if not user_story:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User story not found"
        )
    
    if not user_story.azure_devops_work_item_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User story is not linked to an Azure DevOps work item"
        )
    
    # Get Azure DevOps integration
    integration = db.query(AzureDevOpsIntegrationModel).filter(
        AzureDevOpsIntegrationModel.project_id == user_story.project_id,
        AzureDevOpsIntegrationModel.is_active == True
    ).first()
    
    if not integration:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Azure DevOps integration not found for this project"
        )
    
    # Pull status from Azure DevOps
    success = await azure_devops_service.sync_work_item_status_from_azure_devops(
        db, user_story, integration
    )
    
    if success:
        return {"message": "Status updated from Azure DevOps successfully"}
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to pull status from Azure DevOps"
        )

@router.get("/sync/status/{project_id}")
def get_azure_devops_sync_status(
    project_id: int,
    db: Session = Depends(get_db)
):
    """Get Azure DevOps sync status for all user stories in a project"""
    user_stories = db.query(UserStoryModel).filter(
        UserStoryModel.project_id == project_id
    ).all()
    
    status_summary = {
        "total_stories": len(user_stories),
        "synced": 0,
        "not_synced": 0,
        "sync_failed": 0,
        "manually_linked": 0,
        "stories": []
    }
    
    for story in user_stories:
        story_status = {
            "id": story.id,
            "title": story.title,
            "azure_devops_work_item_id": story.azure_devops_work_item_id,
            "azure_devops_url": story.azure_devops_url,
            "azure_devops_sync_status": story.azure_devops_sync_status,
            "azure_devops_state": story.azure_devops_state,
            "azure_devops_last_sync": story.azure_devops_last_sync
        }
        
        status_summary["stories"].append(story_status)
        
        # Count statuses
        if story.azure_devops_sync_status == "synced":
            status_summary["synced"] += 1
        elif story.azure_devops_sync_status == "sync_failed":
            status_summary["sync_failed"] += 1
        elif story.azure_devops_sync_status == "manually_linked":
            status_summary["manually_linked"] += 1
        else:
            status_summary["not_synced"] += 1
    
    return status_summary

# Background task functions
async def sync_project_stories_background(
    db: Session,
    user_stories: List[UserStoryModel],
    integration: AzureDevOpsIntegrationModel,
    sync_all: bool
):
    """Background task to sync multiple user stories"""
    try:
        for story in user_stories:
            await azure_devops_service.sync_user_story_to_azure_devops(
                db, story, integration, sync_all
            )
    except Exception as e:
        logger.error(f"Error in background sync: {str(e)}")

@router.get("/work-item-types/{project_id}")
async def get_available_work_item_types(
    project_id: int,
    db: Session = Depends(get_db)
):
    """Get available work item types from Azure DevOps"""
    integration = db.query(AzureDevOpsIntegrationModel).filter(
        AzureDevOpsIntegrationModel.project_id == project_id,
        AzureDevOpsIntegrationModel.is_active == True
    ).first()
    
    if not integration:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Azure DevOps integration not found for this project"
        )
    
    # Test connection to get work item types
    connection_test = await azure_devops_service.test_connection(
        integration.organization_url,
        integration.project_name,
        integration.personal_access_token
    )
    
    if connection_test.success:
        return {
            "work_item_types": connection_test.available_work_item_types or [],
            "iterations": connection_test.available_iterations or []
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get work item types: {connection_test.message}"
        )