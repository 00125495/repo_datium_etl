from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.requirements import UserStory as UserStoryModel
from app.schemas.requirements import (
    UserStory,
    UserStoryCreate,
    BulkUserStoryCreate
)

router = APIRouter()

@router.get("/", response_model=List[UserStory])
def get_user_stories(
    skip: int = 0,
    limit: int = 100,
    project_id: Optional[int] = None,
    requirement_id: Optional[int] = None,
    sprint_number: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Get user stories with filtering"""
    query = db.query(UserStoryModel)
    
    if project_id:
        query = query.filter(UserStoryModel.project_id == project_id)
    if requirement_id:
        query = query.filter(UserStoryModel.requirement_id == requirement_id)
    if sprint_number:
        query = query.filter(UserStoryModel.sprint_number == sprint_number)
    
    user_stories = query.offset(skip).limit(limit).all()
    return user_stories

@router.get("/{story_id}", response_model=UserStory)
def get_user_story(
    story_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific user story"""
    story = db.query(UserStoryModel).filter(UserStoryModel.id == story_id).first()
    if not story:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User story not found"
        )
    return story

@router.post("/", response_model=UserStory)
def create_user_story(
    story: UserStoryCreate,
    db: Session = Depends(get_db)
):
    """Create a new user story"""
    db_story = UserStoryModel(**story.dict())
    db.add(db_story)
    db.commit()
    db.refresh(db_story)
    return db_story

@router.post("/bulk", response_model=List[UserStory])
def create_bulk_user_stories(
    bulk_data: BulkUserStoryCreate,
    db: Session = Depends(get_db)
):
    """Create multiple user stories at once"""
    db_stories = []
    for story_data in bulk_data.user_stories:
        db_story = UserStoryModel(**story_data.dict())
        db.add(db_story)
        db_stories.append(db_story)
    
    db.commit()
    for db_story in db_stories:
        db.refresh(db_story)
    
    return db_stories

@router.put("/{story_id}", response_model=UserStory)
def update_user_story(
    story_id: int,
    story: UserStoryCreate,
    db: Session = Depends(get_db)
):
    """Update an existing user story"""
    db_story = db.query(UserStoryModel).filter(UserStoryModel.id == story_id).first()
    if not db_story:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User story not found"
        )
    
    for key, value in story.dict().items():
        setattr(db_story, key, value)
    
    db.commit()
    db.refresh(db_story)
    return db_story

@router.patch("/{story_id}/jira")
def update_jira_integration(
    story_id: int,
    jira_ticket_id: str,
    db: Session = Depends(get_db)
):
    """Update JIRA ticket integration"""
    db_story = db.query(UserStoryModel).filter(UserStoryModel.id == story_id).first()
    if not db_story:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User story not found"
        )
    
    db_story.jira_ticket_id = jira_ticket_id
    db.commit()
    return {"message": f"JIRA ticket {jira_ticket_id} linked to story"}

@router.patch("/{story_id}/github")
def update_github_integration(
    story_id: int,
    github_issue_id: str,
    db: Session = Depends(get_db)
):
    """Update GitHub issue integration"""
    db_story = db.query(UserStoryModel).filter(UserStoryModel.id == story_id).first()
    if not db_story:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User story not found"
        )
    
    db_story.github_issue_id = github_issue_id
    db.commit()
    return {"message": f"GitHub issue {github_issue_id} linked to story"}

@router.delete("/{story_id}")
def delete_user_story(
    story_id: int,
    db: Session = Depends(get_db)
):
    """Delete a user story"""
    db_story = db.query(UserStoryModel).filter(UserStoryModel.id == story_id).first()
    if not db_story:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User story not found"
        )
    
    db.delete(db_story)
    db.commit()
    return {"message": "User story deleted successfully"}

@router.get("/project/{project_id}/sprint/{sprint_number}")
def get_sprint_stories(
    project_id: int,
    sprint_number: int,
    db: Session = Depends(get_db)
):
    """Get all user stories for a specific sprint"""
    stories = db.query(UserStoryModel).filter(
        UserStoryModel.project_id == project_id,
        UserStoryModel.sprint_number == sprint_number
    ).all()
    
    total_points = sum(story.story_points or 0 for story in stories)
    
    return {
        "sprint_number": sprint_number,
        "project_id": project_id,
        "stories": stories,
        "total_story_points": total_points,
        "story_count": len(stories)
    }