from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.requirements import Project as ProjectModel
from app.schemas.requirements import (
    Project,
    ProjectCreate,
    ProjectWithRequirements
)

router = APIRouter()

@router.get("/", response_model=List[Project])
def get_projects(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get all projects with pagination"""
    projects = db.query(ProjectModel).filter(ProjectModel.is_active == True).offset(skip).limit(limit).all()
    return projects

@router.get("/{project_id}", response_model=ProjectWithRequirements)
def get_project(
    project_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific project with its requirements and user stories"""
    project = db.query(ProjectModel).filter(ProjectModel.id == project_id, ProjectModel.is_active == True).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    return project

@router.post("/", response_model=Project)
def create_project(
    project: ProjectCreate,
    db: Session = Depends(get_db)
):
    """Create a new project"""
    db_project = ProjectModel(**project.dict())
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project

@router.put("/{project_id}", response_model=Project)
def update_project(
    project_id: int,
    project: ProjectCreate,
    db: Session = Depends(get_db)
):
    """Update an existing project"""
    db_project = db.query(ProjectModel).filter(ProjectModel.id == project_id, ProjectModel.is_active == True).first()
    if not db_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    for key, value in project.dict().items():
        setattr(db_project, key, value)
    
    db.commit()
    db.refresh(db_project)
    return db_project

@router.delete("/{project_id}")
def delete_project(
    project_id: int,
    db: Session = Depends(get_db)
):
    """Soft delete a project"""
    db_project = db.query(ProjectModel).filter(ProjectModel.id == project_id, ProjectModel.is_active == True).first()
    if not db_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    db_project.is_active = False
    db.commit()
    return {"message": "Project deleted successfully"}