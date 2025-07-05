from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from app.core.database import get_db
from app.models.requirements import (
    Requirement as RequirementModel,
    RequirementType,
    RequirementStatus,
    Priority
)
from app.schemas.requirements import (
    Requirement,
    RequirementCreate,
    RequirementWithUserStories,
    RequirementFilter,
    PaginatedResponse,
    BulkRequirementCreate
)

router = APIRouter()

@router.get("/", response_model=PaginatedResponse)
def get_requirements(
    skip: int = 0,
    limit: int = 100,
    project_id: Optional[int] = None,
    requirement_type: Optional[RequirementType] = None,
    status: Optional[RequirementStatus] = None,
    priority: Optional[Priority] = None,
    min_ai_score: Optional[float] = None,
    search_query: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get requirements with filtering and pagination"""
    query = db.query(RequirementModel)
    
    # Apply filters
    filters = []
    if project_id:
        filters.append(RequirementModel.project_id == project_id)
    if requirement_type:
        filters.append(RequirementModel.type == requirement_type)
    if status:
        filters.append(RequirementModel.status == status)
    if priority:
        filters.append(RequirementModel.priority == priority)
    if min_ai_score is not None:
        filters.append(RequirementModel.ai_score >= min_ai_score)
    if search_query:
        filters.append(
            or_(
                RequirementModel.title.ilike(f"%{search_query}%"),
                RequirementModel.description.ilike(f"%{search_query}%")
            )
        )
    
    if filters:
        query = query.filter(and_(*filters))
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    requirements = query.offset(skip).limit(limit).all()
    
    return PaginatedResponse(
        items=requirements,
        total=total,
        page=skip // limit + 1,
        per_page=limit,
        total_pages=(total + limit - 1) // limit
    )

@router.get("/{requirement_id}", response_model=RequirementWithUserStories)
def get_requirement(
    requirement_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific requirement with user stories"""
    requirement = db.query(RequirementModel).filter(RequirementModel.id == requirement_id).first()
    if not requirement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requirement not found"
        )
    return requirement

@router.post("/", response_model=Requirement)
def create_requirement(
    requirement: RequirementCreate,
    db: Session = Depends(get_db)
):
    """Create a new requirement"""
    db_requirement = RequirementModel(**requirement.dict())
    db.add(db_requirement)
    db.commit()
    db.refresh(db_requirement)
    return db_requirement

@router.post("/bulk", response_model=List[Requirement])
def create_bulk_requirements(
    bulk_data: BulkRequirementCreate,
    db: Session = Depends(get_db)
):
    """Create multiple requirements at once"""
    db_requirements = []
    for req_data in bulk_data.requirements:
        db_requirement = RequirementModel(**req_data.dict())
        db.add(db_requirement)
        db_requirements.append(db_requirement)
    
    db.commit()
    for db_req in db_requirements:
        db.refresh(db_req)
    
    return db_requirements

@router.put("/{requirement_id}", response_model=Requirement)
def update_requirement(
    requirement_id: int,
    requirement: RequirementCreate,
    db: Session = Depends(get_db)
):
    """Update an existing requirement"""
    db_requirement = db.query(RequirementModel).filter(RequirementModel.id == requirement_id).first()
    if not db_requirement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requirement not found"
        )
    
    for key, value in requirement.dict().items():
        setattr(db_requirement, key, value)
    
    db.commit()
    db.refresh(db_requirement)
    return db_requirement

@router.patch("/{requirement_id}/status")
def update_requirement_status(
    requirement_id: int,
    status: RequirementStatus,
    db: Session = Depends(get_db)
):
    """Update requirement status"""
    db_requirement = db.query(RequirementModel).filter(RequirementModel.id == requirement_id).first()
    if not db_requirement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requirement not found"
        )
    
    db_requirement.status = status
    db.commit()
    db.refresh(db_requirement)
    return {"message": f"Requirement status updated to {status}"}

@router.delete("/{requirement_id}")
def delete_requirement(
    requirement_id: int,
    db: Session = Depends(get_db)
):
    """Delete a requirement"""
    db_requirement = db.query(RequirementModel).filter(RequirementModel.id == requirement_id).first()
    if not db_requirement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requirement not found"
        )
    
    db.delete(db_requirement)
    db.commit()
    return {"message": "Requirement deleted successfully"}

@router.get("/project/{project_id}/summary")
def get_project_requirements_summary(
    project_id: int,
    db: Session = Depends(get_db)
):
    """Get summary statistics for project requirements"""
    requirements = db.query(RequirementModel).filter(RequirementModel.project_id == project_id).all()
    
    summary = {
        "total_requirements": len(requirements),
        "by_type": {},
        "by_status": {},
        "by_priority": {},
        "average_ai_score": 0,
        "validated_requirements": 0
    }
    
    ai_scores = []
    for req in requirements:
        # Count by type
        req_type = req.type
        summary["by_type"][req_type] = summary["by_type"].get(req_type, 0) + 1
        
        # Count by status
        req_status = req.status
        summary["by_status"][req_status] = summary["by_status"].get(req_status, 0) + 1
        
        # Count by priority
        req_priority = req.priority
        summary["by_priority"][req_priority] = summary["by_priority"].get(req_priority, 0) + 1
        
        # AI score tracking
        if req.ai_score is not None:
            ai_scores.append(req.ai_score)
            if req.ai_score >= 0.7:
                summary["validated_requirements"] += 1
    
    if ai_scores:
        summary["average_ai_score"] = sum(ai_scores) / len(ai_scores)
    
    return summary