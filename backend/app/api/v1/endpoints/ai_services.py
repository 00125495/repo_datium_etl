from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.requirements import Requirement as RequirementModel, AIFeedback as AIFeedbackModel
from app.schemas.requirements import (
    AIValidationRequest,
    AIValidationResponse,
    UserStoryGenerationRequest,
    UserStoryGenerationResponse,
    UserStoryCreate
)
from app.ai.ai_service import ai_service

router = APIRouter()

@router.post("/validate", response_model=AIValidationResponse)
async def validate_requirement(
    request: AIValidationRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Validate a requirement using AI"""
    # Get the requirement
    requirement = db.query(RequirementModel).filter(RequirementModel.id == request.requirement_id).first()
    if not requirement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requirement not found"
        )
    
    # Check if we need to revalidate
    if not request.force_revalidation and requirement.ai_score is not None:
        # Return existing validation if available
        existing_feedback = db.query(AIFeedbackModel).filter(
            AIFeedbackModel.requirement_id == request.requirement_id
        ).all()
        
        if existing_feedback:
            return AIValidationResponse(
                requirement_id=requirement.id,
                overall_score=requirement.ai_score,
                feedback=existing_feedback,
                suggestions=requirement.ai_feedback.get("suggestions", []) if requirement.ai_feedback else [],
                is_valid=requirement.ai_score >= 0.7
            )
    
    # Perform AI validation
    validation_result = await ai_service.validate_requirement(requirement)
    
    # Save validation results in background
    background_tasks.add_task(
        save_ai_validation_results,
        db,
        requirement.id,
        validation_result
    )
    
    return validation_result

@router.post("/generate-stories", response_model=UserStoryGenerationResponse)
async def generate_user_stories(
    request: UserStoryGenerationRequest,
    db: Session = Depends(get_db)
):
    """Generate user stories from a requirement using AI"""
    # Get the requirement
    requirement = db.query(RequirementModel).filter(RequirementModel.id == request.requirement_id).first()
    if not requirement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requirement not found"
        )
    
    # Generate user stories
    generation_result = await ai_service.generate_user_stories(
        requirement=requirement,
        sprint_number=request.sprint_number,
        max_stories=request.max_stories,
        include_technical_tasks=request.include_technical_tasks
    )
    
    return generation_result

@router.post("/generate-and-save-stories")
async def generate_and_save_user_stories(
    request: UserStoryGenerationRequest,
    db: Session = Depends(get_db)
):
    """Generate user stories and save them to the database"""
    # Get the requirement
    requirement = db.query(RequirementModel).filter(RequirementModel.id == request.requirement_id).first()
    if not requirement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requirement not found"
        )
    
    # Generate user stories
    generation_result = await ai_service.generate_user_stories(
        requirement=requirement,
        sprint_number=request.sprint_number,
        max_stories=request.max_stories,
        include_technical_tasks=request.include_technical_tasks
    )
    
    # Save generated stories to database
    from app.models.requirements import UserStory as UserStoryModel
    
    saved_stories = []
    for story_data in generation_result.generated_stories:
        db_story = UserStoryModel(**story_data.dict())
        db.add(db_story)
        saved_stories.append(db_story)
    
    db.commit()
    for story in saved_stories:
        db.refresh(story)
    
    return {
        "message": f"Generated and saved {len(saved_stories)} user stories",
        "requirement_id": requirement.id,
        "sprint_number": request.sprint_number,
        "saved_stories": saved_stories,
        "total_story_points": generation_result.total_estimated_points,
        "recommendations": generation_result.recommendations
    }

@router.get("/validation-history/{requirement_id}")
def get_validation_history(
    requirement_id: int,
    db: Session = Depends(get_db)
):
    """Get AI validation history for a requirement"""
    feedback_history = db.query(AIFeedbackModel).filter(
        AIFeedbackModel.requirement_id == requirement_id
    ).order_by(AIFeedbackModel.created_at.desc()).all()
    
    return {
        "requirement_id": requirement_id,
        "feedback_history": feedback_history,
        "total_validations": len(feedback_history)
    }

@router.post("/batch-validate")
async def batch_validate_requirements(
    project_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Validate all requirements in a project"""
    requirements = db.query(RequirementModel).filter(
        RequirementModel.project_id == project_id
    ).all()
    
    if not requirements:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No requirements found for this project"
        )
    
    # Queue validation tasks
    for requirement in requirements:
        background_tasks.add_task(
            validate_requirement_background,
            db,
            requirement.id
        )
    
    return {
        "message": f"Queued validation for {len(requirements)} requirements",
        "project_id": project_id,
        "requirements_count": len(requirements)
    }

@router.get("/ai-insights/{project_id}")
def get_ai_insights(
    project_id: int,
    db: Session = Depends(get_db)
):
    """Get AI insights and analytics for a project"""
    requirements = db.query(RequirementModel).filter(
        RequirementModel.project_id == project_id
    ).all()
    
    if not requirements:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No requirements found for this project"
        )
    
    # Calculate insights
    total_requirements = len(requirements)
    validated_requirements = sum(1 for req in requirements if req.ai_score is not None)
    high_quality_requirements = sum(1 for req in requirements if req.ai_score and req.ai_score >= 0.8)
    low_quality_requirements = sum(1 for req in requirements if req.ai_score and req.ai_score < 0.5)
    
    average_score = 0
    if validated_requirements > 0:
        scores = [req.ai_score for req in requirements if req.ai_score is not None]
        average_score = sum(scores) / len(scores)
    
    # Get common feedback themes
    all_feedback = db.query(AIFeedbackModel).join(RequirementModel).filter(
        RequirementModel.project_id == project_id
    ).all()
    
    feedback_themes = {}
    for feedback in all_feedback:
        theme = feedback.feedback_type
        feedback_themes[theme] = feedback_themes.get(theme, 0) + 1
    
    return {
        "project_id": project_id,
        "total_requirements": total_requirements,
        "validated_requirements": validated_requirements,
        "validation_coverage": validated_requirements / total_requirements if total_requirements > 0 else 0,
        "average_ai_score": average_score,
        "high_quality_requirements": high_quality_requirements,
        "low_quality_requirements": low_quality_requirements,
        "feedback_themes": feedback_themes,
        "recommendations": [
            "Focus on improving requirements with AI scores below 0.5",
            "Ensure all requirements have proper acceptance criteria",
            "Consider adding business value justification to requirements",
            "Review and update requirements based on AI feedback"
        ]
    }

# Background task functions
def save_ai_validation_results(db: Session, requirement_id: int, validation_result: AIValidationResponse):
    """Save AI validation results to database"""
    try:
        # Update requirement with AI score and feedback
        requirement = db.query(RequirementModel).filter(RequirementModel.id == requirement_id).first()
        if requirement:
            requirement.ai_score = validation_result.overall_score
            requirement.ai_feedback = {
                "suggestions": validation_result.suggestions,
                "is_valid": validation_result.is_valid,
                "last_validated": str(requirement.updated_at)
            }
            
            # Save individual feedback items
            for feedback in validation_result.feedback:
                db_feedback = AIFeedbackModel(
                    requirement_id=requirement_id,
                    feedback_type=feedback.feedback_type,
                    message=feedback.message,
                    severity=feedback.severity,
                    score=feedback.score,
                    model_used=feedback.model_used
                )
                db.add(db_feedback)
            
            db.commit()
    except Exception as e:
        db.rollback()
        print(f"Error saving AI validation results: {str(e)}")

async def validate_requirement_background(db: Session, requirement_id: int):
    """Background task to validate a requirement"""
    try:
        requirement = db.query(RequirementModel).filter(RequirementModel.id == requirement_id).first()
        if requirement:
            validation_result = await ai_service.validate_requirement(requirement)
            save_ai_validation_results(db, requirement_id, validation_result)
    except Exception as e:
        print(f"Error in background validation for requirement {requirement_id}: {str(e)}")