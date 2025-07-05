from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Float
from sqlalchemy.relationship import relationship
from sqlalchemy.sql import func
from app.core.database import Base
from enum import Enum


class RequirementType(str, Enum):
    DATA_PLATFORM = "data_platform"
    KPI = "kpi"
    SOFTWARE = "software"


class RequirementStatus(str, Enum):
    DRAFT = "draft"
    REVIEW = "review"
    APPROVED = "approved"
    REJECTED = "rejected"


class Priority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Project(Base):
    __tablename__ = "projects"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    
    # Relationships
    requirements = relationship("Requirement", back_populates="project")
    user_stories = relationship("UserStory", back_populates="project")


class Requirement(Base):
    __tablename__ = "requirements"
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    type = Column(String(50), nullable=False)  # RequirementType
    status = Column(String(50), default=RequirementStatus.DRAFT)
    priority = Column(String(50), default=Priority.MEDIUM)
    
    # Project reference
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    
    # Specific fields for different requirement types
    business_value = Column(Text)
    acceptance_criteria = Column(Text)
    technical_requirements = Column(JSON)
    dependencies = Column(JSON)
    estimated_effort = Column(Float)  # In story points or hours
    
    # AI feedback
    ai_feedback = Column(JSON)
    ai_score = Column(Float)  # AI validation score 0-1
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    project = relationship("Project", back_populates="requirements")
    user_stories = relationship("UserStory", back_populates="requirement")


class UserStory(Base):
    __tablename__ = "user_stories"
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    acceptance_criteria = Column(Text)
    
    # Story details
    story_points = Column(Integer)
    priority = Column(String(50), default=Priority.MEDIUM)
    sprint_number = Column(Integer)
    epic = Column(String(255))
    
    # References
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    requirement_id = Column(Integer, ForeignKey("requirements.id"), nullable=True)
    
    # DevOps integration
    jira_ticket_id = Column(String(100))
    github_issue_id = Column(String(100))
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    project = relationship("Project", back_populates="user_stories")
    requirement = relationship("Requirement", back_populates="user_stories")


class AIFeedback(Base):
    __tablename__ = "ai_feedback"
    
    id = Column(Integer, primary_key=True, index=True)
    requirement_id = Column(Integer, ForeignKey("requirements.id"), nullable=False)
    
    # Feedback details
    feedback_type = Column(String(50))  # validation, suggestion, improvement
    message = Column(Text, nullable=False)
    severity = Column(String(50))  # info, warning, error
    score = Column(Float)  # 0-1 confidence score
    
    # AI model info
    model_used = Column(String(100))
    model_version = Column(String(50))
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    requirement = relationship("Requirement")