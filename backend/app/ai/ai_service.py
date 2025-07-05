from typing import List, Dict, Any, Optional
import json
import asyncio
from openai import AsyncOpenAI
from anthropic import AsyncAnthropic
from app.core.config import settings
from app.schemas.requirements import (
    AIValidationResponse, 
    AIFeedbackResponse, 
    UserStoryGenerationResponse,
    UserStoryCreate,
    Requirement
)
from app.models.requirements import RequirementType, Priority
import logging

logger = logging.getLogger(__name__)


class AIService:
    def __init__(self):
        self.openai_client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY) if settings.OPENAI_API_KEY else None
        self.anthropic_client = AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY) if settings.ANTHROPIC_API_KEY else None
    
    async def validate_requirement(self, requirement: Requirement) -> AIValidationResponse:
        """
        Validate a requirement using AI and provide feedback
        """
        try:
            # Prepare the requirement context
            context = self._prepare_requirement_context(requirement)
            
            # Get AI feedback
            if self.openai_client:
                feedback = await self._validate_with_openai(context)
            elif self.anthropic_client:
                feedback = await self._validate_with_anthropic(context)
            else:
                # Fallback to rule-based validation
                feedback = self._rule_based_validation(requirement)
            
            # Calculate overall score
            overall_score = self._calculate_overall_score(feedback)
            
            # Convert to response format
            feedback_responses = [
                AIFeedbackResponse(
                    id=0,  # Will be set when saved to DB
                    feedback_type=f["type"],
                    message=f["message"],
                    severity=f["severity"],
                    score=f.get("score", 0.5),
                    model_used=feedback.get("model_used", "rule-based"),
                    created_at=None  # Will be set when saved to DB
                )
                for f in feedback.get("feedback_items", [])
            ]
            
            return AIValidationResponse(
                requirement_id=requirement.id,
                overall_score=overall_score,
                feedback=feedback_responses,
                suggestions=feedback.get("suggestions", []),
                is_valid=overall_score >= 0.7
            )
            
        except Exception as e:
            logger.error(f"Error validating requirement {requirement.id}: {str(e)}")
            return self._create_error_response(requirement.id, str(e))
    
    async def generate_user_stories(
        self, 
        requirement: Requirement, 
        sprint_number: int,
        max_stories: int = 5,
        include_technical_tasks: bool = True
    ) -> UserStoryGenerationResponse:
        """
        Generate user stories from a requirement
        """
        try:
            # Prepare context
            context = self._prepare_requirement_context(requirement)
            context.update({
                "sprint_number": sprint_number,
                "max_stories": max_stories,
                "include_technical_tasks": include_technical_tasks
            })
            
            # Generate stories
            if self.openai_client:
                stories_data = await self._generate_stories_with_openai(context)
            elif self.anthropic_client:
                stories_data = await self._generate_stories_with_anthropic(context)
            else:
                # Fallback to template-based generation
                stories_data = self._template_based_story_generation(requirement, sprint_number, max_stories)
            
            # Convert to UserStoryCreate objects
            generated_stories = []
            total_points = 0
            
            for story_data in stories_data.get("stories", []):
                story = UserStoryCreate(
                    title=story_data["title"],
                    description=story_data["description"],
                    acceptance_criteria=story_data.get("acceptance_criteria"),
                    story_points=story_data.get("story_points", 3),
                    priority=Priority(story_data.get("priority", "medium")),
                    sprint_number=sprint_number,
                    epic=story_data.get("epic"),
                    project_id=requirement.project_id,
                    requirement_id=requirement.id
                )
                generated_stories.append(story)
                total_points += story.story_points or 0
            
            return UserStoryGenerationResponse(
                requirement_id=requirement.id,
                generated_stories=generated_stories,
                total_estimated_points=total_points,
                recommendations=stories_data.get("recommendations", [])
            )
            
        except Exception as e:
            logger.error(f"Error generating user stories for requirement {requirement.id}: {str(e)}")
            return UserStoryGenerationResponse(
                requirement_id=requirement.id,
                generated_stories=[],
                total_estimated_points=0,
                recommendations=[f"Error generating stories: {str(e)}"]
            )
    
    def _prepare_requirement_context(self, requirement: Requirement) -> Dict[str, Any]:
        """Prepare requirement context for AI processing"""
        return {
            "id": requirement.id,
            "title": requirement.title,
            "description": requirement.description,
            "type": requirement.type,
            "priority": requirement.priority,
            "business_value": requirement.business_value,
            "acceptance_criteria": requirement.acceptance_criteria,
            "technical_requirements": requirement.technical_requirements,
            "dependencies": requirement.dependencies,
            "estimated_effort": requirement.estimated_effort
        }
    
    async def _validate_with_openai(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Validate requirement using OpenAI"""
        prompt = self._create_validation_prompt(context)
        
        response = await self.openai_client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": "You are an expert requirements analyst. Analyze the requirement and provide structured feedback."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=2000
        )
        
        try:
            result = json.loads(response.choices[0].message.content)
            result["model_used"] = "gpt-4-turbo-preview"
            return result
        except json.JSONDecodeError:
            return self._parse_unstructured_response(response.choices[0].message.content, "gpt-4-turbo-preview")
    
    async def _validate_with_anthropic(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Validate requirement using Anthropic Claude"""
        prompt = self._create_validation_prompt(context)
        
        response = await self.anthropic_client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=2000,
            temperature=0.1,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        try:
            result = json.loads(response.content[0].text)
            result["model_used"] = "claude-3-sonnet"
            return result
        except json.JSONDecodeError:
            return self._parse_unstructured_response(response.content[0].text, "claude-3-sonnet")
    
    async def _generate_stories_with_openai(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate user stories using OpenAI"""
        prompt = self._create_story_generation_prompt(context)
        
        response = await self.openai_client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": "You are an expert agile coach. Generate well-structured user stories from requirements."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=3000
        )
        
        try:
            return json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            return self._parse_story_response(response.choices[0].message.content)
    
    async def _generate_stories_with_anthropic(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate user stories using Anthropic Claude"""
        prompt = self._create_story_generation_prompt(context)
        
        response = await self.anthropic_client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=3000,
            temperature=0.3,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        try:
            return json.loads(response.content[0].text)
        except json.JSONDecodeError:
            return self._parse_story_response(response.content[0].text)
    
    def _create_validation_prompt(self, context: Dict[str, Any]) -> str:
        """Create validation prompt for AI"""
        return f"""
        Analyze the following requirement and provide structured feedback:

        **Requirement Details:**
        - Title: {context['title']}
        - Description: {context['description']}
        - Type: {context['type']}
        - Priority: {context['priority']}
        - Business Value: {context.get('business_value', 'Not specified')}
        - Acceptance Criteria: {context.get('acceptance_criteria', 'Not specified')}
        - Technical Requirements: {context.get('technical_requirements', 'Not specified')}
        - Dependencies: {context.get('dependencies', 'Not specified')}
        - Estimated Effort: {context.get('estimated_effort', 'Not specified')}

        Please provide your analysis in the following JSON format:
        {{
            "feedback_items": [
                {{
                    "type": "validation|suggestion|improvement",
                    "message": "Detailed feedback message",
                    "severity": "info|warning|error",
                    "score": 0.8
                }}
            ],
            "suggestions": ["Suggestion 1", "Suggestion 2"],
            "overall_assessment": "Summary of the requirement quality"
        }}

        Focus on:
        1. Clarity and completeness of the requirement
        2. Business value alignment
        3. Technical feasibility
        4. Testability and acceptance criteria
        5. Dependencies and risk factors
        """
    
    def _create_story_generation_prompt(self, context: Dict[str, Any]) -> str:
        """Create user story generation prompt"""
        return f"""
        Generate user stories from the following requirement:

        **Requirement Details:**
        - Title: {context['title']}
        - Description: {context['description']}
        - Type: {context['type']}
        - Priority: {context['priority']}
        - Business Value: {context.get('business_value', 'Not specified')}
        - Acceptance Criteria: {context.get('acceptance_criteria', 'Not specified')}
        - Technical Requirements: {context.get('technical_requirements', 'Not specified')}

        **Generation Parameters:**
        - Sprint Number: {context['sprint_number']}
        - Max Stories: {context['max_stories']}
        - Include Technical Tasks: {context['include_technical_tasks']}

        Please provide user stories in the following JSON format:
        {{
            "stories": [
                {{
                    "title": "As a [user], I want [goal] so that [benefit]",
                    "description": "Detailed description of the story",
                    "acceptance_criteria": "Given...When...Then...",
                    "story_points": 3,
                    "priority": "medium",
                    "epic": "Epic name if applicable"
                }}
            ],
            "recommendations": ["Recommendation 1", "Recommendation 2"]
        }}

        Focus on:
        1. Clear user-centric stories
        2. Proper acceptance criteria
        3. Appropriate story point estimation
        4. Logical breakdown of the requirement
        5. Technical tasks if requested
        """
    
    def _rule_based_validation(self, requirement: Requirement) -> Dict[str, Any]:
        """Fallback rule-based validation"""
        feedback_items = []
        
        # Check title length
        if len(requirement.title) < 10:
            feedback_items.append({
                "type": "validation",
                "message": "Title is too short. Consider adding more descriptive information.",
                "severity": "warning",
                "score": 0.6
            })
        
        # Check description length
        if len(requirement.description) < 50:
            feedback_items.append({
                "type": "validation",
                "message": "Description is too brief. Add more details about the requirement.",
                "severity": "warning",
                "score": 0.5
            })
        
        # Check acceptance criteria
        if not requirement.acceptance_criteria:
            feedback_items.append({
                "type": "validation",
                "message": "Acceptance criteria is missing. This is crucial for testing.",
                "severity": "error",
                "score": 0.3
            })
        
        # Check business value
        if not requirement.business_value:
            feedback_items.append({
                "type": "suggestion",
                "message": "Consider adding business value to justify the requirement.",
                "severity": "info",
                "score": 0.7
            })
        
        return {
            "feedback_items": feedback_items,
            "suggestions": [
                "Add more specific acceptance criteria",
                "Include business value justification",
                "Consider potential risks and dependencies"
            ],
            "overall_assessment": "Basic validation completed",
            "model_used": "rule-based"
        }
    
    def _template_based_story_generation(self, requirement: Requirement, sprint_number: int, max_stories: int) -> Dict[str, Any]:
        """Fallback template-based story generation"""
        stories = []
        
        # Generate basic stories based on requirement type
        if requirement.type == RequirementType.DATA_PLATFORM:
            stories.append({
                "title": f"As a data engineer, I want to {requirement.title.lower()} so that data can be processed efficiently",
                "description": f"Implementation of {requirement.description}",
                "acceptance_criteria": "Given the data platform requirements, When implemented, Then data processing should work as expected",
                "story_points": 8,
                "priority": "high",
                "epic": "Data Platform"
            })
        elif requirement.type == RequirementType.KPI:
            stories.append({
                "title": f"As a business user, I want to track {requirement.title.lower()} so that I can measure performance",
                "description": f"KPI implementation: {requirement.description}",
                "acceptance_criteria": "Given the KPI requirements, When implemented, Then metrics should be accurately tracked",
                "story_points": 5,
                "priority": "medium",
                "epic": "Analytics"
            })
        elif requirement.type == RequirementType.SOFTWARE:
            stories.append({
                "title": f"As a user, I want {requirement.title.lower()} so that I can achieve my goals",
                "description": f"Software feature: {requirement.description}",
                "acceptance_criteria": "Given the software requirements, When implemented, Then functionality should work as specified",
                "story_points": 5,
                "priority": "medium",
                "epic": "Software Development"
            })
        
        return {
            "stories": stories[:max_stories],
            "recommendations": [
                "Consider breaking down large stories into smaller ones",
                "Add more specific acceptance criteria",
                "Review story point estimates with the team"
            ]
        }
    
    def _calculate_overall_score(self, feedback: Dict[str, Any]) -> float:
        """Calculate overall validation score"""
        feedback_items = feedback.get("feedback_items", [])
        if not feedback_items:
            return 0.5
        
        total_score = sum(item.get("score", 0.5) for item in feedback_items)
        return total_score / len(feedback_items)
    
    def _create_error_response(self, requirement_id: int, error_msg: str) -> AIValidationResponse:
        """Create error response for validation failures"""
        return AIValidationResponse(
            requirement_id=requirement_id,
            overall_score=0.0,
            feedback=[
                AIFeedbackResponse(
                    id=0,
                    feedback_type="error",
                    message=f"AI validation failed: {error_msg}",
                    severity="error",
                    score=0.0,
                    model_used="system",
                    created_at=None
                )
            ],
            suggestions=["Please check the requirement and try again"],
            is_valid=False
        )
    
    def _parse_unstructured_response(self, response: str, model: str) -> Dict[str, Any]:
        """Parse unstructured AI response"""
        return {
            "feedback_items": [
                {
                    "type": "validation",
                    "message": response,
                    "severity": "info",
                    "score": 0.5
                }
            ],
            "suggestions": ["AI response was unstructured"],
            "overall_assessment": "Unstructured response received",
            "model_used": model
        }
    
    def _parse_story_response(self, response: str) -> Dict[str, Any]:
        """Parse unstructured story generation response"""
        return {
            "stories": [
                {
                    "title": "Generated from unstructured response",
                    "description": response,
                    "acceptance_criteria": "To be defined",
                    "story_points": 3,
                    "priority": "medium",
                    "epic": "Generated"
                }
            ],
            "recommendations": ["Response was unstructured, please review generated content"]
        }


# Create global instance
ai_service = AIService()