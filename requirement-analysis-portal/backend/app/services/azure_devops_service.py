import base64
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import httpx
from sqlalchemy.orm import Session
from app.models.requirements import (
    UserStory as UserStoryModel,
    AzureDevOpsIntegration as AzureDevOpsIntegrationModel,
    Project as ProjectModel
)
from app.schemas.requirements import (
    AzureDevOpsWorkItemCreate,
    AzureDevOpsWorkItemUpdate,
    AzureDevOpsSyncResponse,
    AzureDevOpsConnectionTestResponse
)

logger = logging.getLogger(__name__)


class AzureDevOpsService:
    def __init__(self):
        self.api_version = "7.0"
        self.timeout = 30.0

    def _get_auth_header(self, personal_access_token: str) -> str:
        """Create basic auth header for Azure DevOps API"""
        credentials = f":{personal_access_token}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded_credentials}"

    def _get_headers(self, personal_access_token: str) -> Dict[str, str]:
        """Get standard headers for Azure DevOps API requests"""
        return {
            "Authorization": self._get_auth_header(personal_access_token),
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    async def test_connection(
        self, 
        organization_url: str, 
        project_name: str, 
        personal_access_token: str
    ) -> AzureDevOpsConnectionTestResponse:
        """Test connection to Azure DevOps and get project information"""
        try:
            # Clean organization URL
            if not organization_url.startswith('https://'):
                organization_url = f"https://{organization_url}"
            if not organization_url.endswith('/'):
                organization_url += '/'

            headers = self._get_headers(personal_access_token)
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Test project access
                project_url = f"{organization_url}_apis/projects/{project_name}?api-version={self.api_version}"
                project_response = await client.get(project_url, headers=headers)
                
                if project_response.status_code != 200:
                    return AzureDevOpsConnectionTestResponse(
                        success=False,
                        message=f"Failed to access project: {project_response.status_code} - {project_response.text}"
                    )

                project_info = project_response.json()

                # Get work item types
                wit_url = f"{organization_url}{project_name}/_apis/wit/workitemtypes?api-version={self.api_version}"
                wit_response = await client.get(wit_url, headers=headers)
                
                work_item_types = []
                if wit_response.status_code == 200:
                    wit_data = wit_response.json()
                    work_item_types = [wit["name"] for wit in wit_data.get("value", [])]

                # Get iterations
                iterations_url = f"{organization_url}{project_name}/_apis/wit/classificationnodes/iterations?$depth=2&api-version={self.api_version}"
                iterations_response = await client.get(iterations_url, headers=headers)
                
                iterations = []
                if iterations_response.status_code == 200:
                    iterations_data = iterations_response.json()
                    iterations = self._extract_iteration_paths(iterations_data.get("value", []))

                return AzureDevOpsConnectionTestResponse(
                    success=True,
                    message="Connection successful",
                    project_info={
                        "id": project_info.get("id"),
                        "name": project_info.get("name"),
                        "description": project_info.get("description"),
                        "url": project_info.get("url")
                    },
                    available_work_item_types=work_item_types,
                    available_iterations=iterations
                )

        except httpx.TimeoutException:
            return AzureDevOpsConnectionTestResponse(
                success=False,
                message="Connection timeout. Please check your organization URL and network connection."
            )
        except Exception as e:
            logger.error(f"Azure DevOps connection test failed: {str(e)}")
            return AzureDevOpsConnectionTestResponse(
                success=False,
                message=f"Connection failed: {str(e)}"
            )

    def _extract_iteration_paths(self, iterations: List[Dict]) -> List[str]:
        """Extract iteration paths from Azure DevOps iterations response"""
        paths = []
        for iteration in iterations:
            path = iteration.get("path", "")
            if path:
                paths.append(path)
            
            # Recursively get child iterations
            children = iteration.get("children", [])
            if children:
                paths.extend(self._extract_iteration_paths(children))
        
        return paths

    async def create_work_item(
        self,
        integration: AzureDevOpsIntegrationModel,
        work_item_data: AzureDevOpsWorkItemCreate
    ) -> Dict[str, Any]:
        """Create a work item in Azure DevOps"""
        try:
            headers = self._get_headers(integration.personal_access_token)
            headers["Content-Type"] = "application/json-patch+json"

            # Build work item fields
            fields = [
                {
                    "op": "add",
                    "path": "/fields/System.Title",
                    "value": work_item_data.title
                },
                {
                    "op": "add",
                    "path": "/fields/System.Description",
                    "value": work_item_data.description
                }
            ]

            # Add optional fields
            if work_item_data.assigned_to:
                fields.append({
                    "op": "add",
                    "path": "/fields/System.AssignedTo",
                    "value": work_item_data.assigned_to
                })

            if work_item_data.area_path:
                fields.append({
                    "op": "add",
                    "path": "/fields/System.AreaPath",
                    "value": work_item_data.area_path
                })

            if work_item_data.iteration_path:
                fields.append({
                    "op": "add",
                    "path": "/fields/System.IterationPath",
                    "value": work_item_data.iteration_path
                })

            if work_item_data.story_points:
                fields.append({
                    "op": "add",
                    "path": "/fields/Microsoft.VSTS.Scheduling.StoryPoints",
                    "value": work_item_data.story_points
                })

            if work_item_data.priority:
                fields.append({
                    "op": "add",
                    "path": "/fields/Microsoft.VSTS.Common.Priority",
                    "value": work_item_data.priority
                })

            if work_item_data.tags:
                tags_string = "; ".join(work_item_data.tags)
                fields.append({
                    "op": "add",
                    "path": "/fields/System.Tags",
                    "value": tags_string
                })

            url = f"{integration.organization_url}{integration.project_name}/_apis/wit/workitems/${work_item_data.work_item_type}?api-version={self.api_version}"

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, headers=headers, json=fields)
                
                if response.status_code in [200, 201]:
                    work_item = response.json()
                    return {
                        "success": True,
                        "work_item_id": work_item["id"],
                        "work_item_url": work_item["_links"]["html"]["href"],
                        "work_item": work_item
                    }
                else:
                    logger.error(f"Failed to create work item: {response.status_code} - {response.text}")
                    return {
                        "success": False,
                        "error": f"Failed to create work item: {response.status_code} - {response.text}"
                    }

        except Exception as e:
            logger.error(f"Error creating work item: {str(e)}")
            return {
                "success": False,
                "error": f"Error creating work item: {str(e)}"
            }

    async def update_work_item(
        self,
        integration: AzureDevOpsIntegrationModel,
        work_item_id: int,
        update_data: AzureDevOpsWorkItemUpdate
    ) -> Dict[str, Any]:
        """Update an existing work item in Azure DevOps"""
        try:
            headers = self._get_headers(integration.personal_access_token)
            headers["Content-Type"] = "application/json-patch+json"

            fields = []

            # Build update fields
            if update_data.title:
                fields.append({
                    "op": "add",
                    "path": "/fields/System.Title",
                    "value": update_data.title
                })

            if update_data.description:
                fields.append({
                    "op": "add",
                    "path": "/fields/System.Description",
                    "value": update_data.description
                })

            if update_data.state:
                fields.append({
                    "op": "add",
                    "path": "/fields/System.State",
                    "value": update_data.state
                })

            if update_data.assigned_to:
                fields.append({
                    "op": "add",
                    "path": "/fields/System.AssignedTo",
                    "value": update_data.assigned_to
                })

            if update_data.story_points is not None:
                fields.append({
                    "op": "add",
                    "path": "/fields/Microsoft.VSTS.Scheduling.StoryPoints",
                    "value": update_data.story_points
                })

            if update_data.priority:
                fields.append({
                    "op": "add",
                    "path": "/fields/Microsoft.VSTS.Common.Priority",
                    "value": update_data.priority
                })

            if update_data.tags:
                tags_string = "; ".join(update_data.tags)
                fields.append({
                    "op": "add",
                    "path": "/fields/System.Tags",
                    "value": tags_string
                })

            if not fields:
                return {
                    "success": False,
                    "error": "No fields to update"
                }

            url = f"{integration.organization_url}_apis/wit/workitems/{work_item_id}?api-version={self.api_version}"

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.patch(url, headers=headers, json=fields)
                
                if response.status_code == 200:
                    work_item = response.json()
                    return {
                        "success": True,
                        "work_item": work_item
                    }
                else:
                    logger.error(f"Failed to update work item: {response.status_code} - {response.text}")
                    return {
                        "success": False,
                        "error": f"Failed to update work item: {response.status_code} - {response.text}"
                    }

        except Exception as e:
            logger.error(f"Error updating work item: {str(e)}")
            return {
                "success": False,
                "error": f"Error updating work item: {str(e)}"
            }

    async def get_work_item(
        self,
        integration: AzureDevOpsIntegrationModel,
        work_item_id: int
    ) -> Dict[str, Any]:
        """Get work item details from Azure DevOps"""
        try:
            headers = self._get_headers(integration.personal_access_token)
            url = f"{integration.organization_url}_apis/wit/workitems/{work_item_id}?api-version={self.api_version}"

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, headers=headers)
                
                if response.status_code == 200:
                    return {
                        "success": True,
                        "work_item": response.json()
                    }
                else:
                    return {
                        "success": False,
                        "error": f"Failed to get work item: {response.status_code} - {response.text}"
                    }

        except Exception as e:
            logger.error(f"Error getting work item: {str(e)}")
            return {
                "success": False,
                "error": f"Error getting work item: {str(e)}"
            }

    async def sync_user_story_to_azure_devops(
        self,
        db: Session,
        user_story: UserStoryModel,
        integration: AzureDevOpsIntegrationModel,
        create_new: bool = True
    ) -> AzureDevOpsSyncResponse:
        """Sync a user story to Azure DevOps"""
        try:
            # Prepare work item data
            work_item_data = AzureDevOpsWorkItemCreate(
                title=user_story.title,
                description=user_story.description,
                work_item_type=integration.default_work_item_type,
                area_path=integration.default_area_path,
                iteration_path=integration.default_iteration_path,
                story_points=user_story.story_points,
                priority=user_story.priority,
                tags=[f"RequirementPortal", f"Sprint-{user_story.sprint_number}"] if user_story.sprint_number else ["RequirementPortal"]
            )

            if user_story.azure_devops_work_item_id and not create_new:
                # Update existing work item
                update_data = AzureDevOpsWorkItemUpdate(
                    title=user_story.title,
                    description=user_story.description,
                    story_points=user_story.story_points,
                    priority=user_story.priority
                )
                
                result = await self.update_work_item(
                    integration,
                    user_story.azure_devops_work_item_id,
                    update_data
                )
                
                if result["success"]:
                    user_story.azure_devops_sync_status = "synced"
                    user_story.azure_devops_last_sync = datetime.utcnow()
                    db.commit()
                    
                    return AzureDevOpsSyncResponse(
                        user_story_id=user_story.id,
                        work_item_id=user_story.azure_devops_work_item_id,
                        work_item_url=user_story.azure_devops_url,
                        sync_status="synced",
                        sync_message="Work item updated successfully"
                    )
                else:
                    user_story.azure_devops_sync_status = "sync_failed"
                    db.commit()
                    
                    return AzureDevOpsSyncResponse(
                        user_story_id=user_story.id,
                        work_item_id=user_story.azure_devops_work_item_id,
                        work_item_url=user_story.azure_devops_url,
                        sync_status="sync_failed",
                        sync_message=result.get("error", "Update failed")
                    )
            else:
                # Create new work item
                result = await self.create_work_item(integration, work_item_data)
                
                if result["success"]:
                    user_story.azure_devops_work_item_id = result["work_item_id"]
                    user_story.azure_devops_work_item_type = work_item_data.work_item_type
                    user_story.azure_devops_url = result["work_item_url"]
                    user_story.azure_devops_state = "New"
                    user_story.azure_devops_sync_status = "synced"
                    user_story.azure_devops_last_sync = datetime.utcnow()
                    db.commit()
                    
                    return AzureDevOpsSyncResponse(
                        user_story_id=user_story.id,
                        work_item_id=result["work_item_id"],
                        work_item_url=result["work_item_url"],
                        sync_status="synced",
                        sync_message="Work item created successfully"
                    )
                else:
                    user_story.azure_devops_sync_status = "sync_failed"
                    db.commit()
                    
                    return AzureDevOpsSyncResponse(
                        user_story_id=user_story.id,
                        work_item_id=None,
                        work_item_url=None,
                        sync_status="sync_failed",
                        sync_message=result.get("error", "Creation failed")
                    )

        except Exception as e:
            logger.error(f"Error syncing user story to Azure DevOps: {str(e)}")
            user_story.azure_devops_sync_status = "sync_failed"
            db.commit()
            
            return AzureDevOpsSyncResponse(
                user_story_id=user_story.id,
                work_item_id=None,
                work_item_url=None,
                sync_status="sync_failed",
                sync_message=f"Sync error: {str(e)}"
            )

    async def sync_work_item_status_from_azure_devops(
        self,
        db: Session,
        user_story: UserStoryModel,
        integration: AzureDevOpsIntegrationModel
    ) -> bool:
        """Sync work item status from Azure DevOps back to user story"""
        try:
            if not user_story.azure_devops_work_item_id:
                return False

            result = await self.get_work_item(integration, user_story.azure_devops_work_item_id)
            
            if result["success"]:
                work_item = result["work_item"]
                fields = work_item.get("fields", {})
                
                # Update user story with Azure DevOps data
                user_story.azure_devops_state = fields.get("System.State")
                user_story.azure_devops_assigned_to = fields.get("System.AssignedTo", {}).get("displayName")
                
                # Update story points if changed in Azure DevOps
                azure_story_points = fields.get("Microsoft.VSTS.Scheduling.StoryPoints")
                if azure_story_points and azure_story_points != user_story.story_points:
                    user_story.story_points = int(azure_story_points)
                
                user_story.azure_devops_last_sync = datetime.utcnow()
                db.commit()
                return True
            
            return False

        except Exception as e:
            logger.error(f"Error syncing from Azure DevOps: {str(e)}")
            return False


# Create global instance
azure_devops_service = AzureDevOpsService()