import pytest
import httpx
from unittest.mock import Mock, patch, AsyncMock
from sqlalchemy.orm import Session
from app.services.azure_devops_service import AzureDevOpsService
from app.models.requirements import (
    UserStory as UserStoryModel,
    AzureDevOpsIntegration as AzureDevOpsIntegrationModel,
    Project as ProjectModel
)
from app.schemas.requirements import (
    AzureDevOpsConnectionTest,
    AzureDevOpsWorkItemCreate,
    AzureDevOpsWorkItemUpdate
)


class TestAzureDevOpsService:
    """Test suite for Azure DevOps service layer"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.service = AzureDevOpsService()
        self.mock_integration = Mock(spec=AzureDevOpsIntegrationModel)
        self.mock_integration.organization_url = "https://dev.azure.com/testorg/"
        self.mock_integration.project_name = "TestProject"
        self.mock_integration.personal_access_token = "test-token"
        self.mock_integration.default_work_item_type = "User Story"
        self.mock_integration.default_area_path = "TestProject"
        self.mock_integration.default_iteration_path = "TestProject\\Sprint 1"
        
    def test_get_auth_header(self):
        """Test authentication header generation"""
        token = "test-token"
        header = self.service._get_auth_header(token)
        assert header.startswith("Basic ")
        assert "test-token" in header
        
    def test_get_headers(self):
        """Test standard headers generation"""
        token = "test-token"
        headers = self.service._get_headers(token)
        assert "Authorization" in headers
        assert "Content-Type" in headers
        assert headers["Content-Type"] == "application/json"
        
    @pytest.mark.asyncio
    async def test_test_connection_success(self):
        """Test successful connection to Azure DevOps"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "test-id",
            "name": "TestProject",
            "description": "Test project"
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await self.service.test_connection(
                "https://dev.azure.com/testorg",
                "TestProject",
                "test-token"
            )
            
            assert result.success is True
            assert result.message == "Connection successful"
            assert result.project_info is not None
            
    @pytest.mark.asyncio
    async def test_test_connection_failure(self):
        """Test failed connection to Azure DevOps"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Project not found"
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await self.service.test_connection(
                "https://dev.azure.com/testorg",
                "NonExistentProject",
                "test-token"
            )
            
            assert result.success is False
            assert "Failed to access project" in result.message
            
    @pytest.mark.asyncio
    async def test_test_connection_timeout(self):
        """Test connection timeout"""
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.get.side_effect = httpx.TimeoutException("Timeout")
            
            result = await self.service.test_connection(
                "https://dev.azure.com/testorg",
                "TestProject",
                "test-token"
            )
            
            assert result.success is False
            assert "Connection timeout" in result.message
            
    @pytest.mark.asyncio
    async def test_create_work_item_success(self):
        """Test successful work item creation"""
        work_item_data = AzureDevOpsWorkItemCreate(
            title="Test Work Item",
            description="Test description",
            work_item_type="User Story",
            story_points=5,
            priority="2"
        )
        
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "id": 123,
            "_links": {
                "html": {
                    "href": "https://dev.azure.com/testorg/TestProject/_workitems/edit/123"
                }
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            result = await self.service.create_work_item(self.mock_integration, work_item_data)
            
            assert result["success"] is True
            assert result["work_item_id"] == 123
            assert "work_item_url" in result
            
    @pytest.mark.asyncio
    async def test_create_work_item_failure(self):
        """Test failed work item creation"""
        work_item_data = AzureDevOpsWorkItemCreate(
            title="Test Work Item",
            description="Test description"
        )
        
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            result = await self.service.create_work_item(self.mock_integration, work_item_data)
            
            assert result["success"] is False
            assert "Failed to create work item" in result["error"]
            
    @pytest.mark.asyncio
    async def test_update_work_item_success(self):
        """Test successful work item update"""
        update_data = AzureDevOpsWorkItemUpdate(
            title="Updated Title",
            description="Updated description",
            state="Active"
        )
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": 123,
            "fields": {
                "System.Title": "Updated Title",
                "System.Description": "Updated description",
                "System.State": "Active"
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.patch.return_value = mock_response
            
            result = await self.service.update_work_item(self.mock_integration, 123, update_data)
            
            assert result["success"] is True
            assert "work_item" in result
            
    @pytest.mark.asyncio
    async def test_get_work_item_success(self):
        """Test successful work item retrieval"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": 123,
            "fields": {
                "System.Title": "Test Work Item",
                "System.State": "Active"
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await self.service.get_work_item(self.mock_integration, 123)
            
            assert result["success"] is True
            assert result["work_item"]["id"] == 123
            
    def test_extract_iteration_paths(self):
        """Test iteration path extraction"""
        iterations_data = [
            {
                "path": "\\TestProject\\Sprint 1",
                "children": [
                    {
                        "path": "\\TestProject\\Sprint 1\\Week 1",
                        "children": []
                    }
                ]
            },
            {
                "path": "\\TestProject\\Sprint 2",
                "children": []
            }
        ]
        
        paths = self.service._extract_iteration_paths(iterations_data)
        
        assert len(paths) == 3
        assert "\\TestProject\\Sprint 1" in paths
        assert "\\TestProject\\Sprint 1\\Week 1" in paths
        assert "\\TestProject\\Sprint 2" in paths
        
    @pytest.mark.asyncio
    async def test_sync_user_story_create_new(self):
        """Test syncing user story by creating new work item"""
        # Mock user story
        mock_user_story = Mock(spec=UserStoryModel)
        mock_user_story.id = 1
        mock_user_story.title = "Test User Story"
        mock_user_story.description = "Test description"
        mock_user_story.story_points = 5
        mock_user_story.priority = "medium"
        mock_user_story.sprint_number = 1
        mock_user_story.azure_devops_work_item_id = None
        
        # Mock database session
        mock_db = Mock(spec=Session)
        
        # Mock successful work item creation
        with patch.object(self.service, 'create_work_item') as mock_create:
            mock_create.return_value = {
                "success": True,
                "work_item_id": 123,
                "work_item_url": "https://dev.azure.com/testorg/TestProject/_workitems/edit/123"
            }
            
            result = await self.service.sync_user_story_to_azure_devops(
                mock_db, mock_user_story, self.mock_integration, True
            )
            
            assert result.sync_status == "synced"
            assert result.work_item_id == 123
            assert result.sync_message == "Work item created successfully"
            
    @pytest.mark.asyncio
    async def test_sync_user_story_update_existing(self):
        """Test syncing user story by updating existing work item"""
        # Mock user story with existing work item
        mock_user_story = Mock(spec=UserStoryModel)
        mock_user_story.id = 1
        mock_user_story.title = "Updated User Story"
        mock_user_story.description = "Updated description"
        mock_user_story.story_points = 8
        mock_user_story.priority = "high"
        mock_user_story.azure_devops_work_item_id = 123
        mock_user_story.azure_devops_url = "https://dev.azure.com/testorg/TestProject/_workitems/edit/123"
        
        # Mock database session
        mock_db = Mock(spec=Session)
        
        # Mock successful work item update
        with patch.object(self.service, 'update_work_item') as mock_update:
            mock_update.return_value = {
                "success": True,
                "work_item": {"id": 123}
            }
            
            result = await self.service.sync_user_story_to_azure_devops(
                mock_db, mock_user_story, self.mock_integration, False
            )
            
            assert result.sync_status == "synced"
            assert result.work_item_id == 123
            assert result.sync_message == "Work item updated successfully"
            
    @pytest.mark.asyncio
    async def test_sync_work_item_status_from_azure_devops(self):
        """Test syncing status from Azure DevOps back to user story"""
        # Mock user story
        mock_user_story = Mock(spec=UserStoryModel)
        mock_user_story.azure_devops_work_item_id = 123
        mock_user_story.story_points = 5
        
        # Mock database session
        mock_db = Mock(spec=Session)
        
        # Mock successful work item retrieval
        with patch.object(self.service, 'get_work_item') as mock_get:
            mock_get.return_value = {
                "success": True,
                "work_item": {
                    "fields": {
                        "System.State": "Active",
                        "System.AssignedTo": {"displayName": "John Doe"},
                        "Microsoft.VSTS.Scheduling.StoryPoints": 8
                    }
                }
            }
            
            result = await self.service.sync_work_item_status_from_azure_devops(
                mock_db, mock_user_story, self.mock_integration
            )
            
            assert result is True
            assert mock_user_story.azure_devops_state == "Active"
            assert mock_user_story.azure_devops_assigned_to == "John Doe"
            assert mock_user_story.story_points == 8


class TestAzureDevOpsEndpoints:
    """Test suite for Azure DevOps API endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        from fastapi.testclient import TestClient
        from app.main import app
        return TestClient(app)
        
    @pytest.fixture
    def mock_db(self):
        """Mock database session"""
        return Mock(spec=Session)
        
    def test_test_connection_endpoint(self, client):
        """Test connection endpoint"""
        response = client.post(
            "/api/v1/azure-devops/test-connection",
            json={
                "organization_url": "https://dev.azure.com/testorg",
                "project_name": "TestProject",
                "personal_access_token": "test-token"
            }
        )
        
        # Should return 200 even if connection fails (it's testing the endpoint)
        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        assert "message" in data
        
    def test_create_integration_endpoint(self, client):
        """Test create integration endpoint"""
        # First create a project
        project_response = client.post(
            "/api/v1/projects",
            json={
                "name": "Test Project",
                "description": "Test project for Azure DevOps integration"
            }
        )
        assert project_response.status_code == 200
        project_id = project_response.json()["id"]
        
        # Create integration
        response = client.post(
            "/api/v1/azure-devops/integrations",
            json={
                "project_id": project_id,
                "organization_url": "https://dev.azure.com/testorg",
                "project_name": "TestProject",
                "personal_access_token": "test-token",
                "default_work_item_type": "User Story",
                "auto_sync_enabled": False
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["project_id"] == project_id
        assert data["organization_url"] == "https://dev.azure.com/testorg"
        
    def test_get_integration_endpoint(self, client):
        """Test get integration endpoint"""
        # This will return 404 for non-existent integration
        response = client.get("/api/v1/azure-devops/integrations/999")
        assert response.status_code == 404
        
    def test_sync_user_story_endpoint(self, client):
        """Test sync user story endpoint"""
        # This will return 404 for non-existent user story
        response = client.post(
            "/api/v1/azure-devops/sync/user-story",
            json={
                "user_story_id": 999,
                "create_work_item": True,
                "work_item_type": "User Story"
            }
        )
        assert response.status_code == 404
        
    def test_get_sync_status_endpoint(self, client):
        """Test get sync status endpoint"""
        # Create a test project first
        project_response = client.post(
            "/api/v1/projects",
            json={"name": "Test Project", "description": "Test"}
        )
        project_id = project_response.json()["id"]
        
        response = client.get(f"/api/v1/azure-devops/sync/status/{project_id}")
        assert response.status_code == 200
        data = response.json()
        assert "total_stories" in data
        assert "synced" in data
        assert "stories" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])