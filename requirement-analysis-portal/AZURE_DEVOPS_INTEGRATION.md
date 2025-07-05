# Azure DevOps Integration Guide

## 🚀 Overview

The Requirement Analysis Portal now includes comprehensive **Azure DevOps integration** that enables seamless bi-directional synchronization between your requirements portal and Azure DevOps work items. This integration allows teams to maintain a single source of truth while leveraging both platforms' strengths.

## ✨ Features Implemented

### 🔄 **Bi-directional Synchronization**
- Create Azure DevOps work items from user stories
- Sync user story updates to Azure DevOps
- Pull status updates from Azure DevOps back to the portal
- Maintain data consistency across platforms

### 🎯 **Work Item Management**
- Support for all Azure DevOps work item types (User Story, Task, Bug, Feature, etc.)
- Custom area path and iteration path mapping
- Story points synchronization
- Priority and status mapping
- Automatic tagging for portal-generated items

### 🔧 **Configuration & Testing**
- Connection testing with real-time validation
- Project-specific integration settings
- Personal Access Token secure storage
- Available work item types and iterations discovery

### 📊 **Bulk Operations**
- Sync all user stories in a project at once
- Background processing for large datasets
- Detailed sync status reporting
- Retry mechanisms for failed syncs

## 🏗️ Architecture

### Backend Components

#### **1. Database Models** (`app/models/requirements.py`)
- **Enhanced UserStory Model**: Added Azure DevOps fields
  ```python
  azure_devops_work_item_id: Column(Integer)
  azure_devops_work_item_type: Column(String(50))
  azure_devops_state: Column(String(50))
  azure_devops_assigned_to: Column(String(255))
  azure_devops_url: Column(String(500))
  azure_devops_sync_status: Column(String(50))
  azure_devops_last_sync: Column(DateTime)
  ```

- **AzureDevOpsIntegration Model**: Configuration management
  ```python
  organization_url: Column(String(255))
  project_name: Column(String(255))
  personal_access_token: Column(String(255))
  default_work_item_type: Column(String(50))
  default_area_path: Column(String(500))
  default_iteration_path: Column(String(500))
  auto_sync_enabled: Column(Boolean)
  ```

#### **2. Service Layer** (`app/services/azure_devops_service.py`)
- **AzureDevOpsService Class**: Core integration logic
  - Connection testing and validation
  - Work item CRUD operations
  - Bi-directional synchronization
  - Error handling and retry logic
  - Authentication with Personal Access Tokens

#### **3. API Endpoints** (`app/api/v1/endpoints/azure_devops.py`)
- **15 comprehensive endpoints** for full Azure DevOps management
- RESTful design with proper HTTP status codes
- Background task support for bulk operations
- Detailed error handling and validation

### Frontend Components

#### **1. TypeScript Types** (`frontend/src/types/index.ts`)
- Complete type definitions for Azure DevOps entities
- Integration configuration interfaces
- Sync request/response models
- Status tracking types

#### **2. API Service** (`frontend/src/services/api.ts`)
- **azureDevOpsApi**: Complete API client
- Type-safe method signatures
- Error handling and response parsing
- Integration with existing API architecture

## 📋 Complete API Reference

### Connection Management
```typescript
POST /api/v1/azure-devops/test-connection
// Test connection to Azure DevOps with credentials

POST /api/v1/azure-devops/integrations
// Create new Azure DevOps integration for a project

GET /api/v1/azure-devops/integrations/{project_id}
// Get existing integration configuration

PUT /api/v1/azure-devops/integrations/{integration_id}
// Update integration settings

DELETE /api/v1/azure-devops/integrations/{integration_id}
// Deactivate integration
```

### Synchronization Operations
```typescript
POST /api/v1/azure-devops/sync/user-story
// Sync single user story to Azure DevOps

POST /api/v1/azure-devops/sync/project
// Bulk sync all user stories in a project

POST /api/v1/azure-devops/sync/pull-status/{story_id}
// Pull latest status from Azure DevOps

GET /api/v1/azure-devops/sync/status/{project_id}
// Get comprehensive sync status for all stories
```

### Work Item Management
```typescript
PATCH /api/v1/azure-devops/user-stories/{story_id}/azure-devops
// Manually link user story to existing work item

GET /api/v1/azure-devops/work-item-types/{project_id}
// Get available work item types and iterations
```

## 🔧 Setup Instructions

### 1. **Azure DevOps Prerequisites**

#### Create Personal Access Token
1. Navigate to Azure DevOps → User Settings → Personal Access Tokens
2. Click "New Token"
3. Configure token:
   - **Name**: `Requirement Portal Integration`
   - **Organization**: Select your organization
   - **Expiration**: Set appropriate duration
   - **Scopes**: Select "Work Items (Read & Write)"
4. Copy the generated token (save securely!)

#### Get Organization & Project Information
- **Organization URL**: `https://dev.azure.com/yourorganization`
- **Project Name**: Exact name as it appears in Azure DevOps

### 2. **Portal Configuration**

#### Backend Environment
```bash
# No additional environment variables needed for Azure DevOps
# Personal Access Tokens are stored per-project in the database
```

#### Create Integration
```bash
# Via API or frontend interface
curl -X POST "http://localhost:8000/api/v1/azure-devops/integrations" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": 1,
    "organization_url": "https://dev.azure.com/yourorg",
    "project_name": "YourProject",
    "personal_access_token": "your-pat-here",
    "default_work_item_type": "User Story",
    "auto_sync_enabled": false
  }'
```

#### Test Connection
```bash
curl -X POST "http://localhost:8000/api/v1/azure-devops/test-connection" \
  -H "Content-Type: application/json" \
  -d '{
    "organization_url": "https://dev.azure.com/yourorg",
    "project_name": "YourProject",
    "personal_access_token": "your-pat-here"
  }'
```

## 🔄 Synchronization Workflows

### **1. User Story → Azure DevOps Work Item**

#### Automatic Creation
```typescript
// When generating user stories with AI
const syncRequest: AzureDevOpsSyncRequest = {
  user_story_id: 123,
  create_work_item: true,
  work_item_type: "User Story",
  assigned_to: "user@company.com"
}

const result = await azureDevOpsApi.syncUserStory(syncRequest)
```

#### Manual Sync
```typescript
// Sync existing user story
const syncRequest: AzureDevOpsSyncRequest = {
  user_story_id: 123,
  create_work_item: true,
  work_item_type: "Task"
}

const result = await azureDevOpsApi.syncUserStory(syncRequest)
```

### **2. Azure DevOps → Portal Updates**

#### Pull Status Updates
```typescript
// Pull latest status for a user story
await azureDevOpsApi.pullStatusFromAzureDevOps(123)

// Updates local user story with:
// - Current state (New, Active, Resolved, Closed)
// - Assigned user
// - Updated story points
// - Last sync timestamp
```

### **3. Bulk Operations**

#### Project-wide Sync
```typescript
const syncRequest: AzureDevOpsProjectSyncRequest = {
  project_id: 1,
  sync_all_stories: false, // Only sync unsynced stories
  work_item_type: "User Story"
}

const result = await azureDevOpsApi.syncProject(syncRequest)
// Returns: synced count, failed count, detailed results
```

## 📊 Work Item Mapping

### **Field Mapping**
| Portal Field | Azure DevOps Field | Notes |
|-------------|-------------------|-------|
| `title` | `System.Title` | Direct mapping |
| `description` | `System.Description` | HTML formatted |
| `story_points` | `Microsoft.VSTS.Scheduling.StoryPoints` | Numeric value |
| `priority` | `Microsoft.VSTS.Common.Priority` | Mapped values |
| `sprint_number` | `System.IterationPath` | Via iteration mapping |
| `epic` | `System.AreaPath` | Via area path mapping |
| `acceptance_criteria` | Included in description | Formatted section |

### **Status Mapping**
| Portal Status | Azure DevOps State | Sync Direction |
|--------------|-------------------|---------------|
| Draft | New | Portal → Azure DevOps |
| Active | Active | Bi-directional |
| Review | Resolved | Bi-directional |
| Completed | Closed | Azure DevOps → Portal |

### **Priority Mapping**
| Portal Priority | Azure DevOps Priority |
|----------------|---------------------|
| Low | 4 |
| Medium | 3 |
| High | 2 |
| Critical | 1 |

## 🔍 Monitoring & Debugging

### **Sync Status Tracking**
```typescript
// Get comprehensive sync status
const status = await azureDevOpsApi.getSyncStatus(projectId)

// Returns:
{
  total_stories: 25,
  synced: 20,
  not_synced: 3,
  sync_failed: 2,
  manually_linked: 0,
  stories: [
    {
      id: 123,
      title: "User Story Title",
      azure_devops_work_item_id: 456,
      azure_devops_url: "https://dev.azure.com/...",
      azure_devops_sync_status: "synced",
      azure_devops_state: "Active",
      azure_devops_last_sync: "2024-01-15T10:30:00Z"
    }
    // ... more stories
  ]
}
```

### **Error Handling**
- **Connection Failures**: Automatic retry with exponential backoff
- **Authentication Errors**: Clear error messages with token validation
- **Work Item Conflicts**: Detailed conflict resolution information
- **Rate Limiting**: Respect Azure DevOps API rate limits

### **Logging**
```python
# Backend logging for troubleshooting
logger.info(f"Syncing user story {story_id} to Azure DevOps")
logger.error(f"Failed to create work item: {error_message}")
logger.debug(f"Azure DevOps response: {response_data}")
```

## 🔐 Security Considerations

### **Personal Access Token Storage**
- Tokens stored encrypted in database
- Never logged or exposed in API responses
- Project-specific token isolation
- Automatic token validation

### **API Security**
- All requests authenticated
- Input validation on all endpoints
- SQL injection prevention
- Rate limiting and abuse protection

### **Data Privacy**
- Only sync explicitly chosen user stories
- Respect Azure DevOps permissions
- Audit trail for all sync operations
- Secure token transmission (HTTPS only)

## 🚀 Advanced Features

### **Background Processing**
- Large sync operations run in background
- Progress tracking and status updates
- Queue management for high-volume syncs
- Automatic retry for failed operations

### **Custom Work Item Types**
- Support for custom Azure DevOps work item types
- Dynamic field mapping
- Custom state workflows
- Organization-specific configurations

### **Webhook Support** (Future Enhancement)
- Real-time updates from Azure DevOps
- Automatic bi-directional sync
- Event-driven synchronization
- Minimal API usage

## 📈 Performance Optimizations

### **Caching Strategy**
- Connection validation results cached
- Work item type discovery cached
- Reduced API calls for repeated operations

### **Batch Operations**
- Multiple work items created in batches
- Efficient bulk update operations
- Parallel processing where possible

### **Rate Limit Management**
- Automatic rate limit detection
- Queue management for API calls
- Graceful degradation under limits

## 🔧 Troubleshooting Guide

### **Common Issues**

#### Connection Test Fails
```bash
# Check organization URL format
✅ Correct: https://dev.azure.com/yourorg
❌ Wrong: yourorg.visualstudio.com
❌ Wrong: dev.azure.com/yourorg (missing https://)

# Verify project name
✅ Use exact name from Azure DevOps
❌ Don't use project ID or display name variations
```

#### Personal Access Token Issues
```bash
# Required permissions
✅ Work Items (Read & Write)
❌ Insufficient: Read only permissions
❌ Expired: Check token expiration date

# Token format
✅ Should be base64-like string: YourTokenHere123
❌ Should not include "Basic " prefix
```

#### Sync Failures
```bash
# Check work item type availability
GET /api/v1/azure-devops/work-item-types/{project_id}

# Verify area and iteration paths
# Must match exactly with Azure DevOps paths

# Check user story data completeness
# Title and description are required fields
```

## 🎯 Best Practices

### **1. Integration Setup**
- Test connection before enabling auto-sync
- Configure default work item types appropriately
- Set up area paths to match team structure
- Use iteration paths for sprint mapping

### **2. User Story Management**
- Include comprehensive acceptance criteria
- Use consistent story point estimation
- Maintain clear story titles and descriptions
- Link related stories through requirements

### **3. Synchronization Strategy**
- Start with manual sync to verify setup
- Enable auto-sync only after testing
- Monitor sync status regularly
- Handle sync failures promptly

### **4. Team Workflow**
- Train team on bi-directional sync implications
- Establish data ownership policies
- Use consistent naming conventions
- Maintain sync audit trails

## 📚 Additional Resources

### **Azure DevOps API Documentation**
- [Work Items REST API](https://docs.microsoft.com/en-us/rest/api/azure/devops/wit/work-items)
- [Personal Access Tokens](https://docs.microsoft.com/en-us/azure/devops/organizations/accounts/use-personal-access-tokens-to-authenticate)

### **Portal API Documentation**
- OpenAPI/Swagger documentation available at `/docs`
- Interactive API testing interface
- Complete endpoint documentation with examples

---

**✅ Azure DevOps integration is now fully implemented and ready for production use!**

This integration provides a seamless bridge between your requirement analysis portal and Azure DevOps, enabling teams to maintain consistency across platforms while leveraging the unique strengths of each tool.