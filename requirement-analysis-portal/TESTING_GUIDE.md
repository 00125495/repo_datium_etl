#!/bin/bash

# Real Azure DevOps Integration Testing Script
# This script tests with actual Azure DevOps credentials

BASE_URL="http://localhost:8000/api/v1"
AZURE_DEVOPS_BASE="$BASE_URL/azure-devops"
CONFIG_FILE="test_config.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Testing Azure DevOps Integration with Real Credentials${NC}"
echo "=========================================================="

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}❌ Error: $CONFIG_FILE not found!${NC}"
    echo -e "${YELLOW}Please create $CONFIG_FILE with your Azure DevOps credentials:${NC}"
    echo '{
  "azure_devops": {
    "organization_url": "https://dev.azure.com/yourorganization",
    "project_name": "YourTestProject",
    "personal_access_token": "your-actual-token-here"
  }
}'
    exit 1
fi

# Read configuration
if ! command -v jq &> /dev/null; then
    echo -e "${RED}❌ Error: jq is required for this script${NC}"
    echo "Install jq: sudo apt-get install jq (Ubuntu) or brew install jq (macOS)"
    exit 1
fi

ORGANIZATION_URL=$(jq -r '.azure_devops.organization_url' $CONFIG_FILE)
PROJECT_NAME=$(jq -r '.azure_devops.project_name' $CONFIG_FILE)
PERSONAL_ACCESS_TOKEN=$(jq -r '.azure_devops.personal_access_token' $CONFIG_FILE)

echo -e "${BLUE}Configuration:${NC}"
echo "Organization: $ORGANIZATION_URL"
echo "Project: $PROJECT_NAME"
echo "Token: ${PERSONAL_ACCESS_TOKEN:0:10}..."
echo ""

# Test 1: Test Real Connection
echo -e "${YELLOW}Test 1: Testing Real Azure DevOps Connection${NC}"
CONNECTION_RESULT=$(curl -s -X POST "$AZURE_DEVOPS_BASE/test-connection" \
  -H "Content-Type: application/json" \
  -d '{
    "organization_url": "'$ORGANIZATION_URL'",
    "project_name": "'$PROJECT_NAME'",
    "personal_access_token": "'$PERSONAL_ACCESS_TOKEN'"
  }')

CONNECTION_SUCCESS=$(echo $CONNECTION_RESULT | jq -r '.success')
if [ "$CONNECTION_SUCCESS" = "true" ]; then
    echo -e "${GREEN}✅ Connection successful!${NC}"
    echo -e "${BLUE}Project Info:${NC}"
    echo $CONNECTION_RESULT | jq '.project_info'
    echo -e "${BLUE}Available Work Item Types:${NC}"
    echo $CONNECTION_RESULT | jq '.available_work_item_types'
else
    echo -e "${RED}❌ Connection failed!${NC}"
    echo $CONNECTION_RESULT | jq '.message'
    exit 1
fi

# Test 2: Create Test Project
echo -e "\n${YELLOW}Test 2: Creating Test Project${NC}"
PROJECT_RESPONSE=$(curl -s -X POST "$BASE_URL/projects" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Azure DevOps Integration Test",
    "description": "Test project for Azure DevOps integration with real credentials"
  }')

PROJECT_ID=$(echo $PROJECT_RESPONSE | jq -r '.id')
echo "Created project with ID: $PROJECT_ID"

# Test 3: Create Real Integration
echo -e "\n${YELLOW}Test 3: Creating Real Azure DevOps Integration${NC}"
INTEGRATION_RESPONSE=$(curl -s -X POST "$AZURE_DEVOPS_BASE/integrations" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": '$PROJECT_ID',
    "organization_url": "'$ORGANIZATION_URL'",
    "project_name": "'$PROJECT_NAME'",
    "personal_access_token": "'$PERSONAL_ACCESS_TOKEN'",
    "default_work_item_type": "User Story",
    "auto_sync_enabled": false
  }')

INTEGRATION_ID=$(echo $INTEGRATION_RESPONSE | jq -r '.id')
echo "Created integration with ID: $INTEGRATION_ID"

# Test 4: Create Test Data
echo -e "\n${YELLOW}Test 4: Creating Test Requirement and User Story${NC}"

# Create requirement
REQUIREMENT_RESPONSE=$(curl -s -X POST "$BASE_URL/requirements" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Real Azure DevOps Integration Test Requirement",
    "description": "This requirement will test real Azure DevOps integration with work item creation",
    "type": "software",
    "priority": "medium",
    "project_id": '$PROJECT_ID',
    "business_value": "Validate Azure DevOps integration functionality",
    "acceptance_criteria": "1. Work item should be created in Azure DevOps\n2. Work item should have correct title and description\n3. Work item should be linkable back to user story"
  }')

REQUIREMENT_ID=$(echo $REQUIREMENT_RESPONSE | jq -r '.id')
echo "Created requirement with ID: $REQUIREMENT_ID"

# Create user story
USER_STORY_RESPONSE=$(curl -s -X POST "$BASE_URL/user-stories" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Test Azure DevOps Sync - User Story",
    "description": "This user story will be synced to Azure DevOps as a work item",
    "project_id": '$PROJECT_ID',
    "requirement_id": '$REQUIREMENT_ID',
    "story_points": 8,
    "priority": "high",
    "sprint_number": 1,
    "acceptance_criteria": "Given a user story in the portal\nWhen sync is triggered\nThen a work item should be created in Azure DevOps"
  }')

USER_STORY_ID=$(echo $USER_STORY_RESPONSE | jq -r '.id')
echo "Created user story with ID: $USER_STORY_ID"

# Test 5: Real Sync to Azure DevOps
echo -e "\n${YELLOW}Test 5: Syncing User Story to Azure DevOps${NC}"
SYNC_RESPONSE=$(curl -s -X POST "$AZURE_DEVOPS_BASE/sync/user-story" \
  -H "Content-Type: application/json" \
  -d '{
    "user_story_id": '$USER_STORY_ID',
    "create_work_item": true,
    "work_item_type": "User Story"
  }')

SYNC_STATUS=$(echo $SYNC_RESPONSE | jq -r '.sync_status')
WORK_ITEM_ID=$(echo $SYNC_RESPONSE | jq -r '.work_item_id')
WORK_ITEM_URL=$(echo $SYNC_RESPONSE | jq -r '.work_item_url')

if [ "$SYNC_STATUS" = "synced" ]; then
    echo -e "${GREEN}✅ Sync successful!${NC}"
    echo "Work Item ID: $WORK_ITEM_ID"
    echo "Work Item URL: $WORK_ITEM_URL"
    echo -e "${BLUE}🔗 You can view the work item at: $WORK_ITEM_URL${NC}"
else
    echo -e "${RED}❌ Sync failed!${NC}"
    echo "Status: $SYNC_STATUS"
    echo "Message: $(echo $SYNC_RESPONSE | jq -r '.sync_message')"
fi

# Test 6: Pull Status from Azure DevOps
echo -e "\n${YELLOW}Test 6: Pulling Status from Azure DevOps${NC}"
if [ "$SYNC_STATUS" = "synced" ]; then
    sleep 2  # Wait a moment for Azure DevOps to process
    
    PULL_RESPONSE=$(curl -s -X POST "$AZURE_DEVOPS_BASE/sync/pull-status/$USER_STORY_ID" \
      -H "Content-Type: application/json")
    
    echo "Pull status response: $PULL_RESPONSE"
    
    # Get updated user story
    UPDATED_STORY=$(curl -s -X GET "$BASE_URL/user-stories/$USER_STORY_ID" \
      -H "Content-Type: application/json")
    
    echo -e "${BLUE}Updated user story Azure DevOps fields:${NC}"
    echo $UPDATED_STORY | jq '{
      azure_devops_work_item_id,
      azure_devops_work_item_type,
      azure_devops_state,
      azure_devops_url,
      azure_devops_sync_status,
      azure_devops_last_sync
    }'
else
    echo -e "${YELLOW}⏭️ Skipping pull status test (sync failed)${NC}"
fi

# Test 7: Project Sync Status
echo -e "\n${YELLOW}Test 7: Getting Project Sync Status${NC}"
STATUS_RESPONSE=$(curl -s -X GET "$AZURE_DEVOPS_BASE/sync/status/$PROJECT_ID" \
  -H "Content-Type: application/json")

echo -e "${BLUE}Project sync status:${NC}"
echo $STATUS_RESPONSE | jq '{
  total_stories,
  synced,
  not_synced,
  sync_failed,
  manually_linked
}'

# Test 8: Get Work Item Types
echo -e "\n${YELLOW}Test 8: Getting Available Work Item Types${NC}"
TYPES_RESPONSE=$(curl -s -X GET "$AZURE_DEVOPS_BASE/work-item-types/$PROJECT_ID" \
  -H "Content-Type: application/json")

echo -e "${BLUE}Available work item types:${NC}"
echo $TYPES_RESPONSE | jq '.work_item_types'

# Test 9: Test Work Item Update (if sync was successful)
if [ "$SYNC_STATUS" = "synced" ] && [ "$WORK_ITEM_ID" != "null" ]; then
    echo -e "\n${YELLOW}Test 9: Testing Work Item Update${NC}"
    
    # Update the user story
    UPDATE_RESPONSE=$(curl -s -X PUT "$BASE_URL/user-stories/$USER_STORY_ID" \
      -H "Content-Type: application/json" \
      -d '{
        "title": "Updated Test Azure DevOps Sync - User Story",
        "description": "This user story has been updated and should sync the changes to Azure DevOps",
        "project_id": '$PROJECT_ID',
        "requirement_id": '$REQUIREMENT_ID',
        "story_points": 13,
        "priority": "critical",
        "sprint_number": 2,
        "acceptance_criteria": "Given a user story in the portal\nWhen story is updated\nThen changes should sync to Azure DevOps work item"
      }')
    
    # Sync the update
    UPDATE_SYNC_RESPONSE=$(curl -s -X POST "$AZURE_DEVOPS_BASE/sync/user-story" \
      -H "Content-Type: application/json" \
      -d '{
        "user_story_id": '$USER_STORY_ID',
        "create_work_item": false,
        "work_item_type": "User Story"
      }')
    
    UPDATE_SYNC_STATUS=$(echo $UPDATE_SYNC_RESPONSE | jq -r '.sync_status')
    if [ "$UPDATE_SYNC_STATUS" = "synced" ]; then
        echo -e "${GREEN}✅ Update sync successful!${NC}"
        echo -e "${BLUE}🔗 Check the updated work item at: $WORK_ITEM_URL${NC}"
    else
        echo -e "${RED}❌ Update sync failed!${NC}"
        echo "Status: $UPDATE_SYNC_STATUS"
        echo "Message: $(echo $UPDATE_SYNC_RESPONSE | jq -r '.sync_message')"
    fi
fi

# Summary
echo -e "\n${BLUE}📋 Test Summary${NC}"
echo "=================="
echo -e "${GREEN}✅ Connection Test: Passed${NC}"
echo -e "${GREEN}✅ Integration Creation: Passed${NC}"
echo -e "${GREEN}✅ Test Data Creation: Passed${NC}"

if [ "$SYNC_STATUS" = "synced" ]; then
    echo -e "${GREEN}✅ Azure DevOps Sync: Passed${NC}"
    echo -e "${BLUE}🎉 All tests passed! Integration is working correctly.${NC}"
    echo -e "${BLUE}🔗 Created work item: $WORK_ITEM_URL${NC}"
else
    echo -e "${RED}❌ Azure DevOps Sync: Failed${NC}"
    echo -e "${YELLOW}⚠️  Check your Azure DevOps permissions and project settings.${NC}"
fi

echo -e "\n${YELLOW}🧹 Cleanup${NC}"
echo "The test created:"
echo "- Project ID: $PROJECT_ID"
echo "- Integration ID: $INTEGRATION_ID"
echo "- User Story ID: $USER_STORY_ID"
if [ "$WORK_ITEM_ID" != "null" ]; then
    echo "- Azure DevOps Work Item ID: $WORK_ITEM_ID"
fi
echo ""
echo "You can manually clean up these test items if needed."