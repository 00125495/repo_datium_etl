#!/bin/bash

# Azure DevOps Integration Testing Script
# This script tests all Azure DevOps endpoints

BASE_URL="http://localhost:8000/api/v1"
AZURE_DEVOPS_BASE="$BASE_URL/azure-devops"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🚀 Testing Azure DevOps Integration${NC}"
echo "=================================="

# Test 1: Test Connection (without real credentials)
echo -e "\n${YELLOW}Test 1: Testing Connection Endpoint${NC}"
curl -X POST "$AZURE_DEVOPS_BASE/test-connection" \
  -H "Content-Type: application/json" \
  -d '{
    "organization_url": "https://dev.azure.com/testorg",
    "project_name": "TestProject", 
    "personal_access_token": "fake-token-for-testing"
  }' \
  -w "\nStatus: %{http_code}\n" \
  -s

# Test 2: Create a test project first
echo -e "\n${YELLOW}Test 2: Creating Test Project${NC}"
PROJECT_RESPONSE=$(curl -X POST "$BASE_URL/projects" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Azure DevOps Test Project",
    "description": "Test project for Azure DevOps integration testing"
  }' \
  -s)

PROJECT_ID=$(echo $PROJECT_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin)['id'])" 2>/dev/null || echo "1")
echo "Created project with ID: $PROJECT_ID"

# Test 3: Create Azure DevOps Integration
echo -e "\n${YELLOW}Test 3: Creating Azure DevOps Integration${NC}"
curl -X POST "$AZURE_DEVOPS_BASE/integrations" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": '$PROJECT_ID',
    "organization_url": "https://dev.azure.com/testorg",
    "project_name": "TestProject",
    "personal_access_token": "fake-token-for-testing",
    "default_work_item_type": "User Story",
    "auto_sync_enabled": false
  }' \
  -w "\nStatus: %{http_code}\n" \
  -s

# Test 4: Get Integration
echo -e "\n${YELLOW}Test 4: Getting Integration${NC}"
curl -X GET "$AZURE_DEVOPS_BASE/integrations/$PROJECT_ID" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n" \
  -s

# Test 5: Create a test requirement
echo -e "\n${YELLOW}Test 5: Creating Test Requirement${NC}"
REQUIREMENT_RESPONSE=$(curl -X POST "$BASE_URL/requirements" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Test Requirement for Azure DevOps",
    "description": "This is a test requirement to verify Azure DevOps integration",
    "type": "software",
    "priority": "medium",
    "project_id": '$PROJECT_ID',
    "business_value": "Test business value",
    "acceptance_criteria": "Test acceptance criteria"
  }' \
  -s)

REQUIREMENT_ID=$(echo $REQUIREMENT_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin)['id'])" 2>/dev/null || echo "1")
echo "Created requirement with ID: $REQUIREMENT_ID"

# Test 6: Create a test user story
echo -e "\n${YELLOW}Test 6: Creating Test User Story${NC}"
USER_STORY_RESPONSE=$(curl -X POST "$BASE_URL/user-stories" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Test User Story for Azure DevOps",
    "description": "This is a test user story to verify Azure DevOps sync",
    "project_id": '$PROJECT_ID',
    "requirement_id": '$REQUIREMENT_ID',
    "story_points": 5,
    "priority": "medium",
    "sprint_number": 1
  }' \
  -s)

USER_STORY_ID=$(echo $USER_STORY_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin)['id'])" 2>/dev/null || echo "1")
echo "Created user story with ID: $USER_STORY_ID"

# Test 7: Test User Story Sync (will fail without real credentials)
echo -e "\n${YELLOW}Test 7: Testing User Story Sync${NC}"
curl -X POST "$AZURE_DEVOPS_BASE/sync/user-story" \
  -H "Content-Type: application/json" \
  -d '{
    "user_story_id": '$USER_STORY_ID',
    "create_work_item": true,
    "work_item_type": "User Story"
  }' \
  -w "\nStatus: %{http_code}\n" \
  -s

# Test 8: Test Project Sync
echo -e "\n${YELLOW}Test 8: Testing Project Sync${NC}"
curl -X POST "$AZURE_DEVOPS_BASE/sync/project" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": '$PROJECT_ID',
    "sync_all_stories": false,
    "work_item_type": "User Story"
  }' \
  -w "\nStatus: %{http_code}\n" \
  -s

# Test 9: Get Sync Status
echo -e "\n${YELLOW}Test 9: Getting Sync Status${NC}"
curl -X GET "$AZURE_DEVOPS_BASE/sync/status/$PROJECT_ID" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n" \
  -s

# Test 10: Test Work Item Types Endpoint
echo -e "\n${YELLOW}Test 10: Getting Work Item Types${NC}"
curl -X GET "$AZURE_DEVOPS_BASE/work-item-types/$PROJECT_ID" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n" \
  -s

echo -e "\n${GREEN}✅ All endpoint tests completed!${NC}"
echo -e "${YELLOW}Note: Some tests may fail due to invalid credentials - this is expected${NC}"
echo -e "${YELLOW}For full testing, add real Azure DevOps credentials${NC}"