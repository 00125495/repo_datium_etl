import axios from 'axios'
import type {
  Project,
  ProjectCreate,
  ProjectWithRequirements,
  Requirement,
  RequirementCreate,
  RequirementWithUserStories,
  UserStory,
  UserStoryCreate,
  AIValidationResponse,
  UserStoryGenerationResponse,
  PaginatedResponse,
  RequirementFilter,
  AIInsights,
  AzureDevOpsIntegration,
  AzureDevOpsIntegrationCreate,
  AzureDevOpsConnectionTest,
  AzureDevOpsConnectionTestResponse,
  AzureDevOpsSyncRequest,
  AzureDevOpsSyncResponse,
  AzureDevOpsProjectSyncRequest,
  AzureDevOpsProjectSyncResponse,
  AzureDevOpsSyncStatus,
} from '../types'

// Create axios instance with base configuration
const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1',
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor for logging
api.interceptors.request.use(
  (config) => {
    console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`)
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => {
    return response
  },
  (error) => {
    console.error('API Error:', error.response?.data || error.message)
    return Promise.reject(error)
  }
)

// Project API
export const projectApi = {
  getAll: async (skip = 0, limit = 100): Promise<Project[]> => {
    const response = await api.get(`/projects?skip=${skip}&limit=${limit}`)
    return response.data
  },

  getById: async (id: number): Promise<ProjectWithRequirements> => {
    const response = await api.get(`/projects/${id}`)
    return response.data
  },

  create: async (project: ProjectCreate): Promise<Project> => {
    const response = await api.post('/projects', project)
    return response.data
  },

  update: async (id: number, project: ProjectCreate): Promise<Project> => {
    const response = await api.put(`/projects/${id}`, project)
    return response.data
  },

  delete: async (id: number): Promise<void> => {
    await api.delete(`/projects/${id}`)
  },
}

// Requirements API
export const requirementApi = {
  getAll: async (params?: RequirementFilter & { skip?: number; limit?: number }): Promise<PaginatedResponse<Requirement>> => {
    const response = await api.get('/requirements', { params })
    return response.data
  },

  getById: async (id: number): Promise<RequirementWithUserStories> => {
    const response = await api.get(`/requirements/${id}`)
    return response.data
  },

  create: async (requirement: RequirementCreate): Promise<Requirement> => {
    const response = await api.post('/requirements', requirement)
    return response.data
  },

  createBulk: async (requirements: { project_id: number; requirements: RequirementCreate[] }): Promise<Requirement[]> => {
    const response = await api.post('/requirements/bulk', requirements)
    return response.data
  },

  update: async (id: number, requirement: RequirementCreate): Promise<Requirement> => {
    const response = await api.put(`/requirements/${id}`, requirement)
    return response.data
  },

  updateStatus: async (id: number, status: string): Promise<void> => {
    await api.patch(`/requirements/${id}/status`, { status })
  },

  delete: async (id: number): Promise<void> => {
    await api.delete(`/requirements/${id}`)
  },

  getSummary: async (projectId: number): Promise<any> => {
    const response = await api.get(`/requirements/project/${projectId}/summary`)
    return response.data
  },
}

// User Stories API
export const userStoryApi = {
  getAll: async (params?: { project_id?: number; requirement_id?: number; sprint_number?: number; skip?: number; limit?: number }): Promise<UserStory[]> => {
    const response = await api.get('/user-stories', { params })
    return response.data
  },

  getById: async (id: number): Promise<UserStory> => {
    const response = await api.get(`/user-stories/${id}`)
    return response.data
  },

  create: async (userStory: UserStoryCreate): Promise<UserStory> => {
    const response = await api.post('/user-stories', userStory)
    return response.data
  },

  createBulk: async (userStories: { project_id: number; user_stories: UserStoryCreate[] }): Promise<UserStory[]> => {
    const response = await api.post('/user-stories/bulk', userStories)
    return response.data
  },

  update: async (id: number, userStory: UserStoryCreate): Promise<UserStory> => {
    const response = await api.put(`/user-stories/${id}`, userStory)
    return response.data
  },

  updateJiraIntegration: async (id: number, jiraTicketId: string): Promise<void> => {
    await api.patch(`/user-stories/${id}/jira`, { jira_ticket_id: jiraTicketId })
  },

  updateGithubIntegration: async (id: number, githubIssueId: string): Promise<void> => {
    await api.patch(`/user-stories/${id}/github`, { github_issue_id: githubIssueId })
  },

  delete: async (id: number): Promise<void> => {
    await api.delete(`/user-stories/${id}`)
  },

  getSprintStories: async (projectId: number, sprintNumber: number): Promise<any> => {
    const response = await api.get(`/user-stories/project/${projectId}/sprint/${sprintNumber}`)
    return response.data
  },
}

// AI Services API
export const aiApi = {
  validateRequirement: async (requirementId: number, forceRevalidation = false): Promise<AIValidationResponse> => {
    const response = await api.post('/ai/validate', {
      requirement_id: requirementId,
      force_revalidation: forceRevalidation,
    })
    return response.data
  },

  generateUserStories: async (params: {
    requirement_id: number
    sprint_number: number
    max_stories?: number
    include_technical_tasks?: boolean
  }): Promise<UserStoryGenerationResponse> => {
    const response = await api.post('/ai/generate-stories', params)
    return response.data
  },

  generateAndSaveUserStories: async (params: {
    requirement_id: number
    sprint_number: number
    max_stories?: number
    include_technical_tasks?: boolean
  }): Promise<any> => {
    const response = await api.post('/ai/generate-and-save-stories', params)
    return response.data
  },

  getValidationHistory: async (requirementId: number): Promise<any> => {
    const response = await api.get(`/ai/validation-history/${requirementId}`)
    return response.data
  },

  batchValidateRequirements: async (projectId: number): Promise<any> => {
    const response = await api.post('/ai/batch-validate', { project_id: projectId })
    return response.data
  },

  getAIInsights: async (projectId: number): Promise<AIInsights> => {
    const response = await api.get(`/ai/ai-insights/${projectId}`)
    return response.data
  },
}

// Azure DevOps API
export const azureDevOpsApi = {
  testConnection: async (connectionTest: AzureDevOpsConnectionTest): Promise<AzureDevOpsConnectionTestResponse> => {
    const response = await api.post('/azure-devops/test-connection', connectionTest)
    return response.data
  },

  createIntegration: async (integration: AzureDevOpsIntegrationCreate): Promise<AzureDevOpsIntegration> => {
    const response = await api.post('/azure-devops/integrations', integration)
    return response.data
  },

  getIntegration: async (projectId: number): Promise<AzureDevOpsIntegration> => {
    const response = await api.get(`/azure-devops/integrations/${projectId}`)
    return response.data
  },

  updateIntegration: async (integrationId: number, integration: AzureDevOpsIntegrationCreate): Promise<AzureDevOpsIntegration> => {
    const response = await api.put(`/azure-devops/integrations/${integrationId}`, integration)
    return response.data
  },

  deleteIntegration: async (integrationId: number): Promise<void> => {
    await api.delete(`/azure-devops/integrations/${integrationId}`)
  },

  syncUserStory: async (syncRequest: AzureDevOpsSyncRequest): Promise<AzureDevOpsSyncResponse> => {
    const response = await api.post('/azure-devops/sync/user-story', syncRequest)
    return response.data
  },

  syncProject: async (syncRequest: AzureDevOpsProjectSyncRequest): Promise<AzureDevOpsProjectSyncResponse> => {
    const response = await api.post('/azure-devops/sync/project', syncRequest)
    return response.data
  },

  linkUserStoryToWorkItem: async (storyId: number, workItemId: number, workItemUrl?: string): Promise<void> => {
    await api.patch(`/azure-devops/user-stories/${storyId}/azure-devops`, {
      work_item_id: workItemId,
      work_item_url: workItemUrl,
    })
  },

  pullStatusFromAzureDevOps: async (storyId: number): Promise<void> => {
    await api.post(`/azure-devops/sync/pull-status/${storyId}`)
  },

  getSyncStatus: async (projectId: number): Promise<AzureDevOpsSyncStatus> => {
    const response = await api.get(`/azure-devops/sync/status/${projectId}`)
    return response.data
  },

  getWorkItemTypes: async (projectId: number): Promise<{ work_item_types: string[]; iterations: string[] }> => {
    const response = await api.get(`/azure-devops/work-item-types/${projectId}`)
    return response.data
  },
}

export default api