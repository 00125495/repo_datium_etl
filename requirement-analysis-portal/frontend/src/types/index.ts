// Enums
export enum RequirementType {
  DATA_PLATFORM = 'data_platform',
  KPI = 'kpi',
  SOFTWARE = 'software',
}

export enum RequirementStatus {
  DRAFT = 'draft',
  REVIEW = 'review',
  APPROVED = 'approved',
  REJECTED = 'rejected',
}

export enum Priority {
  LOW = 'low',
  MEDIUM = 'medium',
  HIGH = 'high',
  CRITICAL = 'critical',
}

// Base interfaces
export interface Project {
  id: number
  name: string
  description?: string
  created_at: string
  updated_at?: string
  is_active: boolean
  azure_devops_organization?: string
  azure_devops_project?: string
  azure_devops_area_path?: string
  azure_devops_iteration_path?: string
}

export interface ProjectWithRequirements extends Project {
  requirements: Requirement[]
  user_stories: UserStory[]
}

export interface Requirement {
  id: number
  title: string
  description: string
  type: RequirementType
  status: RequirementStatus
  priority: Priority
  project_id: number
  business_value?: string
  acceptance_criteria?: string
  technical_requirements?: Record<string, any>
  dependencies?: string[]
  estimated_effort?: number
  ai_feedback?: Record<string, any>
  ai_score?: number
  created_at: string
  updated_at?: string
}

export interface RequirementWithUserStories extends Requirement {
  user_stories: UserStory[]
  project: Project
}

export interface UserStory {
  id: number
  title: string
  description: string
  acceptance_criteria?: string
  story_points?: number
  priority: Priority
  sprint_number?: number
  epic?: string
  project_id: number
  requirement_id?: number
  jira_ticket_id?: string
  github_issue_id?: string
  azure_devops_work_item_id?: number
  azure_devops_work_item_type?: string
  azure_devops_state?: string
  azure_devops_assigned_to?: string
  azure_devops_url?: string
  azure_devops_sync_status?: string
  azure_devops_last_sync?: string
  created_at: string
  updated_at?: string
}

export interface AIFeedback {
  id: number
  feedback_type: string
  message: string
  severity: string
  score?: number
  model_used?: string
  created_at: string
}

export interface AIValidationResponse {
  requirement_id: number
  overall_score: number
  feedback: AIFeedback[]
  suggestions: string[]
  is_valid: boolean
}

export interface UserStoryGenerationResponse {
  requirement_id: number
  generated_stories: UserStoryCreate[]
  total_estimated_points: number
  recommendations: string[]
}

// Azure DevOps interfaces
export interface AzureDevOpsIntegration {
  id: number
  project_id: number
  organization_url: string
  project_name: string
  personal_access_token?: string
  default_work_item_type: string
  default_area_path?: string
  default_iteration_path?: string
  auto_sync_enabled: boolean
  is_active: boolean
  last_sync?: string
  sync_status: string
  created_at: string
  updated_at?: string
}

export interface AzureDevOpsConnectionTest {
  organization_url: string
  project_name: string
  personal_access_token: string
}

export interface AzureDevOpsConnectionTestResponse {
  success: boolean
  message: string
  project_info?: {
    id: string
    name: string
    description?: string
    url: string
  }
  available_work_item_types?: string[]
  available_iterations?: string[]
}

export interface AzureDevOpsSyncRequest {
  user_story_id: number
  create_work_item: boolean
  work_item_type: string
  assigned_to?: string
}

export interface AzureDevOpsSyncResponse {
  user_story_id: number
  work_item_id?: number
  work_item_url?: string
  sync_status: string
  sync_message: string
}

export interface AzureDevOpsProjectSyncRequest {
  project_id: number
  sync_all_stories: boolean
  work_item_type: string
}

export interface AzureDevOpsProjectSyncResponse {
  project_id: number
  synced_stories: number
  failed_stories: number
  sync_details: AzureDevOpsSyncResponse[]
  message?: string
}

export interface AzureDevOpsSyncStatus {
  total_stories: number
  synced: number
  not_synced: number
  sync_failed: number
  manually_linked: number
  stories: Array<{
    id: number
    title: string
    azure_devops_work_item_id?: number
    azure_devops_url?: string
    azure_devops_sync_status?: string
    azure_devops_state?: string
    azure_devops_last_sync?: string
  }>
}

// Create interfaces
export interface ProjectCreate {
  name: string
  description?: string
  azure_devops_organization?: string
  azure_devops_project?: string
  azure_devops_area_path?: string
  azure_devops_iteration_path?: string
}

export interface RequirementCreate {
  title: string
  description: string
  type: RequirementType
  priority: Priority
  project_id: number
  business_value?: string
  acceptance_criteria?: string
  technical_requirements?: Record<string, any>
  dependencies?: string[]
  estimated_effort?: number
}

export interface UserStoryCreate {
  title: string
  description: string
  acceptance_criteria?: string
  story_points?: number
  priority: Priority
  sprint_number?: number
  epic?: string
  project_id: number
  requirement_id?: number
}

export interface AzureDevOpsIntegrationCreate {
  project_id: number
  organization_url: string
  project_name: string
  personal_access_token?: string
  default_work_item_type: string
  default_area_path?: string
  default_iteration_path?: string
  auto_sync_enabled: boolean
}

// API Response types
export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  per_page: number
  total_pages: number
}

export interface RequirementFilter {
  project_id?: number
  type?: RequirementType
  status?: RequirementStatus
  priority?: Priority
  min_ai_score?: number
  search_query?: string
}

export interface AIInsights {
  project_id: number
  total_requirements: number
  validated_requirements: number
  validation_coverage: number
  average_ai_score: number
  high_quality_requirements: number
  low_quality_requirements: number
  feedback_themes: Record<string, number>
  recommendations: string[]
}