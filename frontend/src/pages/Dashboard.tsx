import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { PlusIcon } from '@heroicons/react/24/outline'
import { projectApi, requirementApi } from '../services/api'

export function Dashboard() {
  const { data: projects, isLoading: projectsLoading } = useQuery({
    queryKey: ['projects'],
    queryFn: () => projectApi.getAll(),
  })

  const { data: requirements, isLoading: requirementsLoading } = useQuery({
    queryKey: ['requirements'],
    queryFn: () => requirementApi.getAll({ limit: 10 }),
  })

  const isLoading = projectsLoading || requirementsLoading

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600"></div>
      </div>
    )
  }

  const stats = [
    {
      name: 'Total Projects',
      value: projects?.length || 0,
      href: '/projects',
      color: 'bg-blue-500',
    },
    {
      name: 'Active Requirements',
      value: requirements?.items?.length || 0,
      href: '/requirements',
      color: 'bg-green-500',
    },
    {
      name: 'Validated Requirements',
      value: requirements?.items?.filter(req => req.ai_score && req.ai_score >= 0.7).length || 0,
      href: '/requirements',
      color: 'bg-purple-500',
    },
    {
      name: 'Average AI Score',
      value: calculateAverageScore(requirements?.items || []),
      href: '/ai-insights',
      color: 'bg-orange-500',
    },
  ]

  return (
    <div className="p-6">
      <div className="sm:flex sm:items-center">
        <div className="sm:flex-auto">
          <h1 className="text-2xl font-semibold text-gray-900">Dashboard</h1>
          <p className="mt-2 text-sm text-gray-700">
            Overview of your requirement analysis portal
          </p>
        </div>
        <div className="mt-4 sm:mt-0 sm:ml-16 sm:flex-none">
          <Link
            to="/projects/new"
            className="btn btn-primary flex items-center"
          >
            <PlusIcon className="h-4 w-4 mr-2" />
            New Project
          </Link>
        </div>
      </div>

      {/* Stats */}
      <div className="mt-8 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
        {stats.map((stat) => (
          <div key={stat.name} className="card p-6">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <div className={`w-8 h-8 rounded-full ${stat.color}`} />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    {stat.name}
                  </dt>
                  <dd className="flex items-baseline">
                    <div className="text-2xl font-semibold text-gray-900">
                      {typeof stat.value === 'number' && stat.name.includes('Score')
                        ? stat.value.toFixed(2)
                        : stat.value}
                    </div>
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Recent Activity */}
      <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Recent Projects */}
        <div className="card">
          <div className="px-6 py-4 border-b border-gray-200">
            <h3 className="text-lg font-medium text-gray-900">Recent Projects</h3>
          </div>
          <div className="px-6 py-4">
            {projects && projects.length > 0 ? (
              <div className="space-y-4">
                {projects.slice(0, 5).map((project) => (
                  <div key={project.id} className="flex items-center justify-between">
                    <div>
                      <Link
                        to={`/projects/${project.id}`}
                        className="text-sm font-medium text-gray-900 hover:text-primary-600"
                      >
                        {project.name}
                      </Link>
                      <p className="text-xs text-gray-500">
                        {new Date(project.created_at).toLocaleDateString()}
                      </p>
                    </div>
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                      Active
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <p className="text-sm text-gray-500">No projects yet</p>
                <Link
                  to="/projects/new"
                  className="mt-2 inline-flex items-center text-sm text-primary-600 hover:text-primary-500"
                >
                  <PlusIcon className="h-4 w-4 mr-1" />
                  Create your first project
                </Link>
              </div>
            )}
          </div>
        </div>

        {/* Recent Requirements */}
        <div className="card">
          <div className="px-6 py-4 border-b border-gray-200">
            <h3 className="text-lg font-medium text-gray-900">Recent Requirements</h3>
          </div>
          <div className="px-6 py-4">
            {requirements && requirements.items.length > 0 ? (
              <div className="space-y-4">
                {requirements.items.slice(0, 5).map((requirement) => (
                  <div key={requirement.id} className="flex items-center justify-between">
                    <div>
                      <Link
                        to={`/requirements/${requirement.id}`}
                        className="text-sm font-medium text-gray-900 hover:text-primary-600"
                      >
                        {requirement.title}
                      </Link>
                      <p className="text-xs text-gray-500">
                        {requirement.type} • {requirement.priority}
                      </p>
                    </div>
                    <div className="flex items-center space-x-2">
                      {requirement.ai_score && (
                        <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                          requirement.ai_score >= 0.8
                            ? 'bg-green-100 text-green-800'
                            : requirement.ai_score >= 0.6
                            ? 'bg-yellow-100 text-yellow-800'
                            : 'bg-red-100 text-red-800'
                        }`}>
                          {requirement.ai_score.toFixed(2)}
                        </span>
                      )}
                      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                        requirement.status === 'approved'
                          ? 'bg-green-100 text-green-800'
                          : requirement.status === 'review'
                          ? 'bg-yellow-100 text-yellow-800'
                          : 'bg-gray-100 text-gray-800'
                      }`}>
                        {requirement.status}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <p className="text-sm text-gray-500">No requirements yet</p>
                <Link
                  to="/requirements/new"
                  className="mt-2 inline-flex items-center text-sm text-primary-600 hover:text-primary-500"
                >
                  <PlusIcon className="h-4 w-4 mr-1" />
                  Create your first requirement
                </Link>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function calculateAverageScore(requirements: any[]): number {
  if (!requirements || requirements.length === 0) return 0
  
  const validScores = requirements
    .filter(req => req.ai_score != null)
    .map(req => req.ai_score)
  
  if (validScores.length === 0) return 0
  
  return validScores.reduce((sum, score) => sum + score, 0) / validScores.length
}