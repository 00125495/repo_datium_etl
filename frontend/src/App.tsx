import React from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Layout } from './components/Layout'
import { Dashboard } from './pages/Dashboard'
import { Projects } from './pages/Projects'
import { Requirements } from './pages/Requirements'
import { UserStories } from './pages/UserStories'
import { RequirementForm } from './pages/RequirementForm'
import { ProjectForm } from './pages/ProjectForm'
import { AIInsights } from './pages/AIInsights'

// Create a client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
})

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <Layout>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/projects" element={<Projects />} />
            <Route path="/projects/new" element={<ProjectForm />} />
            <Route path="/projects/:id/edit" element={<ProjectForm />} />
            <Route path="/requirements" element={<Requirements />} />
            <Route path="/requirements/new" element={<RequirementForm />} />
            <Route path="/requirements/:id/edit" element={<RequirementForm />} />
            <Route path="/user-stories" element={<UserStories />} />
            <Route path="/ai-insights" element={<AIInsights />} />
          </Routes>
        </Layout>
      </Router>
    </QueryClientProvider>
  )
}

export default App