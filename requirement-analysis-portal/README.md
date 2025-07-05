# Requirement Analysis Portal

A comprehensive requirement analysis portal with agentic AI capabilities that helps teams capture, validate, and manage requirements for data platform, KPIs, and software projects. The system provides real-time AI feedback and automatically generates DevOps user stories based on sprint planning.

## 🚀 Features

### Core Functionality
- **Project Management**: Create and organize projects with requirements and user stories
- **Multi-Type Requirements**: Support for Data Platform, KPI, and Software requirements
- **AI-Powered Validation**: Real-time requirement validation using OpenAI/Anthropic APIs
- **User Story Generation**: Automatic generation of user stories from requirements
- **Sprint Planning**: Organize user stories by sprints with story point estimation
- **DevOps Integration**: JIRA and GitHub issue integration for user stories

### AI Capabilities
- **Real-time Validation**: AI analyzes requirements as you type
- **Smart Feedback**: Contextual suggestions for improving requirement quality
- **Automated User Stories**: Generate well-structured user stories from requirements
- **Quality Scoring**: AI scoring system to measure requirement completeness
- **Batch Processing**: Validate multiple requirements simultaneously

### Technical Features
- **Modern Stack**: React + TypeScript frontend, FastAPI backend
- **Database Support**: PostgreSQL with SQLAlchemy ORM
- **Caching**: Redis for performance optimization
- **API Documentation**: Auto-generated OpenAPI/Swagger documentation
- **Docker Support**: Full containerization for easy deployment

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Backend       │    │   AI Services   │
│   (React/TS)    │◄──►│   (FastAPI)     │◄──►│   (OpenAI/      │
│                 │    │                 │    │    Anthropic)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   UI Components │    │   Database      │    │   Background    │
│   (TailwindCSS) │    │   (PostgreSQL)  │    │   Tasks (Redis) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🛠️ Technology Stack

### Frontend
- **React 18** with TypeScript
- **Vite** for build tooling
- **TailwindCSS** for styling
- **React Router** for navigation
- **TanStack Query** for data fetching
- **React Hook Form** for form management
- **Heroicons** for icons

### Backend
- **FastAPI** with Python 3.11
- **SQLAlchemy** for ORM
- **PostgreSQL** as primary database
- **Redis** for caching and background tasks
- **Pydantic** for data validation
- **OpenAI/Anthropic** for AI services

### DevOps
- **Docker** and Docker Compose
- **GitHub Actions** ready
- **Environment-based configuration**
- **Health checks** and monitoring

## 📋 Prerequisites

- Docker and Docker Compose
- Node.js 18+ (for local development)
- Python 3.11+ (for local development)
- OpenAI API key (optional, for AI features)
- Anthropic API key (optional, for AI features)

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone <repository-url>
cd requirement-analysis-portal
```

### 2. Environment Setup
```bash
# Copy environment files
cp backend/.env.example backend/.env
cp frontend/.env.example frontend/.env

# Edit the environment files with your configurations
# Add your OpenAI/Anthropic API keys for AI features
```

### 3. Start with Docker Compose
```bash
# Start all services
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build
```

### 4. Access the Application
- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs

## 🔧 Development Setup

### Backend Development
```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the server
uvicorn app.main:app --reload
```

### Frontend Development
```bash
cd frontend

# Install dependencies
npm install

# Start development server
npm run dev
```

## 📊 Usage Guide

### 1. Create a Project
1. Navigate to the Projects section
2. Click "New Project"
3. Fill in project details and save

### 2. Add Requirements
1. Go to Requirements section
2. Click "New Requirement"
3. Choose requirement type (Data Platform, KPI, or Software)
4. Fill in details with business value and acceptance criteria
5. The AI will automatically validate and provide feedback

### 3. Generate User Stories
1. Open a requirement
2. Click "Generate User Stories"
3. Specify sprint number and options
4. Review and save generated stories

### 4. Manage Sprints
1. Go to User Stories section
2. Filter by sprint number
3. Manage story points and priorities
4. Link to JIRA tickets or GitHub issues

## 🤖 AI Features Configuration

### OpenAI Setup
```bash
# Add to backend/.env
OPENAI_API_KEY=your-openai-api-key-here
```

### Anthropic Setup
```bash
# Add to backend/.env
ANTHROPIC_API_KEY=your-anthropic-api-key-here
```

### AI Validation Features
- **Completeness Check**: Validates if requirements have all necessary fields
- **Clarity Assessment**: Analyzes requirement clarity and suggests improvements
- **Business Value Alignment**: Ensures requirements align with business objectives
- **Technical Feasibility**: Checks for technical implementation considerations
- **Testability**: Validates acceptance criteria and testing approaches

## 🔌 API Endpoints

### Projects
- `GET /api/v1/projects` - List all projects
- `POST /api/v1/projects` - Create a new project
- `GET /api/v1/projects/{id}` - Get project details
- `PUT /api/v1/projects/{id}` - Update project
- `DELETE /api/v1/projects/{id}` - Delete project

### Requirements
- `GET /api/v1/requirements` - List requirements with filtering
- `POST /api/v1/requirements` - Create requirement
- `GET /api/v1/requirements/{id}` - Get requirement details
- `PUT /api/v1/requirements/{id}` - Update requirement
- `DELETE /api/v1/requirements/{id}` - Delete requirement

### User Stories
- `GET /api/v1/user-stories` - List user stories
- `POST /api/v1/user-stories` - Create user story
- `GET /api/v1/user-stories/{id}` - Get user story details
- `PUT /api/v1/user-stories/{id}` - Update user story

### AI Services
- `POST /api/v1/ai/validate` - Validate requirement with AI
- `POST /api/v1/ai/generate-stories` - Generate user stories
- `GET /api/v1/ai/ai-insights/{project_id}` - Get AI analytics

## 🔒 Security Features

- **Input Validation**: All inputs are validated using Pydantic
- **CORS Configuration**: Proper CORS setup for frontend-backend communication
- **Environment Variables**: Sensitive data stored in environment variables
- **API Rate Limiting**: Built-in rate limiting for API endpoints
- **SQL Injection Prevention**: SQLAlchemy ORM prevents SQL injection

## 📈 Performance Features

- **Caching**: Redis caching for frequently accessed data
- **Background Tasks**: Async processing for AI operations
- **Database Optimization**: Indexed queries and efficient relationships
- **Lazy Loading**: Frontend components load data on demand
- **Query Optimization**: TanStack Query for efficient data fetching

## 🧪 Testing

### Backend Tests
```bash
cd backend
pytest tests/
```

### Frontend Tests
```bash
cd frontend
npm test
```

## 🚀 Deployment

### Production Deployment
1. Set up production environment variables
2. Configure production database
3. Set up SSL certificates
4. Deploy using Docker Compose or Kubernetes

### Environment Variables for Production
```bash
# Backend
DATABASE_URL=postgresql://user:password@host:port/db
SECRET_KEY=your-production-secret-key
ENVIRONMENT=production
OPENAI_API_KEY=your-production-openai-key
ANTHROPIC_API_KEY=your-production-anthropic-key

# Frontend
VITE_API_URL=https://your-api-domain.com/api/v1
```

## 🔮 Future Enhancements

### Planned Features
- **Advanced AI Models**: Integration with more AI models
- **Team Collaboration**: Real-time collaboration features
- **Advanced Analytics**: Detailed requirement analytics dashboard
- **Integration Hub**: More DevOps tool integrations
- **Mobile App**: Mobile application for on-the-go access
- **Workflow Engine**: Custom workflow definitions
- **Reporting**: Advanced reporting and export capabilities

### Extensibility
- **Plugin System**: Support for custom plugins
- **Custom AI Models**: Ability to integrate custom AI models
- **Third-party APIs**: Easy integration with external services
- **Custom Fields**: Configurable requirement fields
- **Theming**: Custom UI themes and branding

## 📝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Create an issue in the GitHub repository
- Check the API documentation at `/docs`
- Review the troubleshooting guide below

## 🔍 Troubleshooting

### Common Issues

**Backend not starting:**
- Check if PostgreSQL is running
- Verify database connection string
- Ensure all required environment variables are set

**Frontend not loading:**
- Check if backend is running on port 8000
- Verify API URL in frontend environment
- Check browser console for errors

**AI features not working:**
- Verify API keys are correctly set
- Check API key permissions and quotas
- Review AI service logs for errors

**Database connection issues:**
- Check PostgreSQL service status
- Verify database credentials
- Ensure database exists and is accessible

## 📊 Monitoring

The application includes health check endpoints:
- Backend health: `GET /health`
- Database connection: Built into health check
- AI services: Status included in health check