# VS Code Development Guide

## 🚀 Perfect VS Code Setup for Azure DevOps Integration

VS Code is **perfectly suited** for this full-stack project! Here's your complete guide to an optimal development experience.

## 📦 Quick Setup

### **1. Open Project in VS Code**
```bash
# Clone and open the project
git clone <repository-url>
cd requirement-analysis-portal
code .  # Opens VS Code in current directory
```

### **2. Install Recommended Extensions**
VS Code will automatically prompt you to install recommended extensions when you open the project. Click **"Install All"** or install manually:

**Essential Extensions (Auto-recommended):**
- 🐍 **Python** - Python development
- ⚛️ **TypeScript/React** - Frontend development
- 🐳 **Docker** - Container management
- 🔧 **REST Client** - API testing
- 📊 **SQLTools** - Database management
- 🎨 **Prettier** - Code formatting
- 🧪 **Pytest** - Python testing

### **3. First-Time Setup**
```bash
# VS Code will automatically configure everything, but you can also:
# 1. Open Command Palette (Cmd+Shift+P / Ctrl+Shift+P)
# 2. Type "Python: Select Interpreter"
# 3. Choose ./backend/venv/bin/python (if running locally)
```

## 🛠️ Development Workflow

### **Option 1: Docker Development (Recommended)**

#### **Start Services**
```bash
# Method 1: Use VS Code Task
Cmd+Shift+P → "Tasks: Run Task" → "🐳 Docker: Start All Services"

# Method 2: Use Terminal
Ctrl+` (open terminal)
docker-compose up --build -d
```

#### **View Logs**
```bash
# Use VS Code Task
Cmd+Shift+P → "Tasks: Run Task" → "🐳 Docker: View Logs"
```

### **Option 2: Local Development**

#### **Backend Setup**
```bash
# Use VS Code Task or Terminal
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Start backend server
Cmd+Shift+P → "Tasks: Run Task" → "🐍 Python: Run Backend Server"
```

#### **Frontend Setup**
```bash
# Use VS Code Task or Terminal
cd frontend
npm install

# Start frontend dev server
Cmd+Shift+P → "Tasks: Run Task" → "⚛️ Frontend: Start Dev Server"
```

## 🧪 Testing in VS Code

### **Run Tests with One Click**

#### **Python Tests**
```bash
# Method 1: Use Debug Configuration
F5 → Select "🐍 Python: Azure DevOps Tests"

# Method 2: Use Tasks
Cmd+Shift+P → "Tasks: Run Task" → "🐍 Python: Run Azure DevOps Tests"

# Method 3: Use Test Explorer
Click "Test" icon in sidebar → Run specific tests
```

#### **Integration Tests**
```bash
# Basic API tests (no credentials needed)
Cmd+Shift+P → "Tasks: Run Task" → "🧪 Test: Azure DevOps Integration (Basic)"

# Real Azure DevOps tests (needs credentials)
Cmd+Shift+P → "Tasks: Run Task" → "🧪 Test: Azure DevOps Integration (Real)"
```

### **API Testing with REST Client**

1. **Open API Test File**: `api-tests/azure-devops.http`
2. **Click "Send Request"** above any HTTP request
3. **View Response** in split panel

**Example:**
```http
### Test Azure DevOps Connection
POST http://localhost:8000/api/v1/azure-devops/test-connection
Content-Type: application/json

{
  "organization_url": "https://dev.azure.com/yourorg",
  "project_name": "YourProject", 
  "personal_access_token": "your-token"
}
```

## 🐛 Debugging in VS Code

### **Debug Configurations Available**

#### **1. Full Stack Debugging**
```bash
F5 → Select "🚀 Full Stack Debug"
# Starts both backend and frontend with debugging
```

#### **2. Backend Only**
```bash
F5 → Select "🐍 Python: FastAPI Backend"
# Debug Python code with breakpoints
```

#### **3. Frontend Only**
```bash
F5 → Select "⚛️ React: Frontend Debug"
# Debug React/TypeScript code
```

#### **4. Chrome Debugging**
```bash
F5 → Select "🌐 Chrome: Debug Frontend"
# Debug in Chrome DevTools integrated with VS Code
```

### **Setting Breakpoints**
1. **Python**: Click left margin in `.py` files
2. **TypeScript**: Click left margin in `.ts/.tsx` files
3. **API Debugging**: Use REST Client + Debug configuration

### **Debug Features**
- ✅ **Variables Panel**: View variable values
- ✅ **Call Stack**: See function call hierarchy
- ✅ **Watch Expressions**: Monitor specific variables
- ✅ **Debug Console**: Execute code in debug context

## 📊 Database Management

### **Connect to Database**
1. Click **SQLTools icon** in sidebar
2. Select **"PostgreSQL - Development"** connection
3. **Browse tables**, run queries, view data

### **Quick Database Access**
```bash
# Use VS Code Task
Cmd+Shift+P → "Tasks: Run Task" → "📊 Database: Connect"

# Or directly via terminal
docker-compose exec postgres psql -U postgres -d requirement_portal
```

## 🎨 Code Formatting & Linting

### **Automatic Formatting**
- **Save File**: `Cmd+S` automatically formats code
- **Manual Format**: `Shift+Alt+F`
- **Python**: Uses Black formatter
- **TypeScript/React**: Uses Prettier

### **Linting**
- **Python**: Pylint + MyPy (shows errors in Problems panel)
- **TypeScript**: ESLint (auto-fixes on save)
- **Real-time**: Issues shown with red squiggles

### **Manual Format Tasks**
```bash
# Format Python code
Cmd+Shift+P → "Tasks: Run Task" → "🔧 Format: Python Code"

# Format Frontend code  
Cmd+Shift+P → "Tasks: Run Task" → "🔧 Format: Frontend Code"
```

## 🗂️ File Navigation

### **Optimized File Explorer**
- **Hidden files**: `__pycache__`, `node_modules`, `.pytest_cache`
- **Visible**: `.env`, `.git` (for configuration)
- **Grouped**: Related files appear together

### **Quick Navigation**
- **Go to File**: `Cmd+P` → Type filename
- **Go to Symbol**: `Cmd+Shift+O` → Type function/class name
- **Go to Definition**: `F12` on any symbol
- **Find References**: `Shift+F12`

### **Multi-root Workspace**
- **Backend**: Python files, tests, API logic
- **Frontend**: React components, TypeScript, styles
- **Root**: Docker, scripts, documentation

## 🔧 VS Code Features for This Project

### **IntelliSense & Auto-completion**
- **Python**: Full FastAPI, SQLAlchemy, Pydantic support
- **TypeScript**: React, API types, Azure DevOps interfaces
- **Database**: SQL query completion
- **Docker**: Dockerfile and docker-compose syntax

### **Integrated Terminal**
- **Multiple terminals**: Backend, Frontend, Docker, Tests
- **Smart defaults**: Automatically opens in correct directory
- **Task integration**: Run tasks directly in terminal

### **Git Integration**
- **Source Control panel**: View changes, commit, push
- **Diff view**: See line-by-line changes
- **GitLens**: Enhanced Git information inline

### **Extensions Benefits**

#### **Python Development**
```python
# Example: Hovering over any function shows type hints
async def sync_user_story_to_azure_devops(
    db: Session,
    user_story: UserStoryModel,
    integration: AzureDevOpsIntegrationModel
) -> AzureDevOpsSyncResponse:
    # VS Code shows parameter types, return type, docstrings
```

#### **TypeScript Development**
```typescript
// Example: Auto-imports and type checking
import { azureDevOpsApi } from '../services/api'

const result = await azureDevOpsApi.syncUserStory({
  // VS Code shows available properties with types
  user_story_id: 123,
  create_work_item: true,
  work_item_type: "User Story"  // Auto-completion
})
```

#### **Docker Integration**
- **Container management**: Start/stop containers from VS Code
- **Log viewing**: View container logs in integrated panels
- **Image management**: Build, push, pull images

#### **Database Integration**
- **Schema browsing**: Explore database structure
- **Query execution**: Run SQL directly in VS Code
- **Result visualization**: View query results in tables

## 🚀 Productivity Tips

### **Keyboard Shortcuts (Essential)**
```bash
# General
Cmd+Shift+P          # Command Palette (most important!)
Cmd+P                # Quick Open File
Cmd+`                # Toggle Terminal
Cmd+Shift+`          # New Terminal

# Navigation
F12                  # Go to Definition
Shift+F12           # Find All References
Cmd+Shift+O         # Go to Symbol in File
Cmd+T               # Go to Symbol in Workspace

# Debugging
F5                  # Start Debugging
F9                  # Toggle Breakpoint
F10                 # Step Over
F11                 # Step Into

# Testing
Cmd+Shift+T         # Run Tests (with Python extension)
```

### **Multi-cursor Editing**
```bash
Alt+Click           # Add cursor at click position
Cmd+Alt+Down        # Add cursor below
Cmd+D              # Select next occurrence of word
Cmd+Shift+L        # Select all occurrences
```

### **Workspace Features**
- **Split editors**: View backend and frontend side-by-side
- **Integrated panels**: Problems, Terminal, Debug Console
- **Command palette**: Access all features with `Cmd+Shift+P`

## 📝 Development Scenarios

### **Scenario 1: Adding New Azure DevOps Feature**

1. **Open relevant files**:
   ```bash
   Cmd+P → azure_devops_service.py
   Cmd+P → azure_devops.py (API endpoints)
   Cmd+P → api.ts (frontend service)
   ```

2. **Add backend method with IntelliSense**:
   ```python
   # VS Code provides auto-completion for all Azure DevOps types
   async def new_azure_devops_feature(self, integration: AzureDevOpsIntegrationModel):
   ```

3. **Test with REST Client**:
   - Open `api-tests/azure-devops.http`
   - Add new test request
   - Click "Send Request"

4. **Add frontend integration**:
   ```typescript
   // VS Code shows type errors immediately
   const result = await azureDevOpsApi.newFeature(params)
   ```

5. **Debug if needed**:
   - Set breakpoints in Python code
   - F5 → Select debug configuration
   - Test with browser or REST Client

### **Scenario 2: Running All Tests**

1. **Quick test run**:
   ```bash
   Cmd+Shift+P → "Tasks: Run Task" → "🐍 Python: Run Azure DevOps Tests"
   ```

2. **Debug failing test**:
   ```bash
   F5 → "🐍 Python: Azure DevOps Tests"
   # Set breakpoints in test code
   ```

3. **Integration testing**:
   ```bash
   Cmd+Shift+P → "Tasks: Run Task" → "🧪 Test: Azure DevOps Integration (Basic)"
   ```

### **Scenario 3: Database Debugging**

1. **View database schema**:
   - Click SQLTools icon
   - Expand tables to see structure

2. **Debug database queries**:
   ```python
   # Set breakpoint in service layer
   result = db.query(UserStoryModel).filter(...).all()
   # Inspect result in debug console
   ```

3. **Manual database queries**:
   ```sql
   -- In SQLTools panel
   SELECT * FROM user_stories WHERE azure_devops_work_item_id IS NOT NULL;
   ```

## 🎯 VS Code Advantages for This Project

### **Full-Stack Development**
- ✅ **Single IDE**: Handle Python, TypeScript, Docker, SQL
- ✅ **Integrated debugging**: Debug across frontend/backend
- ✅ **Unified terminal**: All commands in one place

### **Azure DevOps Specific**
- ✅ **API testing**: Test endpoints without leaving editor
- ✅ **Type safety**: Full TypeScript and Python type checking
- ✅ **Database inspection**: View synced data immediately

### **Development Speed**
- ✅ **IntelliSense**: Smart auto-completion everywhere
- ✅ **Error detection**: Catch issues before runtime
- ✅ **Refactoring**: Safe rename across all files

### **Testing Integration**
- ✅ **One-click testing**: Run any test configuration
- ✅ **Debug tests**: Set breakpoints in test code
- ✅ **Test explorer**: Visual test management

## 🔧 Customization

### **Personal Settings**
Add to your VS Code User Settings (`Cmd+,`):

```json
{
  "workbench.colorTheme": "Dark+ (default dark)",
  "editor.fontSize": 14,
  "terminal.integrated.fontSize": 13,
  "python.terminal.activateEnvironment": true,
  "git.confirmSync": false,
  "editor.wordWrap": "on"
}
```

### **Project-Specific Keybindings**
Create `.vscode/keybindings.json`:

```json
[
  {
    "key": "cmd+shift+t",
    "command": "workbench.action.tasks.runTask",
    "args": "🧪 Test: Azure DevOps Integration (Basic)"
  },
  {
    "key": "cmd+shift+d",
    "command": "workbench.action.debug.start",
    "args": "🚀 Full Stack Debug"
  }
]
```

---

## 🎉 You're All Set!

VS Code is now perfectly configured for Azure DevOps integration development. Key benefits:

- ✅ **One-click testing** of all Azure DevOps features
- ✅ **Full-stack debugging** with breakpoints
- ✅ **API testing** directly in the editor
- ✅ **Database management** with visual tools
- ✅ **Auto-formatting** and error detection
- ✅ **Integrated Docker** container management

**Start developing**: Open any file and enjoy the intelligent code completion, error detection, and seamless workflow! 🚀