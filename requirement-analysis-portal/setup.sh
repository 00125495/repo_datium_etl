#!/bin/bash

echo "🚀 Setting up Requirement Analysis Portal..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create environment files from examples
echo "📝 Setting up environment files..."
if [ ! -f backend/.env ]; then
    cp backend/.env.example backend/.env
    echo "✅ Created backend/.env from example"
else
    echo "⚠️  backend/.env already exists, skipping..."
fi

if [ ! -f frontend/.env ]; then
    cp frontend/.env.example frontend/.env
    echo "✅ Created frontend/.env from example"
else
    echo "⚠️  frontend/.env already exists, skipping..."
fi

# Prompt for AI API keys
echo ""
echo "🤖 AI Configuration (Optional):"
echo "To enable AI features, you can add your API keys to backend/.env"
echo ""
read -p "Do you have an OpenAI API key? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    read -p "Enter your OpenAI API key: " openai_key
    if [ ! -z "$openai_key" ]; then
        sed -i.bak "s/OPENAI_API_KEY=your-openai-api-key/OPENAI_API_KEY=$openai_key/" backend/.env
        echo "✅ OpenAI API key configured"
    fi
fi

echo ""
read -p "Do you have an Anthropic API key? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    read -p "Enter your Anthropic API key: " anthropic_key
    if [ ! -z "$anthropic_key" ]; then
        sed -i.bak "s/ANTHROPIC_API_KEY=your-anthropic-api-key/ANTHROPIC_API_KEY=$anthropic_key/" backend/.env
        echo "✅ Anthropic API key configured"
    fi
fi

# Clean up backup files
rm -f backend/.env.bak 2>/dev/null

echo ""
echo "🐳 Building and starting Docker containers..."
docker-compose up --build -d

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check if services are running
if docker-compose ps | grep -q "Up"; then
    echo ""
    echo "🎉 Setup complete! Your Requirement Analysis Portal is ready!"
    echo ""
    echo "🌐 Access your application:"
    echo "   Frontend: http://localhost:3000"
    echo "   Backend API: http://localhost:8000"
    echo "   API Documentation: http://localhost:8000/docs"
    echo ""
    echo "📚 Next steps:"
    echo "   1. Open http://localhost:3000 in your browser"
    echo "   2. Create your first project"
    echo "   3. Add requirements and see AI validation in action"
    echo "   4. Generate user stories for your sprints"
    echo ""
    echo "🛠️  Development commands:"
    echo "   - View logs: docker-compose logs -f"
    echo "   - Stop services: docker-compose down"
    echo "   - Restart services: docker-compose restart"
    echo ""
else
    echo "❌ Some services failed to start. Check the logs with:"
    echo "   docker-compose logs"
fi