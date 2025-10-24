#!/bin/bash

# EV Analytics Setup Script

echo "🚗 Setting up EV Charging Station Analytics..."

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data
mkdir -p logs
mkdir -p data_collectors
mkdir -p data_processing
mkdir -p data_storage
mkdir -p database

# Set permissions
echo "🔐 Setting permissions..."
chmod +x setup.sh

# Copy environment file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp env.example .env
    echo "⚠️  Please edit .env file with your actual API keys and database credentials"
fi

# Create .gitignore if it doesn't exist
if [ ! -f .gitignore ]; then
    echo "📝 Creating .gitignore..."
    cat > .gitignore << EOF
# Environment variables
.env

# Data files
data/
logs/

# Python
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
env/
venv/
.venv/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
EOF
fi

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your API keys and database credentials"
echo "2. Set up your Supabase database and run database/schema.sql"
echo "3. Run: docker-compose up --build"
echo ""
echo "For more information, see README.md"
