#!/bin/bash
set -e

echo "🚀 Setting up lite-data-stack-postgres dbt local development environment"

# Function to check Python version
check_python_version() {
    local version=$1
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python 3 is not installed. Please install Python $version or higher"
        exit 1
    fi
    
    local current_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
    if [ "$(printf '%s\n' "$version" "$current_version" | sort -V | head -n1)" != "$version" ]; then
        echo "❌ Python $version or higher is required (you have $current_version)"
        exit 1
    fi
}

# Check Python version
MIN_PYTHON_VERSION="3.11.0"
echo "🐍 Checking Python version..."
check_python_version $MIN_PYTHON_VERSION

# Create virtual environment
VENV_DIR="venv"
echo "🔧 Creating virtual environment: $VENV_DIR"

if [ -d "$VENV_DIR" ]; then
    echo "♻️  Removing existing virtual environment..."
    rm -rf "$VENV_DIR"
fi

python3 -m venv "$VENV_DIR"
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source "$VENV_DIR/Scripts/activate"
else
    source "$VENV_DIR/bin/activate"
fi

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip uv

# Install dbt and dependencies from pyproject.toml
echo "🔨 Installing dbt and dependencies..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
uv pip install -e "$REPO_ROOT[transform]"

echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Configure your profiles.yml:"
echo "   cp profiles.yml.example profiles.yml"
echo "   Edit profiles.yml with your database credentials"
echo ""
echo "2. Activate the virtual environment:"
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    echo "   source ${VENV_DIR}/Scripts/activate"
else
    echo "   source ${VENV_DIR}/bin/activate"
fi
echo ""
echo "3. Set DBT_USER environment variable for dev mode:"
echo "   export DBT_USER=\"yourname\""
echo ""
echo "4. Run dbt commands:"
echo "   dbt deps"
echo "   dbt run"
echo "   dbt test"
