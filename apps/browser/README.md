# Data Browser Application

A simple Gradio-based web application for data browsing and analysis.

## Features

- Clean, modern web interface
- Interactive data exploration capabilities
- Built with Gradio for easy deployment

## Getting Started

### Prerequisites

- Python 3.12+
- pip or uv package manager

### Installation

1. Navigate to the browser app directory:

   ```bash
   cd apps/browser
   ```

2. Install dependencies:
   ```bash
   pip install -e .
   # or with uv:
   # uv sync
   ```

### Running the Application

1. Start the application:

   ```bash
   python main.py
   ```

2. Open your web browser and navigate to:
   ```
   http://localhost:7860
   ```

### Development

The application uses Gradio's Blocks API for creating the interface. The main components are:

- `hello_world()`: Simple function that returns a greeting message
- `create_interface()`: Sets up the Gradio interface with buttons and outputs
- `main()`: Launches the Gradio server

## Next Steps

This is a basic "Hello World" implementation. Future enhancements will include:

- File upload functionality for CSV/Excel files
- Data preview and exploration features
- AI-powered data analysis capabilities
- Concentration analysis tools
