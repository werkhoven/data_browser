# Data Pipeline Service

A FastAPI service that provides data loading and processing capabilities.

## Features

- **File Upload & Storage**: Upload CSV files directly to AWS S3
- **File Loading**: Load CSV files and return processed data
- **Schema Inference**: Automatic data type detection
- **Data Analysis**: Access to dimension and numeric columns
- **File Retrieval**: Retrieve uploaded files from S3
- **RESTful API**: Clean HTTP endpoints for integration

## Getting Started

### Prerequisites

- Python 3.12+
- uv package manager
- AWS account with S3 access
- AWS credentials configured

### Installation

1. Navigate to the pipelines app directory:

   ```bash
   cd apps/pipelines
   ```

2. Install dependencies:

   ```bash
   uv sync
   ```

3. Configure environment variables:

   Create a `.env` file in the `apps/pipelines` directory:

   ```bash
   # AWS Configuration
   AWS_ACCESS_KEY_ID=your_access_key_here
   AWS_SECRET_ACCESS_KEY=your_secret_key_here
   AWS_REGION=us-east-1
   S3_BUCKET=your-s3-bucket-name

   # Optional Configuration
   API_TITLE=Data Pipeline Service
   API_VERSION=1.0.0
   HOST=0.0.0.0
   PORT=8000
   MAX_FILE_SIZE_MB=100
   ```

### Running the Service

1. Start the FastAPI server:

   ```bash
   uv run python main.py
   ```

2. The service will be available at:

   ```
   http://localhost:8000
   ```

3. Access the interactive API docs at:
   ```
   http://localhost:8000/docs
   ```

## API Endpoints

### `POST /load_file`

Upload a CSV file and return processed data. The file is automatically uploaded to S3.

**Request:**

- **Content-Type**: `multipart/form-data`
- **Body**: File upload (CSV file only)

**Response:**

```json
{
  "success": true,
  "data": {
    "name": "filename.csv",
    "source": "file",
    "path": "uploads/2024/01/15/uuid_filename.csv",
    "s3_bucket": "data-browser-uploads",
    "s3_key": "uploads/2024/01/15/uuid_filename.csv",
    "columns": ["col1", "col2", "col3"],
    "shape": [1000, 3],
    "data": [...],
    "dimension_columns": ["col1"],
    "numeric_columns": ["col2", "col3"],
    "datetime_columns": [],
    "categorical_columns": []
  },
  "message": "Successfully loaded filename.csv with 1000 rows and 3 columns. File uploaded to S3: uploads/2024/01/15/uuid_filename.csv",
  "s3_key": "uploads/2024/01/15/uuid_filename.csv"
}
```

### `GET /files/{s3_key}`

Retrieve a file from S3 by its key.

**Path Parameters:**

- `s3_key`: The S3 key of the file to retrieve

**Response:**

```json
{
  "success": true,
  "s3_key": "uploads/2024/01/15/uuid_filename.csv",
  "content": "csv,file,content...",
  "size": 1024
}
```

### `GET /`

Health check endpoint.

### `GET /health`

Service health status including S3 connectivity check.

**Response:**

```json
{
  "status": "healthy",
  "service": "data-pipeline",
  "s3": {
    "status": "healthy",
    "service": "s3",
    "bucket": "data-browser-uploads",
    "region": "us-east-1"
  }
}
```

## Example Usage

### Using curl:

```bash
curl -X POST "http://localhost:8000/load_file" \
     -F "file=@path/to/your/file.csv"
```

### Using Python requests:

```python
import requests

with open("path/to/your/file.csv", "rb") as f:
    response = requests.post(
        "http://localhost:8000/load_file",
        files={"file": f}
    )
data = response.json()
print(f"Loaded {data['data']['shape'][0]} rows")
```

## Next Steps

This is a basic file loading service. Future enhancements could include:

- File upload endpoints
- Data transformation capabilities
- Analysis endpoints (concentration analysis, etc.)
- Authentication and authorization
- Batch processing capabilities
