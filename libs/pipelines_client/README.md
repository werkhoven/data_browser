# Pipelines Client

A Python client library for interacting with the data pipeline service. This client provides a convenient interface to upload files, process data, and run analysis operations through the pipelines API.

## Features

- **File Upload**: Upload CSV files to the pipelines service
- **Data Processing**: Load and process files from S3 with automatic schema inference
- **Concentration Analysis**: Run concentration analysis on processed data
- **Health Monitoring**: Check service health and status
- **Error Handling**: Comprehensive error handling with detailed error messages
- **DataFrame Conversion**: Convert responses to pandas or Polars DataFrames

## Installation

```bash
# Install from the workspace
uv add pipelines-client

# Or install dependencies manually
pip install httpx pydantic
```

## Quick Start

```python
import asyncio
from pipelines_client import PipelinesClient

async def main():
    async with PipelinesClient(base_url="http://localhost:8000") as client:
        # Upload and process a file
        response = await client.upload_and_process_file("data.csv")

        # Run concentration analysis
        analysis = await client.run_concentration_analysis(
            cache_key=response.table.cache_key,
            on="Customer Code",
            by="Revenue"
        )

        # Convert to pandas DataFrame
        df = client.get_table_dataframe(analysis.table)
        print(df.head())

asyncio.run(main())
```

## API Reference

### PipelinesClient

The main client class for interacting with the pipelines service.

#### Constructor

```python
PipelinesClient(
    base_url: str = "http://localhost:8000",
    timeout: float = 30.0,
    api_key: Optional[str] = None
)
```

#### Methods

##### `upload_file(file_path, filename=None) -> dict`

Upload a CSV file to the pipelines service.

- `file_path`: Path to the CSV file (str or Path)
- `filename`: Optional custom filename
- Returns: Upload response dictionary with S3 key and metadata

##### `load_file(s3_key) -> LoadFileResponse`

Load and process a file from S3.

- `s3_key`: S3 key of the file to load
- Returns: LoadFileResponse with processed table data

##### `run_concentration_analysis(cache_key, on, by) -> ConcentrationAnalysisResponse`

Run concentration analysis on a cached table.

- `cache_key`: Cache key of the table to analyze
- `on`: Column to perform concentration analysis on (pivot by)
- `by`: Column to group by (concentration measure)
- Returns: ConcentrationAnalysisResponse with analysis results

##### `upload_and_process_file(file_path, filename=None) -> LoadFileResponse`

Upload a file and immediately process it in one operation.

- `file_path`: Path to the CSV file
- `filename`: Optional custom filename
- Returns: LoadFileResponse with processed table data

##### `health_check() -> dict`

Check the health of the pipelines service.

- Returns: Health status dictionary

##### `get_table_dataframe(table_data) -> pandas.DataFrame`

Convert TableData to a pandas DataFrame.

- `table_data`: TableData object
- Returns: pandas DataFrame

##### `get_table_polars(table_data) -> polars.DataFrame`

Convert TableData to a Polars DataFrame.

- `table_data`: TableData object
- Returns: Polars DataFrame

## Error Handling

The client raises `PipelinesClientError` for all client-related errors:

```python
from pipelines_client import PipelinesClient, PipelinesClientError

try:
    async with PipelinesClient() as client:
        response = await client.upload_file("data.csv")
except PipelinesClientError as e:
    print(f"Client error: {e}")
```

## Examples

### Basic File Processing

```python
async with PipelinesClient() as client:
    # Upload and process
    response = await client.upload_and_process_file("data.csv")

    print(f"Processed {response.table.name}")
    print(f"Shape: {response.table.shape}")
    print(f"Columns: {response.table.columns}")
```

### Concentration Analysis

```python
async with PipelinesClient() as client:
    # Load data
    response = await client.load_file("2024-01-01/data.csv")

    # Run analysis
    analysis = await client.run_concentration_analysis(
        cache_key=response.table.cache_key,
        on="Product",
        by="Revenue"
    )

    # Convert to DataFrame
    df = client.get_table_dataframe(analysis.table)
    print(df.head())
```

### Health Monitoring

```python
async with PipelinesClient() as client:
    health = await client.health_check()

    if health["status"] == "healthy":
        print("✅ Service is healthy")
    else:
        print(f"⚠️ Service status: {health['status']}")
```

## Configuration

The client can be configured with various options:

```python
client = PipelinesClient(
    base_url="https://api.example.com",  # Custom API URL
    timeout=60.0,                        # Longer timeout
    api_key="your-api-key"              # Authentication
)
```

## Requirements

- Python 3.12+
- httpx >= 0.25.0
- pydantic >= 2.11.9
- typing-extensions >= 4.8.0

Optional dependencies for DataFrame conversion:

- pandas (for `get_table_dataframe`)
- polars (for `get_table_polars`)
