from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from data.loader import DataLoader
from data.transforms.standard import ConcentrationAnalysisTransform
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from pipelines_client import (
    ConcentrationAnalysisRequest,
    ConcentrationAnalysisResponse,
    LoadFileResponse,
    TableData,
)

from pipelines.cache import table_cache
from pipelines.config import config
from pipelines.dependencies import get_s3_client, health_check_s3

app = FastAPI(title=config.API_TITLE, version=config.API_VERSION)


def upload_to_s3(file_content: bytes, filename: str) -> str:
    """
    Upload file content to S3 and return the S3 key.

    Args:
        file_content: The file content as bytes
        filename: Original filename

    Returns:
        S3 key where the file was uploaded
    """
    # Generate unique key with timestamp and UUID
    timestamp = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"{timestamp}/{filename}"

    try:
        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=config.S3_BUCKET,
            Key=s3_key,
            Body=file_content,
            ContentType="text/csv",
        )
        return s3_key
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"message": "Data Pipeline Service is running"}


@app.post("/files/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV file to S3 without processing it.

    Args:
        file: Uploaded CSV file

    Returns:
        Dictionary with upload status and S3 key
    """
    try:
        # Validate file type
        if not file.filename or not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")

        # Read the uploaded file content
        content = await file.read()

        # Upload to S3
        s3_key = upload_to_s3(content, file.filename)

        return {
            "success": True,
            "message": f"Successfully uploaded {file.filename} to S3",
            "s3_key": s3_key,
            "s3_bucket": config.S3_BUCKET,
            "filename": file.filename,
            "size": len(content),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@app.get("/files", response_model=LoadFileResponse)
async def get_data_from_file(
    s3_key: str = Query(..., description="S3 key of the file to load"),
    offset: int = Query(0, description="Row offset of the data to return"),
    limit: int = Query(100, description="Number of rows to return", le=5000),
):
    """
    Process a file from S3 using DataLoader and return the table data.

    Args:
        s3_key: S3 key of the file to load (as query parameter)

    Returns:
        LoadFileResponse with the processed data
    """
    try:
        if not s3_key:
            raise HTTPException(status_code=400, detail="s3_key is required")

        # Download file content from S3
        s3_client = get_s3_client()

        try:
            response = s3_client.get_object(Bucket=config.S3_BUCKET, Key=s3_key)
            csv_bytes = response["Body"].read()
        except ClientError:
            raise HTTPException(
                status_code=404, detail=f"File not found in S3: {s3_key}"
            )

        # Initialize the data loader and process from bytes
        loader = DataLoader()
        filename = s3_key.split("/")[-1]  # Extract filename from S3 key

        # Load the CSV from raw bytes
        table = await loader.load_csv_from_bytes(csv_bytes, filename)

        # Cache the table and get the version
        cache_key = table_cache.put(table)

        # Create structured table data response
        table_data = TableData(
            name=filename,
            source=table.source,
            cache_key=cache_key,
            columns=table.data.columns,
            shape=table.data.shape,
            data=table.data.slice(
                offset, limit
            ).to_dicts(),  # Convert to list of dicts for JSON serialization
            dimension_columns=table.dimension_columns,
            numeric_columns=table.numeric_columns,
            datetime_columns=table.datetime_columns,
            categorical_columns=table.categorical_columns,
        )

        return LoadFileResponse(
            success=True,
            table=table_data,
            message=f"Successfully processed {s3_key} with {table.data.shape[0]} rows and {table.data.shape[1]} columns",
            s3_key=s3_key,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/analyses/concentration", response_model=ConcentrationAnalysisResponse)
async def run_concentration(request: ConcentrationAnalysisRequest):
    """
    Run concentration analysis on a cached table.

    Args:
        request: ConcentrationAnalysisRequest with cache_key, on, and by parameters

    Returns:
        ConcentrationAnalysisResponse with the analysis results
    """
    try:
        # Get the cached table
        table = table_cache.get(request.cache_key)

        if table is None:
            raise HTTPException(
                status_code=404, detail=f"Table not found in cache: {request.cache_key}"
            )

        # Run the concentration analysis
        transform = ConcentrationAnalysisTransform(on=request.on, by=request.by)

        result_table = transform(table=table)

        # Cache the analysis results
        cache_key = table_cache.put(result_table)

        # Create structured table data response
        table_data = TableData(
            name=result_table.name,
            source=result_table.source,
            cache_key=cache_key,
            columns=result_table.data.columns,
            shape=result_table.data.shape,
            data=result_table.data.to_dicts(),
            dimension_columns=result_table.dimension_columns,
            numeric_columns=result_table.numeric_columns,
            datetime_columns=result_table.datetime_columns,
            categorical_columns=result_table.categorical_columns,
        )

        return ConcentrationAnalysisResponse(
            success=True,
            table=table_data,
            message=f"Successfully completed concentration analysis on '{request.on}' by '{request.by}' with {result_table.data.shape[0]} rows",
            pivot_by=request.by,
            concentration_measure=request.on,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error running concentration analysis: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    # Get basic health status
    health_status: dict[str, Any] = {"status": "healthy", "service": "data-pipeline"}

    # Add S3 health check
    s3_health = await health_check_s3()
    health_status["s3"] = s3_health

    # Overall status based on S3 health
    if s3_health["status"] == "unhealthy":
        health_status["status"] = "degraded"

    return health_status


def main():
    """Main function to run the FastAPI application."""
    import uvicorn

    uvicorn.run(app, host=config.HOST, port=config.PORT)


if __name__ == "__main__":
    main()
