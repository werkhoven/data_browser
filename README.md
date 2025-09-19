# Data Browser

A Simple Data Browser to browse data and perform concentration analysis.

## Architecture Overview

### System Design Philosophy

This project implements a **microservices architecture** with a focus on:

- **Dynamic Data Handling**: Automatically processes unknown CSV schemas without manual configuration
- **Efficient In-Memory Processing**: Built with Polars for high-performance data operations
- **Auditable Transformations**: Every data transformation is traceable and versioned

### Core Components

#### 1. **Data Processing Pipeline** (`libs/data/`)

The heart of the system, providing:

- **AI Schema Inference**: Pydantic AI Agent to infer the schema of the data
- **Intelligent Data Cleaning**: Applies regex-based cleaning patterns inferred by AI
- **Memory-Efficient Processing**: Built on Polars for high-performance data operations

#### 2. **Pipelines API Service** (`apps/pipelines/`)

A FastAPI-based microservice handling:

- **File Upload & Storage**: Remote file storage via S3 (mocked via LocalStack)
- **Data Processing Orchestration**: Coordinates schema inference and data cleaning
- **Analysis Execution**: Runs concentration analysis and other analytical transforms
- **Caching Layer**: In-memory table caching for performance optimization

#### 3. **Browser Interface** (`apps/browser/`)

A Gradio-based web application providing:

- **File Upload**: CSV file upload
- **Data Preview**: Live data table with automatic column type detection
- **Analysis Controls**: Dynamic dropdowns populated from inferred schema
- **Results Visualization**: Concentration analysis results

#### 4. **Client Library** (`libs/pipelines_client/`)

A client library for communicating with the Pipelines API:

### Schema Inference:

#### Why Use AI?

1. **Zero Configuration**: Works with any CSV structure out-of-the-box
2. **Intelligent Pattern Recognition**: Detects currency symbols, date formats, categorical data
3. **Context-Aware**: Considers semantic meaning of the column headers and data patterns where data
   types and content may be misleading (.e.g is "2024" a partial date or an integer?)
4. **Datetime Fusion**: Groups of columns may contain related content that require complex transformations.

#### Trade-offs Analysis

**Advantages:**

- ✅ **Rapid Deployment**: No manual schema configuration required
- ✅ **Handles Edge Cases**: AI recognizes unusual data patterns humans might miss
- ✅ **Self-Documenting**: AI explanations provide audit trail for decisions
- ✅ **Adaptable**: Adapts to new data sources without code changes

**Disadvantages:**

- ❌ **API Dependency**: Requires OpenAI API access and internet connectivity
- ❌ **Cost**: AI inference adds per-request costs (mitigated by caching)
- ❌ **Latency**: Initial schema inference takes 2-3 seconds
- ❌ **Black Box**: AI decisions may be less predictable than rule-based systems
- ❌ **Failure Loops**: AI inference may persistently fail where traditional rules may succeed.

**Hybrid Approach Recommendation:**
Althogh not implemented here, a hybrid approach would be preferable for production systems:

1. **Non-AI Inference**: Use traditional rules + statistical patterns to infer the schema and clean
   the data.
2. **Use AI for Tricky Cases**: Test AI for tricky cases where traditional rules fail.
3. **Reasonable Fallback Rules**: When cleaning and inference fails, use reasonable fallback rules.
4. **Catalog Failure**: Log failures and use them to improve both AI and rule-based systems.

## How to Run the Application

### Prerequisites

- Docker and Docker Compose installed
- OpenAI API key (for AI schema inference)

### Quick Start

1. **Set up environment variables:**

   ```bash
   cp env.example .env
   # Edit .env and define OPENAI_API_KEY
   # All other defaults are fine.
   ```

2. **Start the application:**

   ```bash
   docker compose down && docker compose up --build
   ```

3. **Access the application:**
   - **Browser Interface**: http://localhost:8001
   - **Pipelines API**: http://localhost:8000

## Areas for Improvement

### 1. **Scalability & Performance**

#### **Current Limitations:**

- **In-Memory Processing**: All data transformations happen in memory
- **Single-Node Architecture**: No horizontal scaling capabilities
- **Limited Caching**: Only basic in-memory table caching

#### **Recommended Improvements:**

- **Streaming Processing**: Implement Apache Spark or Dask for large datasets
- **Distributed Architecture**: Add Kubernetes orchestration for horizontal scaling
- **Persistent Caching**: Redis distributed caching
- **Data Partitioning**: Implement time-based or hash-based data partitioning

### 2. **Data Lineage & Traceability**

#### **Current State:**

- **Basic Caching**: Simple in-memory cache with UUID keys
- **No Versioning**: No version control for data transformations
- **Limited Audit Trail**: Basic logging without comprehensive lineage

#### **Recommended Improvements:**

- **Data Lineage Tracking**: Implement Apache Airflow or Prefect for workflow orchestration
- **Version Control**: Implement version control for data transformations
- **Persistence**: Allow persistent storage of transformed data
- **Audit Logging**: Comprehensive logging of all data transformations
- **Metadata Management**: Data catalog with schema evolution tracking

### 4. **Package & Build Optimization**

#### **Current Issues:**

- **Large Docker Images**: Multiple Python dependencies increase image size
- **No Multi-Stage Builds**: Development dependencies included in production
- **No Dependency Optimization**: All packages installed regardless of usage

#### **Recommended Improvements:**

- **Multi-Stage Docker Builds**: Separate build and runtime stages
- **Alpine Linux Base**: Use lightweight Alpine images
- **Dependency Analysis**: Remove unused dependencies
- **Layer Optimization**: Optimize Docker layer caching

### 5. **Anomaly Detection & Insights Generation**

#### **Current State:**

- **No Automated Insights**: System only performs basic concentration analysis without surfacing business insights
- **No Anomaly Detection**: No identification of unusual patterns, outliers, or data quality issues
- **Manual Analysis Required**: Users must manually interpret results without AI-powered guidance
- **No Business Context**: Analysis lacks domain-specific knowledge about financial data patterns

#### **Simple Improvements:**

- **Outlier Detection**: Flagging individual segments/periods with concrentration above defined thresholds.
- **Trend Analysis**: Use simple statistical models to identify large positive or negative trends in concentration.
- **Missing Data Detection**: Flag unreliable results where the raw data is too sparse/incomplete.

##### **Business Intelligence Layer**

- **Map Data to Standard Models**: Use AI to map schema onto standard data models for more context aware analysis.
- **Financial Metrics**: Calculate key financial ratios (concentration risk, customer lifetime value, churn rates)
- **Benchmarking**: Compare against industry standards and historical performance

#### **Customizable Data Workflows**

- **Workflow Artifacts**: Establish a framework where users can define custom workflows that consist
  of standard transformations (out-of-the-box tools), AI-powered transformations, custom transformations (defined with the help of AI), and AI-driven insights.

**Built with:** Python, FastAPI, Gradio, Polars, Pydantic, Docker, LocalStack, OpenAI GPT-4
