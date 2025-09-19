import logging

import gradio as gr
import polars as pl
from pipelines_client import LoadFileResponse, PipelinesClient, PipelinesClientError
from pipelines_client.responses import ConcentrationAnalysisResponse, TableData

from browser.config import config

logger = logging.getLogger(__name__)


async def load_csv_data(file_path: str, pipelines_url: str | None = None) -> TableData:
    """Load CSV data using the pipelines service and return as DataFrame.

    Args:
        file_path: Path to the CSV file
        pipelines_url: URL of the pipelines service (defaults to config)

    Returns:
        Polars DataFrame
    """
    if pipelines_url is None:
        pipelines_url = config.PIPELINES_URL

    async with PipelinesClient(base_url=pipelines_url) as client:
        # Upload and process the file
        logger.info(f"Pipelines URL: {pipelines_url}")
        logger.info(f"Uploading and processing file: {file_path}")
        response: LoadFileResponse = await client.upload_and_process_file(file_path)

        if not response.success:
            raise Exception(response.message)

        return response.table


def create_interface():
    """Create and configure the Gradio interface."""
    with gr.Blocks(title="Data Browser") as demo:
        gr.Markdown("# Data Browser")
        gr.Markdown("Upload a CSV file to view and analyze your data!")

        # File upload section at the top
        with gr.Column(scale=1):
            with gr.Row():
                with gr.Column(scale=4):
                    file_input = gr.File(
                        label="Upload CSV File",
                        file_types=[".csv"],
                        type="filepath",
                        scale=1,
                    )
                with gr.Column(scale=8):
                    gr.HTML("")  # Empty space to push file input to 25% width
            with gr.Row():
                with gr.Column(scale=1):
                    load_btn = gr.Button("Load Data", variant="primary")
                with gr.Column(scale=6):
                    gr.HTML("")  # Empty space for alignment

        # Data table below
        data_table = gr.Dataframe(
            label="Data Preview",
            interactive=False,
            wrap=True,
        )

        # Analysis controls section
        gr.Markdown("## Concentration Analysis")

        with gr.Column(scale=1):
            with gr.Row():
                with gr.Column(scale=1):
                    pivot_dropdown = gr.Dropdown(
                        label="Pivot By Column",
                        choices=[],
                        interactive=True,
                        allow_custom_value=True,
                    )
                with gr.Column(scale=1):
                    measure_dropdown = gr.Dropdown(
                        label="Concentration Measure",
                        choices=[],
                        interactive=True,
                        allow_custom_value=True,
                    )
                with gr.Column(scale=2):
                    gr.HTML("")  # Empty space for alignment

            with gr.Row():
                with gr.Column(scale=1):
                    analyze_btn = gr.Button("Run Analysis", variant="primary")
                with gr.Column(scale=8):
                    gr.HTML("")  # Empty space for alignment

        # Analysis results table
        analysis_table = gr.Dataframe(
            label="Concentration Analysis Results",
            interactive=False,
            wrap=True,
            visible=False,
        )

        # Load data when file is uploaded or button is clicked
        async def process_file(
            file_path,
        ) -> tuple[pl.DataFrame | None, TableData | None]:
            if file_path is None:
                return None, None

            table: TableData = await load_csv_data(file_path)
            if isinstance(table, str):  # Error message
                return None, table

            # Return the Polars DataFrame and table data
            return pl.DataFrame(table.data), table

        # Function to run concentration analysis
        async def run_analysis(table_data: TableData, pivot_by: str, measure: str):
            try:
                async with PipelinesClient(base_url=config.PIPELINES_URL) as client:
                    # Run concentration analysis using the pipelines service
                    response: ConcentrationAnalysisResponse = (
                        await client.run_concentration_analysis(
                            cache_key=table_data.cache_key,
                            on=measure,
                            by=[pivot_by],
                        )
                    )

                    # Convert to pandas DataFrame for Gradio
                    return pl.DataFrame(response.table.data)

            except PipelinesClientError as e:
                return f"Error running analysis: {str(e)}"
            except Exception as e:
                return f"Unexpected error: {str(e)}"

        # Create state to store the current table
        current_table = gr.State(None)

        # Function to update dropdown choices
        def update_dropdowns(table_data: TableData | None):
            if table_data is None:
                return gr.update(choices=[]), gr.update(choices=[])
            return (
                gr.update(choices=table_data.dimension_columns),
                gr.update(choices=table_data.numeric_columns),
            )

        # Update click handlers to include dropdown updates and table state
        load_btn.click(
            fn=process_file,
            inputs=[file_input],
            outputs=[data_table, current_table],
        ).then(
            fn=update_dropdowns,
            inputs=[current_table],
            outputs=[pivot_dropdown, measure_dropdown],
        )

        # Also load when file is uploaded
        file_input.change(
            fn=process_file,
            inputs=[file_input],
            outputs=[data_table, current_table],
        ).then(
            fn=update_dropdowns,
            inputs=[current_table],
            outputs=[pivot_dropdown, measure_dropdown],
        )

        # Run analysis when button is clicked
        analyze_btn.click(
            fn=run_analysis,
            inputs=[current_table, pivot_dropdown, measure_dropdown],
            outputs=analysis_table,
        ).then(
            fn=lambda: gr.update(visible=True),
            inputs=[],
            outputs=analysis_table,
        )

    return demo


def main():
    """Main function to launch the Gradio application."""
    demo = create_interface()
    demo.launch(
        server_name=config.BROWSER_HOST,
        server_port=config.BROWSER_PORT,
        share=config.BROWSER_SHARE,
        show_error=True,
    )


if __name__ == "__main__":
    main()
