import argparse
import os
from pipeline.components.retrieve import DataRetrievalS3
from pipeline.components.process import Processing
from pipeline.components.metrics import Metrics
from pipeline.components.cleaner import Cleaner
from pipeline.components.historical import UploadHistorical
from pipeline.components.uploader import WriteToS3
from pipeline.base import DataWrapper
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()   # Loads AWS credentials and other env vars from a local .env file (untracked)

def retrieve_state_arg() -> argparse.Namespace:
    """
    Parses and returns command line arguments. Run with --help to see all options
    """
    parser = argparse.ArgumentParser(description="Retrieve per-county outage data from s3")
    parser.add_argument("state", type=str, help="Two-letter state code (e.g., al, ga)")
    parser.add_argument("--dry-run", action="store_true", help="Run pipeline without uploading to s3")
    parser.add_argument("--date", type=str, help="Specific date to process (YYYY-MM-DD)")
    parser.add_argument("--full-test", action="store_true", help="Include uploading to S3 using test bucket (state-metrics-dev)")
    args = parser.parse_args()
    return args

def execute_pipeline(components) -> DataWrapper:
    """
    Runs each pipeline component in sequence, passing the output of one as the 
    input to the next. 
    
    Returns:
        DataWrapper result of the final component 
    """

    data = None
    component_result = None
    for c in components:
        component_result = c.execute_component(data)
        # Uncomment the following line to view intermediate metadata results for each component
        # print(component_result.metadata) 
        data = component_result 
        
    return component_result 

def is_morning_cron():
    return os.environ.get("SCHEDULED_CRON") == '0 11 * * *'

def resolve_date(date_arg):
    if date_arg:
        return datetime.strptime(date_arg, "%Y-%m-%d").date()
    
    offset = 0
    
    # On ALL pushes, use yesterday's date for metrics. 
    # Automated daily runs will resolve to today's date when necessary
    if is_morning_cron() or os.environ.get("GITHUB_EVENT_NAME") == 'push':
        offset = 1
    
    return (datetime.now() - timedelta(days=offset)).date()


def pipeline() -> DataWrapper:
    """
    Builds and executes the full pipeline for a given state and date.
    Components are initialized in order and run sequentially via execute_pipeline().
    The upload component is skipped when --dry-run is passed, in which case results
    are saved as local CSV files.

    Returns:
        DataWrapper containing the final processed output from the last component.
    """
    args = retrieve_state_arg()
    state = args.state
    dry_run = args.dry_run
    target_date = resolve_date(args.date)

    # Initialize different components
    retrieve = DataRetrievalS3(name="Retrieve Outage Data from S3", state=state, date=target_date)
    process = Processing(name="Standardize Outage Data", state=state, date=target_date)
    metrics = Metrics(name="Calculate SAIDI and SAIFI", state=state, date=target_date)
    cleaner = Cleaner(name="Add FIPs codes to each county and format data for final output", state=state, date=target_date)

    components = [retrieve, process, metrics, cleaner]

    if not dry_run:
        branch = os.environ.get("BRANCH_NAME", "dev")
        bucket_name = "state-metrics" if branch == "main" else "state-metrics-dev"

        if os.environ.get("BRANCH_NAME") == "main":
            uploadHistorical = UploadHistorical(name='Append and upload historical data to S3', bucket_name=bucket_name)
            components.append(uploadHistorical) 

        upload = WriteToS3(name='Upload Processed Data to S3', bucket_name=bucket_name)
        components.append(upload)

    result = execute_pipeline(components)
    return result
    

if __name__ == "__main__":
    """
    Output handling for local runs. Live CI/CD pipeline output is handled by the 
    upload component. 
    """
    args = retrieve_state_arg()
    result = pipeline()

    target_date = resolve_date(args.date)

    if args.dry_run:       
        formatted_df = result.data[1]
        # print(formatted_df)
        os.makedirs("testing", exist_ok=True)
        formatted_df.to_csv(os.path.join("testing", f"{args.state}_baseline.csv"), index=False)
        print(f"Dry run complete for {args.state}, {target_date}")

    if args.full_test:
        print(f"✅ Full test run complete for {args.state}, {target_date}. Metrics have been uploaded to state-metrics-dev. ✅")
