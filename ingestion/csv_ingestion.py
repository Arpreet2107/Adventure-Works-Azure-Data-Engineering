from pathlib import Path
from datetime import datetime
import pandas as pd


# -------------------------------------------------------------------
# Project paths
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
LOG_DIR = PROJECT_ROOT / "logs"

LOG_FILE = LOG_DIR / "ingestion_log.csv"


# -------------------------------------------------------------------
# Dataset configuration
# -------------------------------------------------------------------

DATASETS = {
    "Calendar": {
        "file_name": "AdventureWorks_Calendar.csv",
        "encoding": "utf-8",
    },
    "Customers": {
        "file_name": "AdventureWorks_Customers.csv",
        "encoding": "cp1252",
    },
    "Product_Categories": {
        "file_name": "AdventureWorks_Product_Categories.csv",
        "encoding": "utf-8",
    },
    "Product_Subcategories": {
        "file_name": "AdventureWorks_Product_Subcategories.csv",
        "encoding": "utf-8",
    },
    "Products": {
        "file_name": "AdventureWorks_Products.csv",
        "encoding": "utf-8",
    },
    "Returns": {
        "file_name": "AdventureWorks_Returns.csv",
        "encoding": "utf-8",
    },
    "Sales_2015": {
        "file_name": "AdventureWorks_Sales_2015.csv",
        "encoding": "utf-8",
    },
    "Sales_2016": {
        "file_name": "AdventureWorks_Sales_2016.csv",
        "encoding": "utf-8",
    },
    "Sales_2017": {
        "file_name": "AdventureWorks_Sales_2017.csv",
        "encoding": "utf-8",
    },
    "Territories": {
        "file_name": "AdventureWorks_Territories.csv",
        "encoding": "utf-8",
    },
}


# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------

def validate_input_file(file_path: Path) -> None:
    """Validate that the source file exists and is a CSV."""
    
    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {file_path}"
        )

    if file_path.suffix.lower() != ".csv":
        raise ValueError(
            f"Expected a CSV file: {file_path}"
        )


def load_csv(
    file_path: Path,
    encoding: str
) -> pd.DataFrame:
    """
    Load CSV using the configured encoding.
    Falls back to cp1252 if UTF-8 decoding fails.
    """

    try:
        return pd.read_csv(
            file_path,
            encoding=encoding
        )

    except UnicodeDecodeError:

        print(
            f"Encoding '{encoding}' failed for "
            f"{file_path.name}. Trying cp1252..."
        )

        return pd.read_csv(
            file_path,
            encoding="cp1252"
        )


def write_bronze(
    df: pd.DataFrame,
    dataset_name: str
) -> Path:
    """Write dataset into its Bronze directory."""

    dataset_dir = BRONZE_DIR / dataset_name
    dataset_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = dataset_dir / f"{dataset_name}.csv"

    df.to_csv(
        output_file,
        index=False
    )

    return output_file


def write_log(log_record: dict) -> None:
    """Append one ingestion record to the persistent log."""

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    log_df = pd.DataFrame([log_record])

    if LOG_FILE.exists():

        log_df.to_csv(
            LOG_FILE,
            mode="a",
            header=False,
            index=False
        )

    else:

        log_df.to_csv(
            LOG_FILE,
            mode="w",
            header=True,
            index=False
        )


# -------------------------------------------------------------------
# Dataset ingestion
# -------------------------------------------------------------------

def ingest_dataset(
    dataset_name: str,
    config: dict
) -> dict:

    source_file = RAW_DIR / config["file_name"]

    validate_input_file(source_file)

    start_time = datetime.now()

    print("=" * 80)
    print(f"Starting ingestion: {dataset_name}")
    print(f"Source: {source_file}")

    try:

        df = load_csv(
            source_file,
            config["encoding"]
        )

        input_rows = len(df)
        input_columns = len(df.columns)

        output_file = write_bronze(
            df,
            dataset_name
        )

        end_time = datetime.now()

        log_record = {
            "dataset": dataset_name,
            "source_file": str(source_file),
            "rows_processed": input_rows,
            "columns_processed": input_columns,
            "status": "SUCCESS",
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": (
                end_time - start_time
            ).total_seconds(),
            "output_path": str(output_file),
            "error_message": "",
        }

        write_log(log_record)

        print(f"Rows processed : {input_rows:,}")
        print(f"Columns        : {input_columns}")
        print(f"Bronze output  : {output_file}")
        print(f"Status         : SUCCESS")
        print(f"Completed at   : {end_time}")
        print()

        return log_record

    except Exception as error:

        end_time = datetime.now()

        log_record = {
            "dataset": dataset_name,
            "source_file": str(source_file),
            "rows_processed": 0,
            "columns_processed": 0,
            "status": "FAILED",
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": (
                end_time - start_time
            ).total_seconds(),
            "output_path": "",
            "error_message": str(error),
        }

        write_log(log_record)

        print(f"Status         : FAILED")
        print(f"Error          : {error}")
        print()

        return log_record


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main() -> None:

    print("\n" + "=" * 80)
    print("ADVENTURE WORKS - BRONZE INGESTION")
    print("=" * 80)

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    results = []

    for dataset_name, config in DATASETS.items():

        result = ingest_dataset(
            dataset_name,
            config
        )

        results.append(result)

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------

    results_df = pd.DataFrame(results)

    print("\n" + "=" * 80)
    print("INGESTION SUMMARY")
    print("=" * 80)

    print(
        results_df[
            [
                "dataset",
                "rows_processed",
                "columns_processed",
                "status"
            ]
        ].to_string(index=False)
    )

    successful = (
        results_df["status"] == "SUCCESS"
    ).sum()

    failed = (
        results_df["status"] == "FAILED"
    ).sum()

    print("\nSuccessful datasets:", successful)
    print("Failed datasets    :", failed)
    print("Log file            :", LOG_FILE)


if __name__ == "__main__":
    main()