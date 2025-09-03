import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import os
import logging
from prefect import task, flow
from pathlib import Path

# -------------------
# Config
# -------------------
DB_FILE = "student_scores.db"
TABLE_NAME = "scores"

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# -------------------
# Tasks
# -------------------
@task
def find_new_files(db_file=DB_FILE, data_folder="data"):
    """Find CSV files in the folder that are not yet in the database."""
    existing_years = set()
    if os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        try:
            existing_years = set(pd.read_sql(f"SELECT DISTINCT Година FROM {TABLE_NAME}", conn)["Година"].astype(str))
        except Exception as e:
            logging.warning(f"Could not read existing years: {e}")
        finally:
            conn.close()

    # Scan folder
    new_files = {}
    for file_path in Path(data_folder).glob("*.csv"):
        year = file_path.stem.split("_")[-1]  # expects files like DS_2022.csv
        if year not in existing_years:
            new_files[year] = str(file_path)

    logging.info(f"New files to process: {new_files}")
    return new_files


@task(retries=3, retry_delay_seconds=10)
def process_csv(filename: str, year: str, delimiter=";") -> pd.DataFrame:
    """Read and clean CSV data."""
    df = pd.read_csv(filename, delimiter=delimiter)
    df = df.dropna(axis=1, how="all")
    df["Година"] = int(year)
    df = df.dropna(how="all", subset=[col for col in df.columns if col not in ["Група", "Година"]])

    # Convert grades to numeric
    df["Оценка"] = pd.to_numeric(df["Оценка"], errors="coerce").fillna(2.0)

    # Remove duplicates
    df = df.drop_duplicates()

    logging.info(f"Processed {filename} for year {year}, shape={df.shape}")
    return df


@task
def save_to_db(df: pd.DataFrame, db_file=DB_FILE, table=TABLE_NAME):
    """Save DataFrame into SQLite database."""
    conn = sqlite3.connect(db_file)
    df.to_sql(table, conn, if_exists="append", index=False)
    conn.close()
    logging.info(f"Saved {len(df)} rows to {db_file}:{table}")


@task
def load_all_data(db_file=DB_FILE, table=TABLE_NAME) -> pd.DataFrame:
    """Load all data from database."""
    conn = sqlite3.connect(db_file)
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    logging.info(f"Loaded {df.shape[0]} rows from {db_file}:{table}")
    return df


@task
def analyze_and_plot(all_data: pd.DataFrame):
    """Perform EDA and generate plots."""
    avg_by_year = all_data.groupby("Година")["Оценка"].mean().reset_index()
    overall_avg_value = all_data["Оценка"].mean()

    # ---- Print stats ----
    print("Overall Statistics:")
    print(all_data["Оценка"].describe())
    print("\nStatistics by Year:")
    print(all_data.groupby("Година")["Оценка"].describe())

    # ---- Bar Chart ----
    plt.figure(figsize=(10, 6))
    bars = plt.bar(avg_by_year["Година"], avg_by_year["Оценка"], color="skyblue", edgecolor="black")
    plt.axhline(overall_avg_value, color="red", linestyle="--", label=f"Overall Avg = {overall_avg_value:.2f}")
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height + 0.05, f"{height:.2f}", ha="center", va="bottom", fontsize=10)
    plt.title("Average Student Mark by Year")
    plt.xlabel("Year")
    plt.ylabel("Average Mark")
    plt.xticks(avg_by_year["Година"].astype(int))
    plt.ylim(2, 6)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plots/avg_by_year.png")  # save instead of just showing
    plt.close()

    # ---- Pie Charts ----
    grades = [2, 3, 4, 5, 6]
    years = sorted(all_data["Година"].unique())
    plt.figure(figsize=(12, 4 * len(years)))

    for i, year in enumerate(years):
        plt.subplot(1, len(years), i + 1)
        data_year = all_data[all_data["Година"] == year]["Оценка"]
        counts = [sum(data_year == g) for g in grades]
        plt.pie(counts, labels=grades, autopct='%1.1f%%', startangle=90, colors=plt.cm.Pastel1.colors)
        plt.title(f"Grade Distribution in {year}")

    plt.tight_layout()
    plt.savefig("plots/grade_distribution.png")
    plt.close()

    logging.info("Plots saved successfully.")


# -------------------
# Flow
# -------------------
@flow(name="student-scores-pipeline")
def student_scores_pipeline():
    os.makedirs("plots", exist_ok=True)

    new_files = find_new_files()
    logging.info(new_files)
    if new_files:
        logging.info("NEW files found.")
        dfs = []
        for year, filename in new_files.items():
            dfs.append(process_csv(filename, year))
        save_to_db(pd.concat(dfs, ignore_index=True))
        all_data = load_all_data()
        analyze_and_plot(all_data)
    else:
        logging.info("No new files found.")

if __name__ == "__main__":
    student_scores_pipeline()
    # student_scores_pipeline.serve(name="student_grades", cron="0 0 * * *")
