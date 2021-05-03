import os
from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional, TypedDict

import pandas as pd
from dotenv.main import dotenv_values, find_dotenv
from tqdm import tqdm

from usage_plotter.utils import bytes_to

# Type annotations
Project = Literal["E3SM", "E3SM in CMIP6"]
LogLine = TypedDict(
    "LogLine",
    {
        "log_line": str,
        "date": pd.Timestamp,
        "year": Optional[int],
        "month": Optional[int],
        "requester_ip": str,
        "path": str,
        "dataset_id": str,
        "file_id": Optional[str],
        "access_type": str,
        "status_code": str,
        "bytes": str,
        "mb": float,
        "project": Project,
        "realm": Optional[str],
        "data_type": Optional[str],
        "science_driver": Optional[str],
        "campaign": Optional[str],
    },
)

# Environment configuration
CONFIG = dotenv_values(find_dotenv())
# E3SM Facets that are available in file/dataset id and directory format
AVAILABLE_FACETS = {
    "realm": ["ocean", "atmos", "land", "sea-ice"],
    "data_type": ["time-series", "climo", "model-output", "mapping", "restart"],
    "time_frequency": [
        "3hr",
        "3hr_snap",
        "5day_snap",
        "6hr",
        "6hr_ave",
        "6hr_snap",
        "day",
        "day_cosp",
        "fixed",
        "mon",
        "monClim",
    ],
    "science_driver": ["Biogeochemical Cycle", "Cryosphere", "Water Cycle"],
    "campaign": ["BGC-v1", "Cryosphere-v1", "DECK-v1", "HighResMIP-v1"],
    "activity": ["C4MIP", "CMIP", "DAMIP", "ScenarioMIP"],
}


def parse_logs(logs_path: str) -> pd.DataFrame:
    """Main parsing function, parses Apache logs into a DataFrame.

    :param logs_path: Path to access logs, configured using .env file
    :type logs_path: str
    :return: DataFrame containing parsed
    :rtype: pd.DataFrame
    """
    log_lines: List[LogLine] = []
    for log in tqdm(get_logs(logs_path)):
        for line in filter_log_lines(log):
            parsed_line = parse_log_line(line)
            log_lines.append(parsed_line)

    df = pd.DataFrame(log_lines)
    df["date"] = pd.to_datetime(df["date"])
    df["month_year"] = df["date"].dt.to_period("M")

    return df


def get_logs(path: str):
    """Fetch Apache logs from a path using a generator.

    :param path: [description]
    :type path: str
    :yield: [description]
    :rtype: [type]
    """
    for root, dirs, files in os.walk(path):
        if not files:
            continue
        if dirs:
            continue
        for file in files:
            yield str(Path(root, file).absolute())


def filter_log_lines(path: str):
    """Filter log lines using a generator.

    Refer to README.md for the typical directory and dataset id structures.

    :param path: [description]
    :type path: str
    :yield: [description]
    :rtype: [type]
    """
    with open(path, "r") as instream:
        while line := instream.readline():
            if (
                "E3SM" in line
                and "xml" not in line
                and "ico" not in line
                and "cmip6_variables" not in line
                and "html" not in line
                and "catalog" not in line
                and "aggregation" not in line
            ):
                yield line


def parse_log_line(line: str) -> LogLine:
    """Parse raw log line to extract HTTP request info.

    :param line: Raw log line from Apache log
    :type line: str
    :return: Parsed log row as a dictionary
    :rtype: LogRow
    """
    attrs = line.split()
    path = attrs[6].replace("%2F", "/")

    parsed_line: LogLine = {
        "log_line": line,
        "date": None,
        "year": None,
        "month": None,
        "requester_ip": attrs[0],
        "path": path,
        "dataset_id": "",
        "file_id": "",
        "access_type": attrs[11],
        "status_code": attrs[8],
        "bytes": attrs[9],
        "mb": bytes_to(attrs[9], "mb") if "-" not in attrs[9] else 0,
        "project": "E3SM" if "/E3SM-Project" not in path else "E3SM in CMIP6",
        "realm": None,
        "data_type": None,
        "science_driver": None,
        "campaign": None,
    }

    # None values are filled using helper functions below.
    parsed_line = parse_log_timestamp(parsed_line, raw_timestamp=attrs[3])
    parsed_line = parse_log_path(parsed_line, path)
    return parsed_line


def parse_log_timestamp(log_line: LogLine, raw_timestamp: str) -> LogLine:
    """Parse a string timestamp for specific datetime values.

    :param log_line: [description]
    :type log_line: Dict[str, Any]
    :param raw_timestamp: Raw timestamp from Apache log
    Example: "[15/Jul/2019:03:18:49 -0700]"
    :type raw_timestamp: str
    :return: [description]
    :rtype: Dict[str, Any]
    """
    timestamp = raw_timestamp[raw_timestamp.find("[") + 1 : raw_timestamp.find(":")]

    log_line["date"] = datetime.strptime(timestamp, "%d/%b/%Y").date()
    log_line["year"] = log_line["date"].year
    log_line["month"] = log_line["date"].month
    return log_line


def parse_log_path(log_line: LogLine, path):
    """Parses the full path for the dataset and file ids and facets.

    :param log_line: [description]
    :type log_line: [type]
    :return: [description]
    :rtype: [type]
    """
    try:
        idx = path.index("user_pub_work") + len("user_pub_work") + 1
    except ValueError:
        # This usually means an HTTP 302/404 request (incorrect path)
        idx = None
    log_line["dataset_id"] = ".".join(path[idx:].split("/")[:-1])
    log_line["file_id"] = path.split("/")[-1]

    dataset_facets = log_line["dataset_id"].split(".")
    for facet, options in AVAILABLE_FACETS.items():
        matching_facet = None
        for option in options:
            if option in dataset_facets:
                matching_facet = option
        log_line[facet] = matching_facet  # type: ignore

    return log_line


# def groupby_facet(df: pd.DataFrame, facet: str) -> pd.DataFrame:
#     df_gb_facet = df.copy()
#     df_gb_facet["gb"] = df_gb_facet.mb.div(1024)

#     df_gb_facet = (
#         df_gb_facet.groupby(by=["project", "month_year", facet])
#         .agg({"gb": "sum", "log_line": "count"})
#         .reset_index()
#     )
#     df_gb_facet.rename(columns={"log_line": "requests"}, inplace=True)

#     return df_gb_facet


def gen_quarterly_report(df: pd.DataFrame, facet: Optional[str] = None) -> pd.DataFrame:
    """Generates the quarterly report for total requests and data accessed.

    :param df: DataFrame containing monthly report.
    :type df: pd.DataFrame
    :return: DataFrame containing quarterly report.
    :rtype: pd.DataFrame
    """
    agg_cols = ["month_year"]
    if facet:
        agg_cols.append(facet)

    # Total requests on a monthly basis
    df_req_by_mon = df.copy()
    df_req_by_mon = df_req_by_mon.value_counts(subset=[*agg_cols]).reset_index(
        name="requests"
    )

    # Total data accessed on a monthly basis (only successful requests)
    df_data_by_mon = df.copy()
    df_data_by_mon = df_data_by_mon[df_data_by_mon.status_code.str.contains("200|206")]
    df_data_by_mon = (
        df_data_by_mon.groupby(by=[*agg_cols, "status_code"])
        .agg({"mb": "sum"})
        .reset_index()
    )
    df_data_by_mon["gb"] = df_data_by_mon.mb.div(1024)

    # Total requests and data accessed on a quarterly basis
    df_req_by_qt = resample_to_quarter(df_req_by_mon, facet)
    df_data_by_qt = resample_to_quarter(df_data_by_mon, facet)

    # Generate final quarterly report
    merge_cols = ["fy_quarter", "fiscal_year", "quarter", "start_date", "end_date"]
    if facet:
        merge_cols.append(facet)
    df_qt_report = pd.merge(df_data_by_qt, df_req_by_qt, on=merge_cols, how="inner")

    # Reorder columns for printing output
    df_qt_report = df_qt_report[[*merge_cols, "requests", "gb"]]
    return df_qt_report


def resample_to_quarter(df: pd.DataFrame, facet: Optional[str]) -> pd.DataFrame:
    """Groups a pandas DataFrame by E3SM quarters.

    :param df: DataFrame containing monthly report.
    :type df: pd.DataFrame
    :return: DataFrame containing quarterly report.
    :rtype: pd.DataFrame
    """
    # Set index to month_year in order to resample on quarters
    df_resample = df.copy().set_index("month_year")
    if facet:
        df_resample = df_resample.groupby(facet)

    df_qt: pd.DataFrame = (
        df_resample.resample("Q-JUN", convention="end").sum().reset_index()
    )
    df_qt.rename({"month_year": "fy_quarter"}, axis=1, inplace=True)  # noqa
    df_qt["fiscal_year"] = df_qt.fy_quarter.dt.strftime("%f")
    df_qt["quarter"] = df_qt.fy_quarter.dt.strftime("%q")
    df_qt["start_date"] = df_qt.apply(
        lambda row: row.fy_quarter.start_time.date(), axis=1
    )
    df_qt["end_date"] = df_qt.apply(lambda row: row.fy_quarter.end_time.date(), axis=1)
    return df_qt


def plot_report(
    df: pd.DataFrame,
    project: Project,
    facet: str,
    fiscal_year: Literal[19, 20, 21] = 21,
):
    df_fy = df.loc[df["fiscal_year"] == str(fiscal_year)]

    pd.pivot_table(
        df_fy,
        index="quarter",
        values="gb",
        columns=facet,
        aggfunc="sum",
    ).plot(
        title=f"{project} FY{fiscal_year} Total Requests ",
        xlabel="Quarter",
        ylabel="Requests",
        kind="bar",
        stacked=True,
        rot=0,
    )

    pd.pivot_table(
        df_fy,
        index="quarter",
        values="requests",
        columns=facet,
        aggfunc="sum",
    ).plot(
        title=f"{project} FY{fiscal_year} Total Data Access (GB) ",
        xlabel="Quarter",
        ylabel="Data (GB)",
        kind="bar",
        stacked=True,
        rot=0,
    )


def main():
    # Directory that contains the access logs
    logs_path = CONFIG.get("LOGS_PATH")

    if logs_path is None or logs_path == "":
        raise ValueError("LOGS_PATH is not set in .env file!")

    # Parse Apache access logs into a DataFrame.
    df: pd.DataFrame = parse_logs(logs_path)

    # Tier2 Requirements
    # Stacked plot divided by campaigns, science drivers

    # E3SM quarterly report
    # =====================
    df_e3sm = df[df.project == "E3SM"]

    # Time frequency
    df_e3sm_tf = gen_quarterly_report(df_e3sm, facet="time_frequency")
    plot_report(df_e3sm_tf, project="E3SM", facet="time_frequency", fiscal_year=20)

    # E3SM in CMIP6 quarterly report
    # ==============================
    df_e3sm_cmip6 = df[df.project == "E3SM in CMIP6"]

    # Activity
    df_e3sm_cmip6_activity = gen_quarterly_report(df_e3sm_cmip6, facet="activity")
    plot_report(
        df_e3sm_cmip6_activity,
        project="E3SM in CMIP6",
        facet="activity",
        fiscal_year=20,
    )


if __name__ == "__main__":
    main()
