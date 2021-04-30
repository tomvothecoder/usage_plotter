import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import matplotlib.pyplot as plt
import numpy as np  # noqa
import pandas as pd
from tqdm import tqdm

# E3SM templates for parsing
# file_id_template_ = %(source)s.%(model_version)s.%(experiment)s.%(grid_resolution)s.%(realm)s.%(regridding)s.%(data_type)s.%(time_frequency)s.%(ensemble_member)s
# directory_format_template_ = %(root)s/%(source)s/%(model_version)s/%(experiment)s/%(grid_resolution)s/%(realm)s/%(regridding)s/%(data_type)s/%(time_frequency)s/%

# E3SM Facets that are available in file/dataset id and directory format
REALMS = ["ocean", "atmos", "land", "sea-ice"]
DATA_TYPES = ["time-series", "climo", "model-output", "mapping", "restart"]
TIME_FREQUENCY = [
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
]

# Unavailable
CAMPAIGNS = ["BGC-v1", "Cryosphere-v1", "DECK-v1", "HighResMIP-v1"]
SCIENCE_DRIVERS = ["Biogeochemical Cycle", "Cryosphere", "Water Cycle"]


def bytes_to(
    bytes_str: Union[str, int],
    to: Literal["kb", "mb", "gb", "tb"],
    bsize: int = 1024,
):
    """Convert bytes to another unit."""
    map_sizes = {"kb": 1, "mb": 2, "gb": 3, "t": 4}

    bytes = float(bytes_str)
    return bytes / (bsize ** map_sizes[to])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "root", help="path to directory full of access logs for ESGF datasets"
    )
    return parser.parse_args()


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


def filter_lines(path: str):
    """Filter log lines using a generator.

    :param path: [description]
    :type path: str
    :yield: [description]
    :rtype: [type]
    """
    with open(path, "r") as instream:
        while (line := instream.readline()) :
            if (
                "E3SM" in line
                and "CMIP6" not in line
                and "xml" not in line
                and "ico" not in line
                and "cmip6_variables" not in line
                and "html" not in line
                and "catalog" not in line
                and "aggregation" not in line
            ):
                yield line


def parse_log_line(log_line: str):
    """Parse log line to extract HTTP request info.

    Example log:

        '128.211.148.13 - - [22/Sep/2019:12:01:01 -0700] "GET /thredds/fileServer/user_pub_work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/land/native/model-output/mon/ens1/v1/20180215.DECKv1b_H1.ne30_oEC.edison.clm2.h0.1850-01.nc HTTP/1.1" 200 91564624 "-" "Wget/1.14 (linux-gnu)"\n'

    Example log split:
        ['128.211.148.13',
        '-',
        '-',
        '[22/Sep/2019:12:01:01',
        '-0700]',
        '"GET',
        '/thredds/fileServer/user_pub_work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/land/native/model-output/mon/ens1/v1/20180215.DECKv1b_H1.ne30_oEC.edison.clm2.h0.1850-01.nc',
        'HTTP/1.1"',
        '200',
        '91564624',
        '"-"',
        '"Wget/1.14',
        '(linux-gnu)"']

    How to read Apache log line:

        * https://www.keycdn.com/support/apache-access-log#reading-the-apache-access-logs

    :param log_line: [description]
    :type log_line: Dict[str, Any]
    :return: [description]
    :rtype: Dict[str, Any]
    """
    attrs = log_line.split()

    # None values are filled using helper functions.
    parsed_log_line: Dict[str, Any] = {
        "log": log_line,
        "date": None,
        "year": None,
        "month": None,
        "requester_ip": attrs[0],
        "full_path": attrs[6],
        "dataset_id": None,
        "file_id": None,
        "access_type": attrs[11],
        "status_code": attrs[8],
        "bytes": attrs[9],
        "mb": bytes_to(attrs[9], "mb") if "-" not in attrs[9] else 0,
        "realm": None,
        "data_type": None,
        "science_driver": None,
        "campaign": None,
    }

    parsed_log_line = parse_timestamp(attrs[3], parsed_log_line)
    parsed_log_line = parse_path_for_ids(parsed_log_line)
    return parsed_log_line


def parse_timestamp(timestamp: str, log_row: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a string timestamp for specific datetime values.

    :param timestamp: [description]
    :type timestamp: str
    :param log_row: [description]
    :type log_row: Dict[str, Any]
    :return: [description]
    :rtype: Dict[str, Any]
    """
    timestamp_str = timestamp[timestamp.find("[") + 1 : timestamp.find(":")]

    log_row["date"] = datetime.strptime(timestamp_str, "%d/%b/%Y").date()
    log_row["year"] = log_row["date"].year
    log_row["month"] = log_row["date"].month
    return log_row


def parse_path_for_ids(log_row):
    """Parses the full path for the dataset and file ids.

    :param log_row: [description]
    :type log_row: [type]
    :return: [description]
    :rtype: [type]
    """
    try:
        idx = log_row["full_path"].index("user_pub_work") + len("user_pub_work") + 1
    except ValueError:
        idx = None
        print("ERROR: " + log_row["full_path"])

    log_row["dataset_id"] = ".".join(log_row["full_path"][idx:].split("/")[:-1])
    log_row["file_id"] = log_row["full_path"].split("/")[-1]

    facets = log_row["dataset_id"].split(".")
    log_row.update(
        {
            "realm": extract_facets_from_dataset_id(facets, REALMS),
            "data_type": extract_facets_from_dataset_id(facets, DATA_TYPES),
            "time_frequency": extract_facets_from_dataset_id(facets, TIME_FREQUENCY),
            "science_driver": extract_facets_from_dataset_id(facets, SCIENCE_DRIVERS),
            "campaign": extract_facets_from_dataset_id(facets, CAMPAIGNS),
        }
    )

    return log_row


def extract_facets_from_dataset_id(
    file_facets: List[str],
    options: List[str]
    # TODO: Refactor this function
) -> Optional[str]:
    """Extracts facets from a dataset id.

    :param file_facets: [description]
    :type file_facets: List[str]
    :param options: [description]
    :type options: List[str]
    :return: [description]
    :rtype: Optional[str]
    """
    facet = None
    for option in options:
        if option in file_facets:
            facet = option

    return facet


def group_by_quarter(df: pd.DataFrame) -> pd.DataFrame:
    """Groups a pandas DataFrame by E3SM quarters.

    :param df: [description]
    :type df: pd.DataFrame
    :return: [description]
    :rtype: pd.DataFrame

    # TODO: Confirm resampling quarter start month, it is based on IG FY
    """
    # Set index to month_year in order to resample on quarters
    df_gb_mon_yr = df.copy()
    df_gb_mon_yr = df_gb_mon_yr.set_index("month_year")

    df_gb_qt: pd.DataFrame = (
        df_gb_mon_yr.resample("Q-JUN", convention="end").sum().reset_index()
    )
    df_gb_qt.rename({"month_year": "fy_quarter"}, axis=1, inplace=True)  # noqa
    df_gb_qt["fiscal_year"] = df_gb_qt.fy_quarter.dt.strftime("%f")
    df_gb_qt["quarter"] = df_gb_qt.fy_quarter.dt.strftime("%q")
    df_gb_qt["start_date"] = df_gb_qt.apply(
        lambda row: row.fy_quarter.start_time.date(), axis=1
    )
    df_gb_qt["end_date"] = df_gb_qt.apply(
        lambda row: row.fy_quarter.end_time.date(), axis=1
    )
    return df_gb_qt


def plot_qt_report(
    df: pd.DataFrame,
    project: str,
    fiscal_year: Literal["19", "20", "21"] = "21",
):
    """Plot total data accessed and total requests on a quarterly basis.

    :param df: DataFrame containing quarterly data
    :type df: pd.DataFrame
    :param project: The related project
    :type project: str
    :param fiscal_year: The fiscal year to plot, defaults to "21"
    :type fiscal_year: Literal["19", "20", "21"], optional

    # TODO: Refactor function to use generic plot method
    """
    df_fiscal_year = df.loc[df["fiscal_year"] == fiscal_year]

    data_plot = df_fiscal_year.plot(
        title=f"{project} FY{fiscal_year} Total Data Access ",
        kind="bar",
        x="quarter",
        y=["gb"],
        legend=None,
        rot=0,
    )
    data_plot.set(xlabel="Quarter", ylabel="Total Data (GB)")

    for p in data_plot.patches:
        data_plot.annotate(
            "%.2f" % p.get_height(),
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="center",
            xytext=(0, 3.5),
            textcoords="offset points",
        )

    plt.show()

    plot_reqs = df_fiscal_year.plot(
        title=f"{project} FY{fiscal_year} Total Requests ",
        kind="bar",
        x="quarter",
        y=["requests"],
        legend=None,
        rot=0,
    )
    plot_reqs.set(xlabel="Quarter", ylabel="Total Requests")

    for p in plot_reqs.patches:
        plot_reqs.annotate(
            p.get_height(),
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="center",
            xytext=(0, 3.5),
            textcoords="offset points",
        )

    plt.show()
    # fig = plot_data.get_figure()
    # fig.savefig(f"e3sm_requests_by_month_{year}", dpi=fig.dpi, facecolor="w")


def plot_by_month(df: pd.DataFrame, project: str):
    # TODO: Update to support any y-axis
    years = df["year"].unique().tolist()

    for year in years:
        df_agg_year = df.loc[df["year"] == year]
        plot = df_agg_year.plot(
            title=f"{project} Requests by month ({year})",
            kind="bar",
            x="month",
            y=["count"],
            legend=None,
        )
        plot.set(xlabel="month", ylabel="Requests")
        plt.show()
        fig = plot.get_figure()
        fig.savefig(f"e3sm_requests_by_month_{year}", dpi=fig.dpi, facecolor="w")


if __name__ == "__main__":
    root_dir = "../access_logs"

    # Parse request logs
    rows = []
    for log in tqdm(get_logs(root_dir)):
        for line in filter_lines(log):
            row = parse_log_line(line)
            rows.append(row)

    # Generate dataframe of parsed logs
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["month_year"] = df["date"].dt.to_period("M")

    # Total data accessed on a monthly basis (only successful requests)
    df_data_by_mon = df.copy()
    df_data_by_mon = df_data_by_mon[df_data_by_mon.status_code.str.contains("200|206")]
    df_data_by_mon = (
        df_data_by_mon.groupby(by=["month_year", "status_code"])
        .agg({"mb": "sum"})
        .reset_index()
    )
    df_data_by_mon["gb"] = df_data_by_mon.mb.div(1024)

    # Total requests on a monthly basis
    df_req_by_mon = df.copy()
    df_req_by_mon = df_req_by_mon.value_counts(
        subset=["month_year", "status_code"]
    ).reset_index(name="requests")

    # Total data accessed on a quarterly basis
    df_data_by_qt = group_by_quarter(df_data_by_mon)
    # Total requests on a quarterly basis
    df_req_by_qt = group_by_quarter(df_req_by_mon)

    # Generate final quarterly report
    merge_cols = ["fy_quarter", "fiscal_year", "quarter", "start_date", "end_date"]
    df_qt_report = pd.merge(
        df_data_by_qt,
        df_req_by_qt,
        on=merge_cols,
        how="inner",
    )
    # Reorder columns for printing output
    df_qt_report = df_qt_report[[*merge_cols, "gb", "requests"]]
    print(df_qt_report)

    # Plot results
    # plot_requests_by_month(df, project="E3SM")
    plot_qt_report(df_qt_report, project="E3SM", fiscal_year="20")
