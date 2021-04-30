import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import matplotlib.pyplot as plt
import numpy as np  # noqa
import pandas as pd
from tqdm import tqdm

REALMS = ["ocean", "atmos", "land", "sea-ice"]
DATA_TYPES = ["time-series", "climo", "model-output", "mapping", "restart"]
CAMPAIGNS = ["BGC-v1", "Cryosphere-v1", "DECK-v1", "HighResMIP-v1"]
SCIENCE_DRIVERS = ["Biogeochemical Cycle", "Cryosphere", "Water Cycle"]

"""
file_id_template_ = %(source)s.%(model_version)s.%(experiment)s.%(grid_resolution)s.%(realm)s.%(regridding)s.%(data_type)s.%(time_frequency)s.%(ensemble_member)s
directory_format_template_ = %(root)s/%(source)s/%(model_version)s/%(experiment)s/%(grid_resolution)s/%(realm)s/%(regridding)s/%(data_type)s/%(time_frequency)s/%
"""


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
    """Fetch the logs from the specified path.

    :param path:
    :return:
    """
    for root, dirs, files in os.walk(path):
        if not files:
            continue
        if dirs:
            continue
        for file in files:
            yield str(Path(root, file).absolute())


def filter_lines(path: str):
    """Filter lines for parameters specific to a project.

    Currently, it only supports E3SM
    # TODO: Support E3SM project in CMIP6
    :param path:
    :return:
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


def parse_file_id(log_row):
    """Parse file id for metadata.

    Example: 'E3SM.1_0.historical.1deg_atm_60-30km_ocean.sea-ice.180x360.model-output.mon.ens1.v1'

    :param file_id:
    :return:
    """
    try:
        idx = log_row["full_path"].index("user_pub_work") + len("user_pub_work") + 1
    except ValueError:
        idx = None
        print("ERROR: " + log_row["full_path"])

    log_row["file_id"] = ".".join(log_row["full_path"][idx:].split("/")[:-1])

    facets = log_row["file_id"].split(".")
    log_row["realm"] = extract_facet(facets, "realm", REALMS)
    log_row["data_type"] = extract_facet(facets, "data_type", DATA_TYPES)
    log_row["science_driver"] = extract_facet(facets, "science_driver", SCIENCE_DRIVERS)
    log_row["campaign"] = extract_facet(facets, "campaign", CAMPAIGNS)

    return log_row


def extract_facet(
    file_facets: List[str], facet_name: str, options: List[str]
) -> Optional[str]:
    facet = None
    for option in options:
        if option in file_facets:
            facet = option

    return facet


def parse_timestamp(timestamp: str, log_row: Dict[str, Any]) -> Dict[str, Any]:
    """Extract string date ('30/Aug/2019') and convert to date object ('2019-08-30').

    :param timestamp:
    :param log_row:
    :return:
    """
    timestamp_str = timestamp[timestamp.find("[") + 1 : timestamp.find(":")]
    log_row["date"] = datetime.strptime(timestamp_str, "%d/%b/%Y").date()
    log_row["year"] = log_row["date"].year
    log_row["month"] = log_row["date"].month

    return log_row


def identify_requester(ip: str) -> None:
    # try:
    #     log_row['requester_id'] = IPWhois.lookup_rdap(IPWhois(log_row.get('requester_ip')))
    # except exceptions.IPDefinedError:
    #     log_row['requester_id'] = None
    pass


def parse_log_line(log_line: str):
    """Parse Apache log line to extract HTTP request info.

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

    :param log_line:
    :return:
    """
    attrs = log_line.split()
    log_row: Dict[str, Any] = {
        "log": log_line,
        "date": None,
        "year": None,
        "month": None,
        "requester_ip": attrs[0],
        "full_path": attrs[6],
        "status_code": attrs[8],
        "bytes": attrs[9],
        "mb": bytes_to(attrs[9], "mb") if "-" not in attrs[9] else 0,
    }

    log_row = parse_timestamp(attrs[3], log_row)
    log_row = parse_file_id(log_row)
    return log_row


def reqs_by_mon_yr(df: pd.DataFrame) -> pd.DataFrame:
    df_reqs = df.copy()

    df_reqs = df.value_counts(subset=["month_year", "status_code"]).reset_index(
        name="requests"
    )

    return df_reqs


def data_by_mon_yr(df: pd.DataFrame) -> pd.DataFrame:
    df_data = df.copy()

    df_data = (
        df_data.groupby(by=["month_year", "status_code"])
        .agg({"mb": "sum"})
        .reset_index()
    )
    df_data["gb"] = df_data.mb.div(1024)

    return df_data


def group_by_quarter(df: pd.DataFrame) -> pd.DataFrame:
    df_gb_mon_yr = df.copy()
    # Set index to month_year in order to resample on quarters
    df_gb_mon_yr = df_gb_mon_yr.set_index("month_year")

    df_gb_qt: pd.DataFrame = (
        df_gb_mon_yr.resample("Q-JUL", convention="end").sum().reset_index()
    )
    df_gb_qt.rename({"month_year": "year_quarter"}, axis=1, inplace=True)  # noqa

    df_gb_qt["year"] = df_gb_qt.year_quarter.dt.strftime("%f")
    df_gb_qt["quarter"] = df_gb_qt.year_quarter.dt.strftime("%q")

    return df_gb_qt


def plot_qt_report(
    df: pd.DataFrame,
    project: str,
    fiscal_year: Literal["19", "20", "21"] = "21",
):
    df_fy = df.loc[df["year"] == fiscal_year]

    plot_data = df_fy.plot(
        title=f"{project} FY{fiscal_year} Total Data Access ",
        kind="bar",
        x="quarter",
        y=["gb"],
        legend=None,
        rot=0,
    )
    plot_data.set(xlabel="Quarter", ylabel="Total Data (GB)")
    plt.show()

    plot_reqs = df_fy.plot(
        title=f"{project} FY{fiscal_year} Total Requests ",
        kind="bar",
        x="quarter",
        y=["requests"],
        legend=None,
        rot=0,
    )
    plot_reqs.set(xlabel="Quarter", ylabel="Total Requests")
    plt.show()

    # fig = plot_data.get_figure()
    # fig.savefig(f"e3sm_requests_by_month_{year}", dpi=fig.dpi, facecolor="w")


def plot_requests_by_month(df: pd.DataFrame, project: str):
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


def main():
    root_dir = "../access_logs"
    requests = []
    for log in tqdm(get_logs(root_dir)):
        for line in filter_lines(log):
            row = parse_log_line(line)
            requests.append(row)

    columns = [
        "log",
        "date",
        "year",
        "month",
        "requester_ip",
        "requester_id",
        "request_method",
        "full_path",
        "status_code",
        "bytes",
        "mb",
        "file_id",
        "realm",
        "data_type",
        "science_driver",
        "campaign",
    ]

    df = pd.DataFrame(requests, columns=columns)
    df["date"] = pd.to_datetime(df["date"])
    df["month_year"] = df["date"].dt.to_period("M")

    # Total data accessed by quarter
    df_data_by_mon_yr = data_by_mon_yr(df)
    df_data_by_mon_yr = df_data_by_mon_yr[
        df_data_by_mon_yr.status_code.str.contains("200|206")
    ]
    df_data_by_qt = group_by_quarter(df_data_by_mon_yr)
    print(df_data_by_qt)

    # Total requests by quarter
    df_req_by_mon_yr = df.copy()
    df_req_by_mon_yr = reqs_by_mon_yr(df_req_by_mon_yr)
    df_req_by_qt = group_by_quarter(df_req_by_mon_yr)
    print(df_req_by_qt)

    # Generate final quarterly report
    df_qt_report = pd.merge(
        df_data_by_qt, df_req_by_qt, on=["year", "quarter", "year_quarter"], how="inner"
    )
    df_qt_report = df_qt_report[["year_quarter", "year", "quarter", "gb", "requests"]]
    print(df_qt_report)

    # Plot results
    # plot_requests_by_month(df, project="E3SM")
    plot_qt_report(df_qt_report, project="E3SM", fiscal_year="19")


if __name__ == "__main__":
    main()
