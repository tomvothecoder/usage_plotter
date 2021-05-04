import argparse

import pandas as pd

from usage_plotter.parse import gen_report, parse_logs
from usage_plotter.plot import plot_report


def parse_args(console: bool = False) -> argparse.Namespace:
    """Parses command line arguments to configure the software.

    :param console: Bypass argparse when using Python interactive consoles, returns default values
    :type console: bool, default False
    :return: Command line arguments
    :rtype: argparse.NameSpace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--logs_path",
        "-l",
        type=str,
        default="access_logs",
        help="The string path to the ESGF Apache access logs (default: access_logs).",
        required=False,
    )
    parser.add_argument(
        "--year",
        "-y",
        type=str,
        choices=("2019", "2020", "2021", "2022"),
        default="2021",
        help="A string for reporting E3SM IG FY (quarter) or CY (month) (default: 2021).",
        required=False,
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=str,
        choices=("quarter", "month"),
        default="quarter",
        help="The reporting interval (default: quarter).",
        required=False,
    )
    parser.add_argument(
        "--debug",
        "-d",
        help="Add this argument for a debug run, which does not save output plots and csvs.",
        action="store_true",
    )

    if console:
        return parser.parse_args([])
    return parser.parse_args()


def main():
    # Configuration
    # =============
    parsed_args = parse_args()
    logs_path = parsed_args.logs_path
    year = parsed_args.year
    interval = parsed_args.interval
    debug = parsed_args.debug

    print("\nGenerating report with the following config:")
    print(f"- Logs Path: {logs_path}")
    print(f"- Year: {year}")
    print(f"- Interval: {interval}")
    print(f"- Debug: {debug}")
    print("\n")

    # Initial log parsing
    # ===================
    df: pd.DataFrame = parse_logs(logs_path)

    # E3SM report
    # ===========
    df_e3sm = df[df.project == "E3SM"]

    # By time frequency
    df_e3sm_tf = gen_report(df_e3sm, interval=interval, facet="time_frequency")
    plot_report(
        df_e3sm_tf,
        project="E3SM",
        year=year,
        interval=interval,
        facet="time_frequency",
    )

    # E3SM in CMIP6 report
    # ====================
    df_e3sm_cmip6 = df[df.project == "E3SM in CMIP6"]

    # By activity
    df_e3sm_cmip6_activity = gen_report(
        df_e3sm_cmip6, interval=interval, facet="activity"
    )
    plot_report(
        df_e3sm_cmip6_activity,
        project="E3SM in CMIP6",
        year=year,
        interval=interval,
        facet="activity",
    )


if __name__ == "__main__":
    main()
