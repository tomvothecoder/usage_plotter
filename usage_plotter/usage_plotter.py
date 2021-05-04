import argparse

import pandas as pd

from usage_plotter.parse import gen_quarterly_report, parse_logs
from usage_plotter.plot import plot_report


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--logs_path",
        type=str,
        default="access_logs",
        help="The path to the ESGF Apache access logs (default: access_logs)",
        required=False,
    )
    parser.add_argument(
        "--fy",
        type=int,
        default=21,
        choices=range(19, 22),
        help="An integer for an E3SM IG fiscal year to generate report on (default: 21).",
        required=False,
    )
    parser.add_argument(
        "--debug",
        help="Add this argument for a debug run, which does not save outputs.",
        action="store_true",
    )
    return parser.parse_args()


def main():
    # Configuration
    parsed_args = parse_args()
    logs_path = parsed_args.logs_path
    fiscal_year = parsed_args.fy

    # Raw logs parsed into a DataFrame
    df: pd.DataFrame = parse_logs(logs_path)

    # E3SM quarterly report
    # =====================
    df_e3sm = df[df.project == "E3SM"]

    # Time frequency
    df_e3sm_tf = gen_quarterly_report(df_e3sm, facet="time_frequency")
    plot_report(
        df_e3sm_tf, project="E3SM", facet="time_frequency", fiscal_year=fiscal_year
    )

    # E3SM in CMIP6 quarterly report
    # ==============================
    df_e3sm_cmip6 = df[df.project == "E3SM in CMIP6"]

    # Activity
    df_e3sm_cmip6_activity = gen_quarterly_report(df_e3sm_cmip6, facet="activity")
    plot_report(
        df_e3sm_cmip6_activity,
        project="E3SM in CMIP6",
        facet="activity",
        fiscal_year=fiscal_year,
    )


if __name__ == "__main__":
    main()
