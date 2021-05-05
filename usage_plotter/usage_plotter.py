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
        "--fiscal_year",
        "-fy",
        type=str,
        choices=("2019", "2020", "2021"),
        default="2020",
        help="A string for reporting E3SM Infrastructure Group fiscal year(default: 2021).",
        required=False,
    )
    if console:
        return parser.parse_args([])
    return parser.parse_args()


def main():
    # Configuration
    # =============
    parsed_args = parse_args()
    logs_path = parsed_args.logs_path
    fiscal_year = parsed_args.fiscal_year

    print("\nGenerating report with the following config:")
    print(f"- Logs Path: {logs_path}")
    print(f"- Fiscal Year: {fiscal_year}")
    print("\n")

    # Initial log parsing
    # ===================
    df: pd.DataFrame = parse_logs(logs_path)

    # E3SM report
    # ===========
    df_e3sm = df[df.project == "E3SM"]

    # By time frequency
    df_e3sm_tf = gen_report(df_e3sm, facet="time_frequency")
    plot_report(
        df_e3sm_tf,
        project="E3SM",
        fiscal_year=fiscal_year,
        facet="time_frequency",
    )
    # df_e3sm_tf.to_csv(
    #     f"outputs/E3SM_{interval}ly_report_{year_title}_{timestamp}",
    # )

    # E3SM in CMIP6 report
    # ====================
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_e3sm_cmip6 = df[df.project == "E3SM in CMIP6"]

    # By activity
    df_e3sm_cmip6_activity = gen_report(df_e3sm_cmip6, facet="activity")
    # df_e3sm_cmip6_activity.to_csv(
    #     f"outputs/E3SM in CMIP6_{interval}ly_report_{year_title}_{timestamp}",
    # )

    plot_report(
        df_e3sm_cmip6_activity,
        project="E3SM in CMIP6",
        fiscal_year=fiscal_year,
        facet="activity",
    )


if __name__ == "__main__":
    main()
