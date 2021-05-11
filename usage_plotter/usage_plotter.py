import argparse

import pandas as pd

from usage_plotter.log import logger
from usage_plotter.parse import ProjectTitle, gen_report, parse_logs
from usage_plotter.plot import plot_by_facet, plot_cumulative_sum


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

    if console:
        return parser.parse_args([])
    return parser.parse_args()


def main():
    # Configuration
    # =============
    parsed_args = parse_args()
    logs_path = parsed_args.logs_path
    logger.info(f"\nGenerating report for access logs in `{logs_path}`\n")

    # Initial log parsing
    # ===================
    logger.info("\nParsing access logs...")
    df: pd.DataFrame = parse_logs(logs_path)

    # E3SM report
    # ===========
    e3sm_title: ProjectTitle = "E3SM"
    df_e3sm = df.loc[df.project == e3sm_title]

    # 1) Cumulatuve sum report
    df_e3sm_report = gen_report(df_e3sm)
    plot_cumulative_sum(df_e3sm_report, e3sm_title)

    # 2) Report by facet
    # Check dataset template for available facets
    report_e3sm_facets = ["time_frequency"]
    for facet in report_e3sm_facets:
        df_e3sm_by_facet = gen_report(df_e3sm, facet=facet)
        plot_by_facet(df_e3sm_by_facet, project_title=e3sm_title, facet=facet)

    # E3SM in CMIP6 report
    # ====================
    e3sm_cmip6_title: ProjectTitle = "E3SM in CMIP6"
    df_e3sm_cmip6 = df.loc[df.project == e3sm_cmip6_title]

    # 1) Cumulative sum report
    df_e3sm_cmip6_report = gen_report(df_e3sm_cmip6)
    plot_cumulative_sum(df_e3sm_cmip6_report, e3sm_cmip6_title)

    # 2) Check dataset template for available facets
    report_cmip6_facets = ["activity"]
    for facet in report_cmip6_facets:
        df_cmip6_by_facet = gen_report(df_e3sm_cmip6, facet=facet)
        plot_by_facet(df_cmip6_by_facet, project_title=e3sm_title, facet=facet)

    logger.info("\nCompleted, check the /outputs directory.")


if __name__ == "__main__":
    main()
