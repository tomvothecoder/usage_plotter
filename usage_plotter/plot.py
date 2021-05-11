from typing import TYPE_CHECKING, List, Optional

import pandas as pd
from matplotlib import pyplot as plt

from usage_plotter.log import logger
from usage_plotter.parse import E3SM_CY_TO_FY_MAP, ProjectTitle

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure


# Output directory for DataFrame reports and plots
OUTPUT_DIR = "outputs"


def plot_cumulative_sum(df: pd.DataFrame, project_title: ProjectTitle):
    """Plots the cumulative sum for requests and data access over a fiscal year.

    :param df: Fiscal year report
    :type df: pd.DataFrame
    :param project_title: Title of the project
    :type project_title: ProjectTitle
    """
    df_copy = df.copy()
    fiscal_yrs: List[str] = df_copy.fiscal_yr.unique()

    for fiscal_yr in fiscal_yrs:
        df_fy = df_copy[df_copy.fiscal_yr == fiscal_yr]
        df_fy["cumulative_requests"] = df_fy.requests.cumsum()
        df_fy["cumulative_gb"] = df_fy.gb.cumsum()

        fig, ax = plt.subplots(nrows=2, ncols=1, figsize=(12, 8))

        df_fy.plot(
            ax=ax[0],
            title=f"{project_title} FY{fiscal_yr} Cumulative Requests",
            x="fiscal_mon",
            y="cumulative_requests",
            xticks=range(1, 13),
            xlabel="Month",
            ylabel="Requests",
            legend=False,
        )
        df_fy.plot(
            ax=ax[1],
            title=f"{project_title} FY{fiscal_yr} Cumulative Data Access",
            x="fiscal_mon",
            y="cumulative_gb",
            xticks=range(1, 13),
            xlabel="Month",
            ylabel="Data Access (GB)",
            legend=False,
        )

        modify_fig(fig)
        modify_xtick_labels(fig, ax, int(fiscal_yr))
        save_output(fig, df_fy, project_title, fiscal_yr)


def plot_by_facet(
    df: pd.DataFrame,
    project_title: ProjectTitle,
    facet: str,
):
    """Plots the fiscal year monthly report by facet.

    :param df: Fiscal year report
    :type df: pd.DataFrame
    :param project: Name of the project for the subplot titles
    :type project: Project
    :param facet: Facet to stack line charts on
    :type facet: str
    """
    fiscal_yrs: List[str] = df.fiscal_yr.unique()

    for fiscal_yr in fiscal_yrs:
        logger.info(f"\nGenerating report and plot for {project_title} FY{fiscal_yr}")
        df_fy = df[df.fiscal_yr == fiscal_yr]

        pivot_table = pd.pivot_table(
            df_fy,
            index="fiscal_mon",
            values=["requests", "gb"],
            columns=facet,
            aggfunc="sum",
        )

        fig, ax = plt.subplots(nrows=2, ncols=1, figsize=(12, 8))
        # https://pandas.pydata.org/pandas-docs/version/0.15.2/generated/pandas.DataFrame.plot.html
        base_config: pd.DataFrame.plot.__init__ = {
            "kind": "line",
            "stacked": True,
            "legend": False,
            "style": ".-",
            "sharex": True,
            "xticks": range(1, 13),
            "xlabel": "Month",
            "rot": 0,
        }

        pivot_table.requests.plot(
            **base_config,
            ax=ax[0],
            title=f"{project_title} FY{fiscal_yr} Requests by Month ({facet})",
            ylabel="Requests",
        )
        pivot_table.gb.plot(
            **base_config,
            ax=ax[1],
            title=f"{project_title} FY{fiscal_yr} Data Access by Month ({facet})",
            ylabel="Data Access (GB)",
        )

        fig = modify_fig(fig, legend_labels=df[facet].unique())
        ax = modify_xtick_labels(fig, ax, int(fiscal_yr))

        # Save outputs for analysis
        filename = gen_filename(project_title, fiscal_yr, facet)
        df_fy.to_csv(f"{filename}.csv")
        fig.savefig(filename, dpi=fig.dpi, facecolor="w")


def modify_fig(fig: "Figure", legend_labels: Optional[List[str]] = None) -> "Figure":
    """Modifies the figure with additional configuration options.

    :param fig: Figure object
    :type fig: [Figure]
    :param legend_labels: Labels for the legend, which are the unique facet option names
    :type legend_labels: Optional[List[str]]
    :return: Returns the modified Figure object
    :rtype: [Figure]
    """
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1)

    if legend_labels is not None:
        fig.legend(labels=legend_labels, loc="lower center", ncol=len(legend_labels))

    return fig


def modify_xtick_labels(fig: "Figure", ax: "Axes", fiscal_yr: int) -> "Figure":
    """Modifies the xtick labels to display the calendar month/year as a str.

    It also adds a vertical line to separate each quarter.

    Example for FY2021: first xtick is "07/2020" and the last xtick is "06/2021"

    :param fig: Figure object
    :type fig: [Figure]
    :param ax: Axes object
    :type ax: [Axes]
    :param fiscal_yr: Fiscal year
    :type fiscal_yr: [int]
    :return: Returns the Figure object with modified xtick labels
    :rtype: [Figure]
    """
    xticklabels = gen_xticklabels(fiscal_yr)

    for i in range(len(fig.axes)):
        ax[i].set_xticklabels(xticklabels)
        for tick in range(1, 13):
            end_of_quarter = tick % 3 == 0
            if end_of_quarter:
                ax[i].axvline(x=tick, color="gray", linestyle="--", lw=2)

    return fig


def gen_xticklabels(fiscal_yr: int) -> List[str]:
    """Generates a list of xtick labels based on the E3SM CY to FY mapping.

    This is function is useful for cases where data is not available for a month
    or the rest of the year (displays value as 0).

    :param fiscal_yr: Fiscal year
    :type fiscal_yr: int
    :return: List of xtick labels
    :rtype: List[str]
    """
    labels: List[str] = []

    months = E3SM_CY_TO_FY_MAP.keys()
    mons_in_prev_yr = range(7, 13)

    for month in months:
        if month in mons_in_prev_yr:
            label = f"{month}/{fiscal_yr-1}"
        else:
            label = f"{month}/{fiscal_yr}"
        labels.append(label)

    return labels


def save_output(
    fig: "Figure",
    df: pd.DataFrame,
    project_title: ProjectTitle,
    fiscal_yr: str,
    facet: Optional[str] = None,
):
    """Saves the DataFrame report and plots to the outputs directory.

    :param fig: Figure object
    :type fig: Figure
    :param df: DataFrame report
    :type df: pd.DataFrame
    :param project_title: The title of the project
    :type project_title: ProjectTitle
    :param fiscal_yr: Fiscal year
    :type fiscal_yr: str
    :param facet: Name of the facet, defaults to None
    :type facet: Optional[str], optional
    """
    filename = gen_filename(project_title, fiscal_yr, facet)
    df.to_csv(f"{filename}.csv")
    fig.savefig(filename, dpi=fig.dpi, facecolor="w")


def gen_filename(
    project_title: ProjectTitle, fiscal_yr: str, facet: Optional[str]
) -> str:
    """Generates the filename for output files.

    :param project_title: The title of the project
    :type project_title: ProjectTitle
    :param fiscal_yr: Fiscal year
    :type fiscal_year: str
    :param facet: Name of the facet
    :type facet: str
    :return: The name of the file
    :rtype: str
    """
    filename = f"{OUTPUT_DIR}/{project_title.replace(' ', '_')}_FY{fiscal_yr}_report"
    if facet:
        filename = filename + f"_by_{facet}"

    return filename
