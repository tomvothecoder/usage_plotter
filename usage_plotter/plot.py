from typing import TYPE_CHECKING, List

import pandas as pd
from matplotlib import pyplot as plt

from usage_plotter.log import logger
from usage_plotter.parse import E3SM_CY_TO_FY_MAP, ProjectTitle

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure


def plot_report(
    df: pd.DataFrame,
    project_title: ProjectTitle,
    facet: str,
):
    """Generates a figure of subplots consisting of stacked (by facet) line plots.

    :param df: DataFrame containing monthly report
    :type df: pd.DataFrame
    :param project: Name of the project for the subplot titles
    :type project: Project
    :param facet: Facet to stack line charts on
    :type facet: str
    """
    fiscal_yrs = df.fiscal_yr.unique()

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

        fig = modify_legend(fig, legend_labels=df[facet].unique())
        ax = modify_xtick_labels(fig, ax, int(fiscal_yr))

        # Save outputs for analysis
        filename = gen_filename(project_title, fiscal_yr)
        df_fy.to_csv(f"{filename}.csv")
        fig.savefig(filename, dpi=fig.dpi, facecolor="w")


def modify_legend(fig: "Figure", legend_labels: List[str]) -> "Figure":
    """Adds a shared legend at the bottom for clarity.

    :param fig: Figure object
    :type fig: [Figure]
    :param legend_labels: Labels for the legend, which are the unique facet option names
    :type legend_labels: List[str]
    :return: Returns the Figure object with a modified legend
    :rtype: [Figure]
    """
    fig.legend(labels=legend_labels, loc="lower center", ncol=len(legend_labels))
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1)

    return fig


def modify_xtick_labels(fig: "Figure", ax: "Axes", fiscal_yr: int) -> "Figure":
    """Modifies the xtick labels to display the calendar month/year as a str.

    For example, for FY 2021, the first and last xtick labels are 07/2020 and
    06/2021 respectively.

    It also adds a vertical line and text that separates each quarter.

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
        quarter = 1
        for tick in range(1, 13):
            end_of_quarter = tick % 3 == 0
            if end_of_quarter:
                ax[i].axvline(x=tick, color="gray", linestyle="--", lw=2)
                ax[i].text(
                    tick,
                    0,
                    f"Q{quarter}",
                    rotation=90,
                    fontstyle="oblique",
                    horizontalalignment="right",
                    verticalalignment="center",
                )
                quarter += 1
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


def gen_filename(project_title: ProjectTitle, fiscal_yr: int) -> str:
    """Generates the filename for output files (e.g., .csv and .png).

    :param project_title: The title of the project
    :type project_title: ProjectTitle
    :param fiscal_yr: Fiscal year
    :type fiscal_year: int
    :return: The name of the file
    :rtype: str
    """
    output_dir = "outputs"
    filename = (
        f"{output_dir}/FY{fiscal_yr}_{project_title.replace(' ', '_')}_quarterly_report"
    )

    return filename
