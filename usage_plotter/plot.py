from datetime import datetime
from typing import Literal

import pandas as pd
from matplotlib import pyplot as plt

from usage_plotter.parse import Project


def plot_report(
    df: pd.DataFrame,
    project: Project,
    fiscal_year: Literal["2019", "2020", "2021"],
    facet: str,
):
    """Generates a plot consisting of stacked bar subplots.

    :param df: DataFrame containing report over a time interval.
    :type df: pd.DataFrame
    :param project: Name of the project for the subplot titles
    :type project: Project
    :param fiscal_year: Year of the report, FY for quarterly and CY for monthly.
    :type fiscal_year: Literal["2019", "2020", "2021"]
    :param interval: Time interval of the report.
    :type interval: Literal["quarter", "month]
    :param facet: Facet to stack dbars on.
    :type facet: str
    """
    pivot_table = pd.pivot_table(
        df,
        index="fiscal_month",
        values=["requests", "gb"],
        columns=facet,
        aggfunc="sum",
    )

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 8))
    # https://pandas.pydata.org/pandas-docs/version/0.15.2/generated/pandas.DataFrame.plot.html
    base_config: pd.DataFrame.plot.__init__ = {
        "kind": "line",
        "stacked": True,
        "legend": False,
        "style": ".-",
        "sharex": True,
        "xticks": (range(1, 13)),
        "rot": 0,
    }

    pivot_table.requests.plot(
        **base_config,
        ax=axes[0],
        title=f"{project} FY{fiscal_year} Requests by Month ({facet})",
        xlabel="Fiscal Month (July - June)",
        ylabel="Requests",
    )

    pivot_table.gb.plot(
        **base_config,
        ax=axes[1],
        title=f"{project} {fiscal_year} Data Access by Month ({facet})",
        xlabel="Fiscal Month (July - June)",
        ylabel="Data Access (GB)",
    )

    # Add vertical lines to represent quarters
    for i in range(len(fig.axes)):
        axes[i].axvline(x=3, color="blue", linestyle="--", lw=2)
        axes[i].axvline(x=6, color="blue", linestyle="--", lw=2)
        axes[i].axvline(x=9, color="blue", linestyle="--", lw=2)

    # Add legend labels at the bottom to avoid legends overlapping plot values.
    legend_labels = df[facet].unique()
    fig.legend(labels=legend_labels, loc="lower center", ncol=len(legend_labels))
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1)

    # Save figure to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fig.savefig(
        f"outputs/{project}_quarterly_report_FY{fiscal_year}_{timestamp}",
        dpi=fig.dpi,
        facecolor="w",
    )
