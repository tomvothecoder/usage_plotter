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
    base_config = {"kind": "line", "stacked": True, "rot": 0, "legend": False}

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

    labels = df[facet].unique()

    fig.legend(labels=labels, loc="lower center", ncol=len(labels))
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fig.savefig(
        f"outputs/{project}_quarterly_report_FY{fiscal_year}_{timestamp}",
        dpi=fig.dpi,
        facecolor="w",
    )
