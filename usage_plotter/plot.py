from datetime import datetime
from typing import Literal

import pandas as pd
from matplotlib import pyplot as plt

from usage_plotter.parse import Project


def plot_report(
    df: pd.DataFrame,
    project: Project,
    year: Literal["2019", "2020", "2021"],
    interval: Literal["quarter", "month"],
    facet: str,
):
    """Generates a plot consisting of stacked bar subplots.

    :param df: DataFrame containing report over a time interval.
    :type df: pd.DataFrame
    :param project: Name of the project for the subplot titles
    :type project: Project
    :param year: Year of the report, FY for quarterly and CY for monthly.
    :type year: Literal["2019", "2020", "2021"]
    :param interval: Time interval of the report.
    :type interval: Literal["quarter", "month]
    :param facet: Facet to stack bars on.
    :type facet: str
    """
    pivot_table = pd.pivot_table(
        df,
        index=interval,
        values=["requests", "gb"],
        columns=facet,
        aggfunc="sum",
    )

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 8))
    base_config = {"kind": "bar", "stacked": True, "rot": 0, "legend": False}

    if interval == "month":
        pivot_table.requests.plot(
            **base_config,
            ax=axes[0],
            title=f"{project} CY{year} Requests by Month ({facet})",
            xlabel="Month",
            ylabel="Requests",
        )
        pivot_table.gb.plot(
            **base_config,
            ax=axes[1],
            title=f"{project} CY{year} Data Access by Month ({facet})",
            xlabel="Month",
            ylabel="Data Access (GB)",
        )

    elif interval == "quarter":
        pivot_table.requests.plot(
            **base_config,
            ax=axes[0],
            title=f"{project} FY{year} Requests by Quarter ({facet})",
            xlabel="Quarter",
            ylabel="Requests",
        )

        pivot_table.gb.plot(
            **base_config,
            ax=axes[1],
            title=f"{project} FY{year} Data Access By Quarter ({facet})",
            xlabel="Quarter",
            ylabel="Data (GB)",
        )

    labels = df[facet].unique()
    fig.legend(labels=labels, loc="lower center", ncol=len(labels))
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fig.savefig(
        f"{project}_{interval}ly_report_{year}_{timestamp}", dpi=fig.dpi, facecolor="w"
    )
