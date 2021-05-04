from typing import Literal

import pandas as pd

from usage_plotter.parse import Project


def plot_report(
    df: pd.DataFrame,
    project: Project,
    facet: str,
    fiscal_year: Literal[19, 20, 21] = 21,
):
    """Plots the quarterly report for a fiscal year for a specific facet.

    :param df: [description]
    :type df: pd.DataFrame
    :param project: [description]
    :type project: Project
    :param facet: [description]
    :type facet: str
    :param fiscal_year: [description], defaults to 21
    :type fiscal_year: Literal[19, 20, 21], optional
    """
    df_fy = df.loc[df["fiscal_year"] == str(fiscal_year)]

    pd.pivot_table(
        df_fy,
        index=["quarter"],
        values="gb",
        columns=facet,
        aggfunc="sum",
    ).plot(
        title=f"{project} FY{fiscal_year} Total Requests ",
        xlabel="Quarter",
        ylabel="Requests",
        kind="bar",
        stacked=True,
        rot=0,
        figsize=(10, 10),
    )

    pd.pivot_table(
        df_fy,
        index="quarter",
        values="requests",
        columns=facet,
        aggfunc="sum",
    ).plot(
        title=f"{project} FY{fiscal_year} Total Data Access (GB) ",
        xlabel="Quarter",
        ylabel="Data (GB)",
        kind="bar",
        stacked=True,
        rot=0,
        figsize=(10, 10),
    )
