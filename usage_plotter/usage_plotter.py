import pandas as pd
from dotenv.main import dotenv_values, find_dotenv

from usage_plotter.parse import gen_quarterly_report, parse_logs
from usage_plotter.plot import plot_report

CONFIG = dotenv_values(find_dotenv())


def main():
    logs_path = CONFIG.get("LOGS_PATH")
    if logs_path is None or logs_path == "":
        raise ValueError("LOGS_PATH is not set in .env file!")

    df: pd.DataFrame = parse_logs(logs_path)

    # E3SM quarterly report
    # =====================
    df_e3sm = df[df.project == "E3SM"]

    # Time frequency
    df_e3sm_tf = gen_quarterly_report(df_e3sm, facet="time_frequency")
    plot_report(df_e3sm_tf, project="E3SM", facet="time_frequency", fiscal_year=20)

    # E3SM in CMIP6 quarterly report
    # ==============================
    df_e3sm_cmip6 = df[df.project == "E3SM in CMIP6"]

    # Activity
    df_e3sm_cmip6_activity = gen_quarterly_report(df_e3sm_cmip6, facet="activity")
    plot_report(
        df_e3sm_cmip6_activity,
        project="E3SM in CMIP6",
        facet="activity",
        fiscal_year=20,
    )


if __name__ == "__main__":
    main()
