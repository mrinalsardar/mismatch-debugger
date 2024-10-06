"""
task 3: keep going even if there is no result returned from one source
(for null in s1, take filter from s2 and vice versa)
"""

import os
import logging
import pymssql
import snowflake
import pandas as pd
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

logFormatter = logging.Formatter(
    "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logfile_fqn = os.path.dirname(os.path.abspath(__file__)).replace(
    ".py", f"{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
)
fileHandler = logging.FileHandler(logfile_fqn)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


def get_sqlserver_connection():
    conn = pymssql.connect(
        server="server_name",
        database="db_name",
    )
    return conn


def get_connection(adapter, conn_properties):
    if adapter == "sqlserver":
        conn = pymssql.connect(**conn_properties)
    elif adapter == "snowflake":
        conn = snowflake.connector.connect(**conn_properties)
    else:
        logger.error(f"Unsupported adapter: {adapter}")

    return conn


# def get_base_query():
#     pass


def extract_data_from_table(adapter, conn_properties, query):
    try:
        st = time.time()
        logger.debug(f"Acquiring connection to {adapter}")
        conn = get_connection(adapter, conn_properties)

        logger.debug(f"Executing query: {query}")
        cursor = conn.cursor()
        cursor.execute(query)

        if adapter == "sqlserver":
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[col[0] for col in cursor.description])
        elif adapter == "snowflake":
            df = cursor.fetch_pandas_all()
        else:
            logger.error(f"Unsupported adapter: {adapter}")

        logger.debug(f"Query executed in {time.time() - st}s")
        return df
    except Exception as e:
        logger.error(f"Query execution failed with error: {e}")
    finally:
        conn.close()
        cursor.close()


def get_base_path(s1_table_fqn, s2_table_fqn):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = Path(
        script_dir,
        f"{s1_table_fqn.replace('.', '__')}_{s2_table_fqn.replace('.', '__')}",
    )
    base_path = Path(base_dir).mkdir(parents=True, exist_ok=True)
    return base_path


def main():
    # Connection parameters
    sqlserver_conn_properties = {
        "server": "server_name",
        "database": "db_name",
    }
    snowflake_conn_properties = {
        "account": "<account_identifier>",
        "user": "<user>",
        "private_key_file": os.environ["SNOWSQL_PRIVATE_KEY_PATH"],
        "private_key_file_pwd": os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"],
        "warehouse": "<warehouse>",
        "database": "<database>",
        "schema": "<schema>",
    }

    s1_adapter = "sqlserver"
    s2_adapter = "snowflake"

    debug_trace = {
        "level_1": {
            "s1_table_fqn": "s1d1.s1s1.s1t1",
            "s2_table_fqn": "s2d1.s2s1.s2t1",
            "s1_pk": "pcol1",  # supports column expression
            "s2_pk": "pcol1",  # supports column expression
            "s1_mismatch_col": "mcol2",  # supports column expression
            "s2_mismatch_col": "mcol2",  # supports column expression
            "s1_filter_expr": "colx = 'y'",  # without 'where'
            "s2_filter_expr": "colx = 'y'",  # without 'where'
        },
        "level_2": {
            "propagation_type": "base",  # 'base'/'join' (only direct join supported)
            "s1_table_fqn": "s1d2.s1s2.s1t2",
            "s2_table_fqn": "s2d2.s2s2.s2t2",
            "s1_pk": "pcol2",  # supports column expression
            "s2_pk": "pcol2",  # supports column expression
            "s1_mismatch_col": "mcol2",  # supports column expression
            "s2_mismatch_col": "mcol2",  # supports column expression
            # "s1_filter_expr": "colx = 'y'",  # without 'where'
            # "s2_filter_expr": "colx = 'y'",  # without 'where'
        },
        "level_3": {
            "propagation_type": "join",  # 'base'/'join' (only direct join supported)
            "s1_table_fqn": "s1d3.s1s3.s1t3",
            "s3_table_fqn": "s2d3.s2s3.s2t3",
            "s1_pk": "pcol3",  # supports column expression
            "s3_pk": "pcol3",  # supports column expression
            "s1_mismatch_col": "mcol3",  # supports column expression
            "s3_mismatch_col": "mcol3",  # supports column expression
            # "s1_filter_expr": "colx = 'y'",  # without 'where'
            # "s2_filter_expr": "colx = 'y'",  # without 'where'
        },
    }

    s1_suffix = f"__{s1_adapter}"
    s2_suffix = f"__{s2_adapter}"

    base_path = get_base_path(
        debug_trace[0]["s1_table_fqn"], debug_trace[0]["s2_table_fqn"]
    )
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Level 1 processing
    level_1 = debug_trace["level_1"]
    logger.debug(f"level_1 colfig: {level_1}")

    nlc = debug_trace["level_2"]
    logger.debug(f"Next level config: {nlc}")

    s1_join_col_expr = f", {nlc['s1_pk']}" if nlc["propagation_type"] == "join" else ""
    s2_join_col_expr = f", {nlc['s2_pk']}" if nlc["propagation_type"] == "join" else ""

    s1_query = f"""
    SELECT DISTINCT
        {level_1["s1_pk"]}, {level_1["s1_mismatch_col"]}{s1_join_col_expr}
    FROM
        {level_1["s1_table_fqn"]}
    WHERE
        1 = 1
        {"AND " + level_1["s1_filter_expr"] if "s1_filter_expr" in level_1 else ""}
    """

    s2_query = f"""
    SELECT DISTINCT
        {level_1["s2_pk"], level_1["s2_mismatch_col"]}{s2_join_col_expr}
    FROM
        {level_1["s2_table_fqn"]}
    WHERE
        1 = 1
        {"AND " + level_1["s2_filter_expr"] if "s2_filter_expr" in level_1 else ""}
    """

    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit tasks and collect the Future objects
        future_s1_df = executor.submit(
            extract_data_from_table,
            s2_adapter,
            sqlserver_conn_properties,
            s2_query,
        )
        future_s2_df = executor.submit(
            extract_data_from_table,
            s2_adapter,
            snowflake_conn_properties,
            s2_query,
        )

        # Get the results from each Future object
        s1_df = future_s1_df.result()
        s2_df = future_s2_df.result()

    file1_fqn = Path(
        base_path,
        f"{level_1['s2_table_fqn'].replace('.', '')}_{level_1['s1_mismatch_col']}_{timestamp}.csv",
    )
    logger.debug(
        f"Writing {{level_1['s2_table_fqn'].replace('.', '')}} dataframe to {file1_fqn}"
    )
    s1_df.to_csv(file1_fqn, header=True, index=False)
    file2_fqn = Path(
        base_path,
        f"{level_1['s2_table_fqn'].replace('.', '')}_{level_1['s2_mismatch_col']}_{timestamp}.csv",
    )
    logger.debug(
        f"Writing {level_1['s2_table_fqn'].replace('.', '')} dataframe to {file2_fqn}"
    )
    s2_df.to_csv(file2_fqn, header=True, index=False)

    print(f"Dataframe from {level_1['s1_table_fqn']}:\n{s1_df.head()}")
    print(f"Dataframe from {level_1['s2_table_fqn']}:\n{s2_df.head()}")

    logger.info("Comparing datasets")
    st = time.time()
    join_df = s1_df.merge(
        s2_df,
        left_on=level_1["s1_pk"],
        right_on=level_1["s2_pk"],
        how="outer",
        suffixes=(s1_suffix, s2_suffix),
        indicator=True,
    )
    mismatch_df = join_df.loc[
        (
            join_df[level_1["s1_mismatch_col"] + s1_suffix]
            != join_df[level_1["s2_mismatch_col"] + s2_suffix]
        )
        & (join_df["_merge"].isin(("left_only", "right_only")))
    ]
    logger.debug(f"Computed mismatch in {time.time() - st}s")
    print(mismatch_df.head())

    # Extract 1 mismatch value
    first_row = mismatch_df.iloc[0]
    logger.info(f"Debugging for row: {first_row}")

    # Setup next level filter expression
    # and pd.api.types.is_string_dtype(mismatch_df[level_1['s1_pk']+s1_suffix])
    if "level_2" in debug_trace:
        s1_con_filter = (
            f"CAST({debug_trace['level_2']['s1_pk']}, VARCHAR)"
            f" = '{first_row[level_1['s1_pk']+s1_suffix]}'"
        )
        s2_con_filter = (
            f"CAST({debug_trace['level_2']['s2_pk']}, VARCHAR)"
            f" = '{first_row[level_1['s2_pk']+s1_suffix]}'"
        )
        logger.debug(
            "level_2 filters:\n"
            f"s1_con_filter: {s1_con_filter}"
            f"s2_con_filter: {s2_con_filter}"
        )
        debug_trace["level_2"]["s1_connecting_filter_expr"] = s1_con_filter
        debug_trace["level_2"]["s2_connecting_filter_expr"] = s2_con_filter

    file_fqn = Path(
        base_path,
        f"{level_1['s1_table_fqn'].split('.')[2]}_{level_1['s1_mismatch_col'].split('.')[2]}"
        f"{level_1['s2_table_fqn'].split('.')[2]}_{level_1['s2_mismatch_col'].split('.')[2]}"
        f"_{timestamp}.csv",
    )
    logger.debug(f"Writing dataframe to {file_fqn}")
    mismatch_df.to_csv(file_fqn, header=True, index=False)

    for ck in list(debug_trace.keys())[1:]:
        lc = debug_trace[ck]
        logger.debug(f"{ck} colfig: {level_1}")

        next_level = f"level_{int(ck.split('_')[1]) + 1}"
        nlc = debug_trace[next_level]
        logger.debug(f"Next level config: {nlc}")

        s1_join_col_expr = (
            f", {nlc['s1_pk']}" if nlc["propagation_type"] == "join" else ""
        )
        s2_join_col_expr = (
            f", {nlc['s2_pk']}" if nlc["propagation_type"] == "join" else ""
        )

        s1_query = f"""
        SELECT DISTINCT
            {lc["s1_pk"]}, {lc["s1_mismatch_col"]}{s1_join_col_expr}
        FROM
            {lc["s1_table_fqn"]}
        WHERE
            {lc['s1_connecting_filter_expr']}
            {"AND " + lc["s1_filter_expr"] if "s1_filter_expr" in lc else ""}
        """

        s2_query = f"""
        SELECT DISTINCT
            {lc["s2_pk"]}, {lc["s2_mismatch_col"]}{s2_join_col_expr}
        FROM
            {lc["s2_table_fqn"]}
        WHERE
            {lc['s2_connecting_filter_expr']}
            {"AND " + lc["s2_filter_expr"] if "s2_filter_expr" in lc else ""}
        """

        # Create a ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit tasks and collect the Future objects
            future_s1_df = executor.submit(
                extract_data_from_table,
                s1_adapter,
                sqlserver_conn_properties,
                s1_query,
            )
            future_s2_df = executor.submit(
                extract_data_from_table,
                s2_adapter,
                snowflake_conn_properties,
                s2_query,
            )

            # Get the results from each Future object
            s1_df = future_s1_df.result()
            s2_df = future_s2_df.result()

        print(f"Dataframe from {lc['s1_table_fqn']}:\n{s1_df.head()}")
        print(f"Dataframe from {lc['s2_table_fqn']}:\n{s2_df.head()}")

        logger.info("Comparing datasets")
        st = time.time()
        join_df = s1_df.merge(
            s2_df,
            left_on=lc["s1_pk"],
            right_on=lc["s2_pk"],
            how="inner",
            suffixes=(s1_suffix, s2_suffix),
            indicator=True,
        )
        logger.debug(f"Comparision completed in {time.time() - st}s")
        print(join_df.head())

        # Extract 1 mismatch value
        first_row = join_df.iloc[0]
        logger.info(f"Debugging for row: {first_row}")

        # Setup next level filter expression
        # and pd.api.types.is_string_dtype(mismatch_df[lc['s1_pk']+s1_suffix])
        if next_level in debug_trace:
            if nlc["propagation_type"] == "base":
                s1_con_filter = (
                    f"CAST({debug_trace[next_level]['s1_pk']}, VARCHAR)"
                    f" = '{first_row[lc['s1_pk']+s1_suffix]}'"
                )
                s2_con_filter = (
                    f"CAST({debug_trace[next_level]['s2_pk']}, VARCHAR)"
                    f" = '{first_row[lc['s2_pk']+s1_suffix]}'"
                )
            elif nlc["propagation_type"] == "join":
                s1_con_filter = (
                    f"CAST({debug_trace[next_level]['s1_pk']}, VARCHAR)"
                    f" = '{first_row[next_level['s1_pk']+s1_suffix]}'"
                )
                s2_con_filter = (
                    f"CAST({debug_trace[next_level]['s2_pk']}, VARCHAR)"
                    f" = '{first_row[next_level['s2_pk']+s1_suffix]}'"
                )
            else:
                logger.error(f"Unsupported propagation type: {nlc['propagation_type']}")

            logger.debug(
                "next_level filters:\n"
                f"s1_con_filter: {s1_con_filter}"
                f"s2_con_filter: {s2_con_filter}"
            )
            debug_trace[next_level]["s1_connecting_filter_expr"] = s1_con_filter
            debug_trace[next_level]["s2_connecting_filter_expr"] = s2_con_filter

        logger.debug(f"Writing dataframe to {file_fqn}")
        join_df.to_csv(file_fqn, header=True, index=False)

        # if match then stop
        munique = join_df["_merge"].unique()
        if len(munique) == 0 and list(munique)[0] == "both":
            logger.info("Exiting as values match")
            break


if __name__ == "__main__":
    logger.info("Starting comparison")
    init_time = time()
    main()
    end_time = time()
    logger.info(f"Finished comparison in {end_time - init_time} seconds")
