import math

def get_row_count(schema, table, additional_filters=""):

    query = ("SELECT count(1)"
                " FROM {}.{}").format(schema, table)
    if additional_filters != "":
        query += " WHERE {}".format(additional_filters)
    return query

def get_queries_by_row_number(pk_columns, columns, schema, table, db_type, total_count, max_results, additional_filter=""):
    if db_type == "ORACLE":
        pk_columns = ",".join(pk_columns)
        if additional_filter != "":
            additional_filter = "WHERE " + additional_filter
        base_query = ("SELECT {}"
                        " FROM ( SELECT {}, ROWNUM AS RNUM"
                        " FROM {}.{} T"
                        f"{additional_filter}"
                        " ORDER BY {})"
                        " WHERE RNUM >= {} AND RNUM < {}"
                        " ORDER BY RNUM")
        return [base_query.format(','.join(columns), ','.join(columns), schema, table, pk_columns, i, i + max_results) for i in range(1, total_count, max_results)]


def get_queries_by_dense_rank(pk_columns, columns, schema, table, db_type, total_count, max_results, additional_filter=""):
    if db_type == "ORACLE":
        pk_columns = ",".join(pk_columns)
        if additional_filter != "":
            additional_filter = " WHERE " + additional_filter
        base_query = ("SELECT {}"
                        " FROM ( SELECT {}, DENSE_RANK() OVER (ORDER BY {}) AS RNUM"
                        " FROM {}.{} T"
                        f"{additional_filter}"
                        " ORDER BY {})"
                        " WHERE RNUM >= {} AND RNUM < {}"
                        " ORDER BY RNUM")
        return [base_query.format(columns, pk_columns, schema, table, pk_columns, i, i + max_results) for i in range(1, total_count, max_results)]

def get_results_for_queries_number(queries, count):
    return math.ceil(count/queries)

def adjust_results_for_result_number(results, count):
    return math.ceil( count / math.ceil(count / results))

def main():

    table = "FACT_ESTIMACION"
    table_schema = "AAP_DRIVE"
    partition_column = ["ID_CADENA", "ID_FECHA", "ID_T_PROD", "ID_CAMPANA", "ID_SECCION", "ID_PLAT"]
    additional_filter = "ID_PLAT = 508"
    additional_filter = ""

    max_results = 50000
    queries = 10

    total_sql = get_row_count(table_schema, table, additional_filter)
    print(total_sql)
    rows = 308014
    # Either
    # Number of queries we want to do
    n_results = get_results_for_queries_number(queries, rows)
    print(f"Table has {rows} rows, we want {queries} queries done, set max_results:{n_results}")
    # Or adjust number of results per query
    n_results = adjust_results_for_result_number(max_results, rows)
    print(f"Table has {rows} rows, we want at most {max_results} per query, set queries: {math.ceil(rows/n_results)} with max_results:{n_results}")

    #mock rows


    queries = get_queries_by_row_number(partition_column, table_schema, table, "ORACLE", rows, max_results, additional_filter)
    for query in queries: print(query)
    queries = get_queries_by_dense_rank(partition_column, table_schema, table, "ORACLE", rows, max_results, additional_filter)
    for query in queries: print(query)

if __name__ == '__main__':
    main()
