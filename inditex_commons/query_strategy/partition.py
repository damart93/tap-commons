from datetime import datetime, timedelta
import json

VALID_TYPES = ['date-time', 'integer']


def get_partition_column_type(column_name, properties):
    if column_name not in properties:
        raise Exception("Partition column is not in schema")
    for t in VALID_TYPES:
        if "integer" in properties[column_name]['type']:
            return "integer"
        elif "string" in properties[column_name]['type'] and 'date-time' == properties[column_name]['format']:
            return "date-time"
    raise Exception(f"Partition column type ({properties[column_name]['type']}) is not valid partition column ({VALID_TYPES})")


def get_sql_partition_values(column_name, schema, table, additional_filters=""):

    query = """SELECT min({}),max({})
                FROM {}.{}""".format(column_name, column_name, schema, table)
    if additional_filters != "":
        query += " WHERE {}".format(additional_filters)
    return query


def partition_clauses_integer(column_name, min_val, max_val, parts):
    diff = max_val - min_val
    if diff < parts:
        parts = diff
    intervals = [min_val + round(diff / parts * i) for i in range(0, parts) ] + [ max_val ]
    clauses = []

    for i in range(len(intervals) - 1):
        clauses.append(" {} >= {} AND {} < {} "
                       .format(column_name, intervals[i], column_name, intervals[i+1]))
    clauses[-1] = clauses[-1].replace("<", "<=")

    return clauses


def partition_clauses_datetime(column_name, min_date, max_date, parts):
    microseconds_diff = int((max_date - min_date) / timedelta(microseconds=1))
    dates = [min_date + timedelta(microseconds=i) for i in range(0, microseconds_diff, int(microseconds_diff / parts) ) ] + [ max_date ]
    clauses = []

    for i in range(len(dates) - 1):
        clauses.append(" {} >= to_timestamp('{}') AND < to_timestamp('{}') "
                       .format(column_name, dates[i].isoformat(), dates[i+1].isoformat()))
    clauses[-1] = clauses[-1].replace("<", "<=")

    return clauses


def get_partition_clauses(column_name, column_type, values, parts):

    min_val, max_val = values

    if column_type == "date-time":
        return partition_clauses_datetime(column_name, min_val, max_val, parts)
    elif column_type == "integer":
        return partition_clauses_integer(column_name, min_val, max_val, parts)


def main():

    table = "TABLE"
    table_schema = "TEST"
    partition_column = "FECHA_ALTA"
    additional_filter = "coalesce(fecha_modificacion, fecha_alta) >= trunc(sysdate - 5)"

    schema_ex = """{"type": "SCHEMA", "stream": "DMCOMERCIAL-DIM_ARTICULO_COLOR_TALLA", "schema": {"properties": {"CALIDAD_NUEVA": {"minimum": -999999, "maximum": 999999, "type": ["null", "integer"]}, "COLOR": {"minimum": -999999, "maximum": 999999, "type": ["null", "integer"]}, "COSTE": {"multipleOf": 1e-38, "type": ["null", "number"]}, "DESCRIPCION": {"maxLength": 120, "type": ["null", "string"]}, "FECHA_ALTA": {"format": "date-time", "type": ["null", "string"]}, "FECHA_MODIFICACION": {"format": "date-time", "type": ["null", "string"]}, "ID_ARTICULO": {"minimum": -9999999999999999999, "maximum": 9999999999999999999, "type": ["null", "integer"]}, "ID_ARTICULO_COLOR": {"minimum": -9999999999999999999, "maximum": 9999999999999999999, "type": ["null", "integer"]}, "ID_ARTICULO_COLOR_TALLA": {"minimum": -9999999999999999999, "maximum": 9999999999999999999, "type": ["integer"]}, "ID_CAMPANA": {"minimum": -99999999999, "maximum": 99999999999, "type": ["null", "integer"]}, "ID_CENTRO_COMPRA": {"minimum": -99999999999, "maximum": 99999999999, "type": ["null", "integer"]}, "ID_PROCESO_ETL": {"minimum": -99999999999, "maximum": 99999999999, "type": ["null", "integer"]}, "INACTIVO": {"minimum": -999999, "maximum": 999999, "type": ["null", "integer"]}, "MODELO": {"minimum": -999999, "maximum": 999999, "type": ["null", "integer"]}, "PVP_SALDO": {"multipleOf": 1e-38, "type": ["null", "number"]}, "PVP_TEMPORADA": {"multipleOf": 1e-38, "type": ["null", "number"]}, "TALLA": {"minimum": -999999, "maximum": 999999, "type": ["null", "integer"]}}, "type": "object"}, "key_properties": ["ID_ARTICULO_COLOR_TALLA"]}
    """

    schema = json.loads(schema_ex)

    partition_column_type = get_partition_column_type(partition_column, schema['schema']['properties'])

    s = get_sql_partition_values(partition_column, table, table_schema, additional_filter)

    import dateutil.parser as parser

    if partition_column_type == "integer":
        values = (13, 38)
    elif partition_column_type == "date-time":
        values = tuple(parser.parse(d) for d in ("2021/08/05", "2021/09/17"))

    where_clauses = get_partition_clauses(partition_column, partition_column_type, values, 5)
    print(where_clauses)

if __name__ == '__main__':
    main()