import hashlib
import os
import base64
import json
from jsonschema import Draft4Validator, FormatChecker
from . import exceptions


def hash_string(s):
    if "HASH_SALT" not in os.environ.keys():
        raise exceptions.HashNoSalt("Trying to hash without salt. Please set-up 'HASH_SALT' in environment in order to hash data")

    s = s.lower().strip()+os.environ['HASH_SALT']
    return base64.b64encode(hashlib.sha256((s.lower().strip()+os.environ['HASH_KEY']).encode('utf8')).digest()).decode('utf8')


def hash_records(records, fields):

    for record in records:
        for field in fields:
            if field in record:

                if record[field] is not None and not isinstance(record[field], str):
                    try:
                        value = str(record[field])
                    except UnicodeDecodeError as ex:
                        raise ex
                else:
                    value = record[field]

                record[field] = hash_string(record[field])

def modify_schema_properties(properties, fields):

    for field in fields:
        if field in properties:
            if "string" in properties[field].type:
                pass
            else:
                if "null" in properties[field].type:
                    properties.type = ['null', 'string']
                else:
                    properties.type = ['string']


def main():

    schema_ex = """{"type": "SCHEMA", "stream": "ONLINE_INFORMACION_ENTREGA", "schema": {"properties": {"ID_ONLINE_TIPO_INFORMACION_ENTREGA": {"inclusion": "available", "minimum": -32768, "maximum": 32767, "type": ["null", "integer"]}, "ID_ONLINE_INFORMACION_ENTREGA": {"inclusion": "automatic", "minimum": -9223372036854775808, "maximum": 9223372036854775807, "type": ["null", "integer"]}, "DESCRIPCION": {"inclusion": "available", "maxLength": 500, "type": ["null", "string"]}, "ID_ONLINE_ENTREGA": {"inclusion": "available", "minimum": -9223372036854775808, "maximum": 9223372036854775807, "type": ["null", "integer"]}}, "type": "object"}, "key_properties": ["ID_ONLINE_INFORMACION_ENTREGA"]}
    """
    record_ex = """{"type": "RECORD", "stream": "ONLINE_INFORMACION_ENTREGA", "record": {"ID_ONLINE_TIPO_INFORMACION_ENTREGA": 2, "ID_ONLINE_INFORMACION_ENTREGA": 51511693, "DESCRIPCION": "10", "ID_ONLINE_ENTREGA": 66623709}, "version": 1649322413157}
    """

    schema = json.loads(schema_ex)
    records = [json.loads(record_ex)]

    os.environ['HASH_KEY'] = 'test'

    fields_to_hash = ['ID_ONLINE_TIPO_INFORMACION_ENTREGA']

    modify_schema_properties(schema['schema']['properties'], fields_to_hash)

    with open("file_db2.out") as f:
        data = [json.loads(line) for line in f.readlines()]

    records = list(filter(lambda x: x['type'] == "RECORD", data))

    hash_records([r['record'] for r in records], fields_to_hash)

    validator = Draft4Validator(schema['schema'], format_checker=FormatChecker())
    validator.validate(records[0]['record'])
    validator.validate(json.loads(record_ex)['record'])

if __name__ == '__main__':
    main()
