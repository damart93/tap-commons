import hashlib
import os
import base64

os.environ['HASH_KEY'] = "test"


def hash_string(s):
    s = s.lower().strip()+os.environ['HASH_KEY']
    return base64.b64encode(hashlib.sha256((s.lower().strip()+os.environ['HASH_KEY']).encode('utf8')).digest()).decode('utf8')


def bulk_hash(data):
    answer = {}
    for element in data:
        answer[element] = hash_string(element)
    return answer
