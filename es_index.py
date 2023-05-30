import logging
from elasticsearch.helpers import parallel_bulk
from collections import deque
import time
import traceback
from tqdm import tqdm
import pytz
from datetime import datetime
import os ,sys
sys.path.append(os.getcwd())

from info import global_es_client, db_dialogs, db_messages


log_format = '%(asctime)s %(levelname)s - %(message)s'
logging.basicConfig(level=logging.WARN, format=log_format)


def es_creat(client, index_name):
    es_exist = client.indices.exists(index=index_name)
    if es_exist:
        logging.debug(index_name+' is existed.')
        return
    index = {
        "settings": {
            "index": {
                "codec": "best_compression",
                "number_of_shards": 4,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "default": {
                            "type": "ik_smart"
                        }
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "dialog": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart"
                },
                "message": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart"
                },
                "date": {
                    "type": "text"
                },
                "user_fn": {
                    "type": "text"
                },
                "username": {
                    "type": "text"
                },
                "date_timestamp": {
                    "type": "double"
                },
                "id": {
                    "type": "long",
                },
                "dialog_id": {
                    "type": "long",
                },
                "user_id": {
                    "type": "long"
                },
            }
        }
    }
    client.indices.create(index=index_name, body=index)
    logging.warn(index_name + ' is created.')


def delete_index(client, index_name):
    client.indices.delete(index=index_name)
    logging.warn(index_name + ' is deleted')


def es_bulk(client, index_name, data, op_type='index'):
    logging.debug('op_type:'+op_type+' ,number is:' + str(len(data)))

    def generate_actions():
        for item in data:
            _id = item['_id']
            del item['_id']
            if op_type == 'index':
                yield {
                    '_op_type': op_type,
                    "_index": index_name,
                    "_id": _id,
                    "_source": item
                }
    deque(parallel_bulk(client=client, actions=generate_actions(),
                        chunk_size=3000, thread_count=8), maxlen=0)
    logging.debug('op_type:'+op_type+' ,number is:' + str(len(data))+' is done')
    return True


def update_mongo_to_es(client, index_name):
    if not client.indices.exists(index=index_name):
        es_creat(client, index_name)
    body = {
        "size": 1,
        "query": {
            "match_all": {}
        },
        "sort": {
            'date_timestamp': {"order": "desc"}
        }
    }
    es_res = client.search(index=index_name, body=body)
    if es_res['hits']['hits']:
        start_time = es_res['hits']['hits'][0]['_source']['date_timestamp']
        info = db_messages.find({'date': {'$gt': datetime.fromtimestamp(start_time)}})
    else:
        info = db_messages.find({})

    dialog_id8name = {item['id']: item['name'][0]['title'] for item in db_dialogs.find({})}
    upsert_data = []
    upsert_num = 0
    for item in tqdm(info, '数据索引中'):
        if len(upsert_data) >= 10 ** 4:
            es_bulk(client, index_name, upsert_data)
            upsert_data = []
        upsert_data.append({
            '_id': str(item['_id']),
            'dialog': dialog_id8name[item['dialog_id']],
            'message': item['message'],
            'date': str(item['date']),
            'user_fn': item.get('user_fn', ''),
            'username': item.get('username', ''),
            'date_timestamp': item['date'].replace(tzinfo=pytz.utc).timestamp(),
            'id': item['id'],
            'dialog_id': item['dialog_id'],
            'user_id': item.get('user_id', -1),
        })
    if upsert_data:
        es_bulk(client, index_name, upsert_data)
        
    return {
        'num': {
            'upsert_data': upsert_num,
        }
    }


if __name__ == "__main__":
    # delete_index(global_es_client, 'telegram')
    while True:
        try:
            info = update_mongo_to_es(global_es_client, 'telegram')
            if sum(info['num'].values()):
                logging.warn(str(info))
        except:
            traceback.print_exc()
        time.sleep(10)
