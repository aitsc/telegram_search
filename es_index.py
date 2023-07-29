import logging
from elasticsearch.helpers import parallel_bulk
from collections import deque
import time
import traceback
from tqdm import tqdm
import pytz
from datetime import datetime
import argparse
import os, sys
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
            "number_of_shards": 4,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "ik_smart"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "date": {
                    "type": "date",
                    "format": "date_optional_time||epoch_millis"
                },
                "create_date": {
                    "type": "date",
                    "format": "date_optional_time||epoch_millis"
                },
                "message": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                },
                "reply_msg": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                },
                "file_name": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                },
                "dialog": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                        }
                    },
                },
                "user_fn": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                        }
                    },
                },
                "username": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                        }
                    },
                },
                "is_group": {
                    "type": "boolean",
                },
                "id": {
                    "type": "long",
                },
                "reply_id": {
                    "type": "long",
                },
                "dialog_id": {
                    "type": "long",
                },
                "user_id": {
                    "type": "long",
                },
                # file 信息
                "file_ext": {
                    "type": "keyword",
                },
                "file_size": {
                    "type": "long",
                },
            }
        }
    }
    client.indices.create(index=index_name, **index)
    logging.warning(index_name + ' is created.')


def delete_index(client, index_name):
    client.indices.delete(index=index_name)
    logging.warning(index_name + ' is deleted')


def es_bulk(client, index_name, data, op_type='index'):
    logging.debug('op_type:'+op_type+' ,number is:' + str(len(data)))

    def generate_actions():
        for item in data:
            _id = item.pop('_id')
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


def doc_to_es(item: dict, dialog_id8info):
    reply_msg, reply_id = '', -1
    if 'reply_to' in item and 'reply_to_msg_id' in item['reply_to']:
        reply = db_messages.find_one({'dialog_id': item['dialog_id'], 'id': item['reply_to']['reply_to_msg_id']})
        if reply:
            reply_msg = reply['message']
            reply_id = reply['id']
    dialog_info = dialog_id8info[item['dialog_id']]
    return {
        '_id': str(item['_id']),
        'dialog': dialog_info['title'],
        'is_group': dialog_info['is_group'],
        'message': item['message'],
        'date': item['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
        'create_date': item['acquisition_time'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
        **({'reply_msg': reply_msg} if reply_msg else {}),
        **({'user_fn': item['user_fn']} if 'user_fn' in item else {}),
        **({'username': item['username']} if 'username' in item else {}),
        'id': item['id'],
        **({'reply_id': reply_id} if reply_msg else {}),
        'dialog_id': item['dialog_id'],
        **({'user_id': item['user_id']} if 'user_id' in item else {}),
        'file_name': item.get('file_name'),
        'file_ext': item.get('file_ext'),
        'file_size': item.get('media', {}).get('document', {}).get('size'),
    }


def update_mongo_to_es(client, index_name):
    if not client.indices.exists(index=index_name):
        es_creat(client, index_name)
    body = {
        "size": 1,
        "query": {
            "match_all": {}
        },
        "sort": {
            'create_date': {"order": "desc"}
        }
    }
    es_res = client.search(index=index_name, **body)
    if es_res['hits']['hits']:
        start_time = es_res['hits']['hits'][0]['_source']['create_date']
        start_time = datetime.strptime(start_time, r"%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC).timestamp()
        start_time += 0.001
        _filter = {'acquisition_time': {'$gte': datetime.fromtimestamp(start_time, tz=pytz.utc)}}
    else:
        _filter = {}
    info = db_messages.find(_filter).sort('acquisition_time', 1)

    dialog_id8info = {item['id']: {
        'title': item['name'][0]['title'],
        'is_group': item['is_group'],
    } for item in db_dialogs.find({})}
    upsert_data = []
    upsert_num = 0
    first_data = next(info, None)
    
    if first_data is not None:
        count = db_messages.count_documents(_filter)
        upsert_data.append(doc_to_es(first_data, dialog_id8info))
        for item in tqdm(info, f'{datetime.now()} 数据索引中', total=count):
            if len(upsert_data) >= 3000:
                es_bulk(client, index_name, upsert_data)
                upsert_data = []
            upsert_data.append(doc_to_es(item, dialog_id8info))
        if upsert_data:
            es_bulk(client, index_name, upsert_data)
        
    return {
        'num': {
            'upsert_data': upsert_num,
        }
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store_true', help='是否删除索引')
    args = parser.parse_args()
    
    index_name = 'telegram_messages'
    if args.d:
        delete_index(global_es_client, index_name)
    else:
        while True:
            try:
                info = update_mongo_to_es(global_es_client, index_name)
                if sum(info['num'].values()):
                    logging.warning(str(info))
            except:
                traceback.print_exc()
            time.sleep(600)
