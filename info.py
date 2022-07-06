import pymongo
import json
import telethon

# 配置文件
with open('config.json', 'r', encoding='utf8') as r:
    config = json.load(r)
exclude_name = config['exclude_name']
    
# mongo 数据库
my_db = pymongo.MongoClient(config['mongo_url'])[config['mongo_db']]
db_messages = my_db['messages']
db_dialogs = my_db['dialogs']

# telegram 客户端
client = telethon.TelegramClient('session_name', config['api_id'], config['api_hash'])
