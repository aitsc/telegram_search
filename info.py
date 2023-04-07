import pymongo
import json
import telethon
import time

# 配置文件
with open('config.json', 'r', encoding='utf8') as r:
    config = json.load(r)
exclude_name = config['exclude_name']
    
# mongo 数据库
while True:
    try:
        my_db = pymongo.MongoClient(config['mongo_url'])[config['mongo_db']]
        db_messages = my_db['messages']
        db_dialogs = my_db['dialogs']
        break
    except:
        print('数据库连接失败, 30秒后重新连接')
        time.sleep(30)

# telegram 客户端
client = telethon.TelegramClient('session_name', config['api_id'], config['api_hash'])
