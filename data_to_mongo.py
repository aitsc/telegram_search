import telethon
from telethon.tl import custom, types
from pprint import pprint
from tqdm import tqdm
from datetime import datetime, timedelta
import bson
import time
from pymongo.collection import Collection
import pytz
import argparse
from info import get_te_client, db_dialogs, db_messages, exclude_name

client = get_te_client()


def to_int64(d):
    """递归将dict或list中的int变为bson.int64.Int64

    Args:
        d (dict, list):

    Returns:
        dict, list: d
    """
    if type(d) == dict:
        for k, v in d.items():
            if type(v) in {dict, list}:
                d[k] = to_int64(v)
            elif type(v) == int:
                d[k] = bson.int64.Int64(v)
    if type(d) == list:
        for i, v in enumerate(d):
            if type(v) in {dict, list}:
                d[i] = to_int64(v)
            elif type(v) == int:
                d[i] = bson.int64.Int64(v)
    return d


def get_dialogs(client: telethon.TelegramClient, collection: Collection = None, exclude_name=('草稿',)):
    """获取所有频道和群组的对话

    Args:
        client (telethon.TelegramClient): 客户端
        collection (Collection, optional): 要存储的mongodb表
        exclude_name (list, tuple, set, optional): 黑名单, 排除的频道和群组的名称

    Returns:
        list: 所有对话
    """
    dialog: custom.dialog.Dialog
    dialog_L = []
    exclude_name = set(exclude_name)
    new_name = old_name = upserted_count = 0
    # 开始获取对话
    for dialog in tqdm(client.iter_dialogs(), 'get_dialogs'):
        if dialog.is_user or dialog.name in exclude_name:
            continue
        # members = []
        # for member in client.iter_participants(dialog):
        #     member: types.User
        #     members.append(member)
        # print('members:', len(members), dialog.title)
        dialog_ = to_int64({
            'id': dialog.id,  # int, 对话id
            'date': dialog.date,  # datetime.datetime, 最后一个消息的发布时间
            'title': dialog.title,  # str
            'name': [{  # 改名字新的在前面
                'title': dialog.name,  # str, 对话名称
                'start': datetime.utcnow(),  # 第一次发现的时间
                'end': datetime.utcnow(),  # 最后一次发现的时间
            }],
            'is_group': dialog.is_group,  # bool, 是否是群组
            'is_channel': dialog.is_channel,  # bool, 是否是频道
        })
        if collection is not None:
            result = list(collection.find({'id': dialog_['id']}))
            if result:  # 如果匹配到了
                name = result[0]['name']
                if name[0]['title'] != dialog_['name'][0]['title']:
                    name = dialog_['name'] + name
                    new_name += 1  # 新名称
                else:
                    name[0]['end'] = dialog_['name'][0]['end']
                    old_name += 1  # 旧名称
                collection.update_one({'id': dialog_['id']}, {'$set': {'name': name, 'date': dialog_['date']}})
                dialog_['name'] = name
            else:
                collection.insert_one(dialog_)
                upserted_count += 1
        dialog_L.append(dialog_)
    print('new_name:', new_name, '; old_name:', old_name, '; upserted_count:', upserted_count)
    return dialog_L


def get_messages(client: telethon.TelegramClient, dialog_id=-1001078465602,
                 min_id=0, max_id=0, limit=None, collection: Collection = None, tqdm_desc='get_messages',
                 excluded_file_ext:set=None, reverse=True):
    """获取一个对话的所有消息

    Args:
        client (telethon.TelegramClient): 客户端
        dialog_id (int): 对话id
        min_id (int, optional): 下载的最小消息id, 大于0生效
        max_id (int, optional): 下载的最大消息id, 大于0生效
        limit (int, optional): 最多返回多少条消息
        collection (Collection, optional): 要存储的mongodb表
        tqdm_desc (str, optional): 进度条前缀, None表示不用进度条
        excluded_file_ext (set, optional): 排除的文件后缀名
        reverse (bool, optional): 是否从前向后下载, 小心修改, 改成 False 可能导致中断后丢失最新爬取点

    Returns:
        int or list: 有collection则返回数据库保存了多少消息, 否则返回所有消息list
    """
    # 使用telegram desktop直接导出涉及的一些字段: ['action', 'actor', 'actor_id', 'date', 'date_unixtime', 'duration_seconds', 'edited', 'edited_unixtime', 'file', 'forwarded_from', 'from', 'from_id', 'height', 'id', 'inviter', 'media_type', 'members', 'mime_type', 'performer', 'photo', 'poll', 'reply_to_message_id', 'saved_from', 'sticker_emoji', 'text', 'thumbnail', 'title', 'type', 'via_bot', 'width']
    message: custom.message.Message  # message
    # {media:{$ne:null},'media.photo':{$exists:false},'media.webpage':{$exists:false},'media.document':{$exists:false},'media.game':{$exists:false}}

    def to_dict(x):
        if x is not None:
            x = x.to_dict()
            del x['_']
        return x
    message_L = []
    dialog_id = bson.int64.Int64(dialog_id)
    # 单独处理部分 iter_messages 参数, 防止错误
    paras = {}
    if min_id is not None and min_id > 0:
        paras['min_id'] = min_id
    elif collection is not None:  # 自动挖掘数据库中最大的id作为爬取的初始id
        last_id = list(collection.find({'dialog_id': dialog_id}, sort=[('id', -1)], limit=1))
        if last_id:
            paras['min_id'] = last_id[0]['id']
    if max_id is not None and max_id > 0:
        paras['max_id'] = max_id
    matched_count = modified_count = upserted_count = 0
    # 开始获取消息
    bar = client.iter_messages(dialog_id, limit=limit, reverse=reverse, **paras)
    if tqdm_desc is not None:
        print(tqdm_desc)  # 有时 iter_messages 出错
        try:
            for message in client.iter_messages(dialog_id, limit=1, reverse=False):
                total = message.id  # 计算总数. 不是太准, 中间可能有删除的消息, 后面也有可能新增消息
                if 'max_id' in paras:
                    total = min(total, paras['max_id'])
                if 'min_id' in paras:
                    total -= paras['min_id']
                bar = tqdm(bar, tqdm_desc, total=min(total, limit) if limit else total)
        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
            print('ChannelPrivateError:', e)
            return 0
    skip_ext_count = 0  # 跳过后缀名的消息数量
    skip_content = 0  # 跳过空内容的消息数量
    for message in bar:
        if (message.message is None or message.message.strip() == '') and not getattr(message.file, 'name', None):
            skip_content += 1
            continue
        if getattr(message.file, 'ext', None) in excluded_file_ext:
            skip_ext_count += 1
            continue
        message_ = to_int64({
            'pinned': message.pinned,  # bool, 此消息此时是否是置顶帖子
            'id': message.id,  # int, 消息id
            'dialog_id': dialog_id,  # int, 也可以参考 message.peer_id.channel_id, telethon.tl.types.PeerChannel
            'date': message.date,  # datetime.datetime, 发布时间
            'message': message.message,  # str, 内容
            'ttl_period': message.ttl_period,  # int, 消息的生存时间, 例如一些验证, 好像有一些例外
            'fwd_from': to_dict(message.fwd_from),  # 转发标头, telethon.tl.types.MessageFwdHeader
            'reply_to': to_dict(message.reply_to),  # 回复标头, telethon.tl.types.MessageReplyHeader
            # 限制原因, telethon.tl.types.RestrictionReason
            'restriction_reason': [to_dict(i) for i in message.restriction_reason] if message.restriction_reason else None,
            'username': getattr(message.sender, 'username', None),  # str, 用户唯一名, telethon.tl.types.User
            # int, 用户id, 等价于 message.from_id.user_id (telethon.tl.types.PeerUser.user_id)
            'user_id': getattr(message.sender, 'id', None),
            'user_fn': getattr(message.sender, 'first_name', None),  # str, 用户 first_name
            'user_ln': getattr(message.sender, 'last_name', None),  # str, 用户 last_name
            'acquisition_time': datetime.utcnow(),  # datetime.datetime, 获取时间
            'media': to_dict(message.media),  # 媒体, 包含文件大小等各种属性
            'file_name': getattr(message.file, 'name', None),  # 文件名
            'file_ext': getattr(message.file, 'ext', None),  # 文件扩展名
        })
        for k, v in list(message_.items()):
            if v is None:  # 删除 null 节省空间
                del message_[k]
        if collection is None:  # 不保存数据库
            message_L.append(message_)
        else:
            result = collection.update_one({'id': message_['id'], 'dialog_id': message_['dialog_id']},
                                           {'$setOnInsert': message_}, upsert=True)
            matched_count += result.matched_count
            modified_count += result.modified_count
            upserted_count += 1 if result.upserted_id is not None else 0
    print({
        'matched_count': matched_count,
        'modified_count': modified_count,
        'upserted_count': upserted_count,
        'skip_ext_count': skip_ext_count,
        'skip_content': skip_content,
    })
    return upserted_count if upserted_count else message_L


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # 参数针对 每个群/频道 的首次下载, 循环之后不会再用这些参数. 一般不需要, 防止一些漏下
    parser.add_argument('-r', action='store_true', help='是否强制从头(编号1)开始下载消息')
    parser.add_argument("--new_old", default=None, type=int, help="是否在按顺序下载一次后再反向从最新的开始下载一次(并且编号会从头开始,而不是从数据库最新编号开始), 值为最多下载前多少个消息")
    args = parser.parse_args()
    
    excluded_file_ext = {  # 不需要保存的纯文件后缀
        '.webp', '.webm', '.tgs', '.sticker',  # 表情包
    }
    client.start()
    # 不断循环获取群和消息
    while True:
        print(datetime.now())
        while True:
            try:
                dialog_L = get_dialogs(client, collection=db_dialogs, exclude_name=exclude_name)  # 获取群
                break
            except BaseException as e:
                print('出错,10秒后再尝试:', e)
                time.sleep(10)
        dialog_L.sort(key=lambda d: d['title'])
        for i, dialog in enumerate(dialog_L):
            now = datetime.utcnow().replace(tzinfo=pytz.timezone('UTC'))
            try:
                if now - dialog['date'] > timedelta(hours=24*365*50):  # 太长时间没有消息的就跳过(哪怕是首次)
                    print('跳过:', dialog['id'], dialog['title'])
                    continue
            except:
                print('日期计算出错')
            # 获取消息
            kw = {
                'dialog_id': dialog['id'],
                'collection': db_messages,
                'tqdm_desc': '{} ID({}): {}'.format(i+1, dialog['id'], dialog['title']),
                'min_id': int(args.r),  # 设为0会自动从数据库中最新开始下载, 否则从min_id开始下载消息
                'excluded_file_ext': excluded_file_ext,
            }
            get_messages(client, **kw)
            if args.new_old:
                kw.update({
                    'min_id': 1,  # 强制从头
                    'limit': args.new_old,
                    'reverse': False,
                    'tqdm_desc': f'new_old {args.new_old}',
                })
                get_messages(client, **kw)
        print(datetime.now())
        time.sleep(3600)  # 隔多少秒再循环一次
        # 参数复原
        args.r = False
        args.new_old = None
