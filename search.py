from info import db_dialogs, db_messages
from pprint import pprint
import time


def search_dialog(dialog_re, options='$i', show=False):
    """通过正则方式检索mongo数据库对话

    Args:
        dialog_re (str): 检索群组/频道的正则
        options (str, optional): '$i' 表示忽略大小写的检索, '' 表示不忽略
        show (bool, optional): 是否输出到控制台展示结果

    Returns:
        dict: {'id':'title',..}
    """
    dialogs_id_title = {i['id']: i['title'] for i in db_dialogs.aggregate([
        {"$match": {
            "title": {
                "$regex": dialog_re,
                "$options": options,
            },
        }},
        {"$project": {
            "id": "$id",
            "title": "$title",
        }},
    ])}
    if show:
        print('='*20, '检索到的群组/频道({}):'.format(len(dialogs_id_title)), dialog_re)
        pprint(dialogs_id_title)
    return dialogs_id_title


def search(dialog_re, message_re, limit=20, options='$i', show=False):
    """通过正则方式检索mongo数据库对话和消息

    Args:
        dialog_re (str): 检索群组/频道的正则, 忽略大小写
        message_re (str): 检索消息的正则
        limit (int, optional): 最多返回的消息数量
        options (str, optional): '$i' 表示忽略大小写的检索, '' 表示不忽略, 针对 message_re
        show (bool, optional): 是否输出到控制台展示结果

    Returns:
        dict, list: {'id':'title',..}, [{..},..]
    """
    # 检索群组/频道
    if dialog_re:
        dialogs_id_title = search_dialog(dialog_re, options='$i', show=True)
    else:
        dialogs_id_title = search_dialog(dialog_re, options='$i', show=False)
    if message_re is None or len(message_re) == 0:
        return dialogs_id_title, []
    # 检索消息
    start = time.time()
    messages_ret = list(db_messages.aggregate([
        {"$match": {
            **({"dialog_id": {"$in": list(dialogs_id_title)}} if dialog_re else {}),
            "$or": [
                {"message": {  # 检索消息
                    "$regex": message_re,
                    "$options": options,
                }},
                {"file_name": {  # 检索文件
                    "$regex": message_re,
                    "$options": options,
                }},
                {"media.webpage.title": {  # 检索网页标题
                    "$regex": message_re,
                    "$options": options,
                }},
                {"media.webpage.description": {  # 检索网页内容
                    "$regex": message_re,
                    "$options": options,
                }},
            ]
        }},
        {"$sort": {"date": -1}},
        {"$limit": limit},
        {"$lookup": {"from": "dialogs", "localField": "dialog_id", "foreignField": "id", "as": "dialog"}},
        {"$unwind": {"path": "$dialog"}},
        {"$project": {
            "dialog": "$dialog.title",
            "message": "$message",
            "file_name": "$file_name",
            "web_title": "$media.webpage.title",
            "web_description": "$media.webpage.description",
            "date": "$date",
            "user": {"$concat": [{"$ifNull": ["$user_ln", ""]}, " ", {"$ifNull": ["$user_fn", ""]}]},
        }},
    ]))
    if show:
        print(round(time.time()-start, 1), '='*20, '检索到的消息({}):'.format(len(messages_ret)), message_re)
        pprint(messages_ret)
    return dialogs_id_title, messages_ret


if __name__ == '__main__':
    # 检索群组/频道的正则, 留空就是搜索全部
    dialog_re = ""
    # 检索消息的正则
    message_re = '^(?=.*国内)(?=.*nat)'
    # 最多返回的消息数量
    limit = 20
    search(dialog_re, message_re, limit=limit, show=True)
