from info import db_dialogs, db_messages
from datetime import datetime, timedelta
from search import search_dialog


# 展示前多少个群/频道结果
limit_dialog = 20
# 展示前多少个用户结果
limit_user = 20
# 统计最近多久时间的信息
latest_time = timedelta(days=365)
# 检索群组/频道的正则, 只统计检索到的, 留空就是搜索全部
dialog_re = ""


# 群消息统计
dialogs_id_title = search_dialog(dialog_re, options='$i', show=False)
ret = list(db_messages.aggregate([
    {"$match": {
        **({"dialog_id": {"$in": list(dialogs_id_title)}} if dialog_re else {}),
        "date": {"$gte": datetime.utcnow() - latest_time},  # ISODate("2022-01-01T00:00:00Z")
    }},
    {"$group": {
        "_id": {
            "dialog_id": "$dialog_id",
            "user_id": "$user_id",
        },
        "count": {"$sum": 1},
    }},
    {"$group": {
        "_id": "$_id.dialog_id",
        "消息数": {"$sum": "$count"},
        "发言用户数": {"$sum": 1}
    }},
    {"$lookup": {"from": "dialogs", "localField": "_id", "foreignField": "id", "as": "dialog"}},
    {"$unwind": {"path": "$dialog"}},
    {"$project": {
        "dialog": "$dialog.title",
        "消息数": "$消息数",
        "发言用户数": "$发言用户数",
    }},
    {"$sort": {"发言用户数": -1}},
]))
print('总计群/频道数:', len(ret))

print('='*15, '排序: 发言用户数', '='*15)
print('排序\t发言用户数\t消息数\tdialog_id\t群/频道名称')
for i, m in enumerate(ret[:limit_dialog]):
    print(i+1, '\t', m['发言用户数'], '\t', m['消息数'], '\t', m['_id'], '\t', m['dialog'])
print()

print('='*15, '排序: 消息数', '='*15)
print('排序\t消息数\t发言用户数\tdialog_id\t群/频道名称')
for i, m in enumerate(sorted(ret, key=lambda t: t['消息数'], reverse=True)[:limit_dialog]):
    print(i+1, '\t', m['消息数'], '\t', m['发言用户数'], '\t', m['_id'], '\t', m['dialog'])
print()

# 用户发言统计, 不含纯频道
ret = list(db_messages.aggregate([
    {"$match": {
        "dialog_id": {"$in": list({i['id'] for i in db_dialogs.find({"is_group": True})} & set(dialogs_id_title))},
        "user_id": {"$ne": None},
        "date": {"$gte": datetime.utcnow() - latest_time},
    }},
    {"$sort": {"date": -1}},
    {"$group": {
        "_id": {
            "user_id": "$user_id",
            "dialog_id": "$dialog_id",
        },
        "count": {"$sum": 1},
        "username": {"$first": '$username'},
        "user_ln": {"$first": '$user_ln'},
        "user_fn": {"$first": '$user_fn'},
    }},
    {"$group": {
        "_id": "$_id.user_id",
        "发言数": {"$sum": "$count"},
        "发言群数": {"$sum": 1},
        "username": {"$first": '$username'},
        "user_ln": {"$first": '$user_ln'},
        "user_fn": {"$first": '$user_fn'},
    }},
]))
print('总计用户数:', len(ret))

print('='*15, '排序: 发言数', '='*15)
print('排序\t发言数\t发言群数\tuser_id\t姓 名\tusername')
for i, m in enumerate(sorted(ret, key=lambda t: t['发言数'], reverse=True)[:limit_user]):
    print(i+1, '\t', m['发言数'], '\t', m['发言群数'], '\t', m['_id'], '\t', m['user_ln'], m['user_fn'], '\t', m['username'])
print()

print('='*15, '排序: 发言群数', '='*15)
print('排序\t发言群数\t发言数\tuser_id\t姓 名\tusername')
for i, m in enumerate(sorted(ret, key=lambda t: t['发言群数'], reverse=True)[:limit_user]):
    print(i+1, '\t', m['发言群数'], '\t', m['发言数'], '\t', m['_id'], '\t', m['user_ln'], m['user_fn'], '\t', m['username'])
print()
