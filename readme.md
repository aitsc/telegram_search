# 介绍
- 将自己加入telegram的所有群和频道(不包括机器人/用户对话等私密对话)的所有消息实时保存到数据库, 包括时间/内容/发表用户/转发回复/文件信息等等
- 可用于中文分词检索, 用户分析, 关键词触发提醒等等
- 保护隐私, 不记录下载所使用账号的相关信息
- 大约1万条消息1MB
- PS: 大家有遇到好的适合搜索资源群也可以分享到 Issues, 共同构建资源库

# 教程
1. 前往 https://my.telegram.org 获取 api_id 和 api_hash
2. 安装 [mongodb](https://www.mongodb.com/try/download/community), 例如:
   - wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2004-5.0.9.tgz
   - tar -xvf mongodb-* && cd mongodb-* && mkdir data
   - ./bin/mongod --dbpath ./data --bind_ip 0.0.0.0 --port 27017 --auth
   - ./bin/mongo
     - use admin
     - db.auth("user","password")
3. 将 config_example.json 改名为 config.json 并将其中的参数修改为自己的信息
4. 安装 python3, 执行 pip install -r requirements.txt 安装相关包
5. 执行 python -u data_to_mongo.py 实时获取群和频道消息到数据库. 终止后再运行会自动接着数据库中最新消息接着下载
   - 消息存储例子: <img src="message.png" width = "350" alt="" align=center />
6. 获取之后修改 search.py 中的检索正则, 然后执行 python search.py 测试检索
   - 例如: <img src="search.png" width = "350" alt="" align=center />
7. 用搜索到的 message 整句在客户端中检索找到上下文
8. 执行 python mongo_analysis.py 统计数据库中的群/频道和用户信息
   - 例如: <img src="stat.png" width = "350" alt="" align=center />

# 计划
- 扩充分词等字段, 用于快速检索
- 其他分析, 例如群每日活跃/用户活跃/话题趋势等等
- 更加强大的模糊检索web界面, 展示上下文

# 核心代码
1. info.py
2. data_to_mongo.py
3. search.py
4. mongo_analysis.py