## fessql Changelog

###[1.0.1b3] - 2020-3-18

#### Changed 
- 更改Pagination获取下一页时计数错误的问题
- 更改session的commit提交的时机


###[1.0.1b2] - 2020-3-1

#### Added 
- 增加Query类所有的查询操作均在Query类中完成，session类只负责具体的查询
- Query类中增加生成增删改查SQL字符串语句的功能,方便jrpc调用
- Query类中增加生成增删改查SQL对象的功能,方便普通调用
- 重构aio_mysql_client模块query查询向sqlalchemy的写法靠拢,而不再偏向mongodb,方便熟悉sqlalchemy的同时快速上手.
- 重构aio_mysql_client模块所有的CRUD功能全部使用query查询
- 增加Pagination类对于分页查询更简单，也更容易上手(sqlalchemy的写法)
- 升级aiomysql库到20版本,增加insert_many插入多条数据功能
- 增加生成分表model功能，使得分表的使用简单高效
- 增加多库多session的同时切换使用功能，提供对访问多个库的支持功能
- 优化应用停止时并发关闭所有的数据库连接
- session增加query_execute和execute做区分,并且query_execute返回值都为RowProxy相关
- session增加insert_from_select从查询直接insert的功能
- session分页查询find_many增加默认按照id升序排序的功能，可关闭
- 配置增加pool_recycle回旋关闭连接功能
- 配置增加fessql_binds用于多库的配置,并且增加配置校验功能
- 增加jrpc客户端单个方法请求的功能,调用形式和普通的函数调用形式一致
- 增加jrpc客户端批量方法请求的功能,调用形式类似链式调用
- 增加jrpc服务端jsonrpc子类, http和websocket的URL固定和client中的一致
- 对aiomysql类进行拆分为reader类和writer类,reader类会自动commit增加读取的效率
- 对session类也进行拆分为和reader writer对应的session reader和sessionwriter 

#### Changed 
- 优化所有代码中没有类型标注的地方,都改为typing中的类型标注
- SanicJsonRPC类中增加init_app方法保证和其他sanic扩展的初始化和调用方式一致
- jrpc client 和 jrpc server增加jrpc router的修改入口
- 更改jsonrpc三方包中queue没有使用同一个loop而造成的错误
- 再次重构session和query类彻底把query和session分开
- 拆分aclients库和eclients中的和数据库相关的功能形成新的库
- 使用的时候建议直接使用reader类或者writer类
- 修改生成model的功能适配字段映射,model类字段增减等功能,适用于同一个model适配不同的库表
