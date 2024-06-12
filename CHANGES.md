## fessql Changelog

###[1.1.0] - 2024-06-12


#### Added
- 优化重构fastapi部分session及query的整体逻辑,session改为即时用即时回收,最大限度的提高数据库连接的利用率,
防止数据库出现大量sleep连接,以及时不时出现的数据库连接关闭丢失的问题
- 新增FesMgrSession类用于管理FesSession类,类中包含有必要的方法,这样可以即时回收FesSession实例，而当前实例永不过期
- 优化session及query的使用逻辑，现在查询数据或者分页数据后(如first,all,paginate)会立即关闭session到连接池
- 优化重构flask部分逻辑和通用逻辑抛弃flask-sqlalchemy使用和fastapi一样的中间件查询规范，flask-sqlalchemy中大量功能用不到也影响性能 

#### Changed 
- 更改paginate分页的默认分页行为如果传limit=0则最大只返回1000条数据,防止数据过多卡死
- 移动execute和query_execute两个函数到从FastapiAlchemy到FesMgrSession中更合理


###[1.0.8] - 2021-9-6


#### Changed 
- 修复异步查询分页Pagination类中的默认排序逻辑,如果已有排序逻辑则去掉默认排序逻辑
- 增加异步查询分页Pagination类中的prev和next分页排序的默认排序参数


###[1.0.7] - 2021-7-30


#### Changed 
- 修复flask_alchemy出现的获取models的表时增加model造成的动态改变字典的错误
- 修复异步查询分页Pagination类中的prev和next返回数据和查询不一致导致的无法直接使用的问题

###[1.0.6] - 2021-3-30

#### Added 
- 重构flask_alchemy和fastapi_alchemy抽取FesQuery和FesPagination使之共用
- 去掉异步Query中对于max_per_page的处理，不再处理此参数
- 增加sqlalchemy版本限制

#### Changed 
- 修复falstapi alchemy中分页因为参数不对应造成的数据错误问题
- 去掉falstapi alchemy中paginate方法的max_per_page参数
- 修复flask alchemy中分页因为参数不对应造成的数据错误的问题


###[1.0.4] - 2021-1-14

#### Added 
- 经过项目测试可以发布正式版本

#### Changed 
- 修复fastapi alchemy中如果默认的连接失败ping操作也会失败的问题
- 更改flask alchemy中如果ping操作默认key的类型值
- 修复flask alchemy应用结束自动关闭session会出错的问题


###[1.0.3b1~1.0.3b3] - 2020-12-8

#### Added 
- session中新增filter,filter_by等常用可提示功能.
- session中常用的方法都已经增加,方便智能提示.
- sql中增加load_only便捷导入方法
- 拆分fastapi_alchemy中的excecute方法为query_execute和execute

#### Changed 
- 更改binary_prefix的设置方式全部放到connect_args参数中

###[1.0.2b1] - 2020-11-11

#### Added 
- 增加适配fastapi框架使用sqlalchemy的功能.
- 增加fastapi中对提交session上下文的处理,更便捷.
- 增加直接执行sql的execute功能
- 增加多数据库通过bind绑定和访问的功能
- 增加对探测session连通的ping功能
- 增加session上下文生成和关闭的功能

#### Changed 
- 更改异步和同步的类名直接以框架名称命名标识.


###[1.0.1b5~1.0.1b6] - 2020-9-22

#### Changed 
- 修改异步MySQL中的引用功能,增加其他类的暴露.
- 修复应用层如果不转换page和per_page为整型导致分页会报错的问题

###[1.0.1b4] - 2020-9-3

#### Added
- 增加能够选择数据库驱动的功能，默认为pymysql.
- 增加上下文创建session的功能
- 增加创建新的session后还原默认的session的功能，方便在同一个请求上下文使用
- 增加dbalchemy中session的ping功能,探测session是否还连通
- 新增生成session后探测是否还连通,如果不连通则清理,保证生成的session是可用的
- tinymysql中增加上下文管理器功能，优化参数，优化获取连接方式.
- 增加tinymysql中的类型注释,符合mypy要求
- 增加aiomysql中的类型注释,符合mypy要求
- 增加其他关键字参数的传入,不再写固定的参数
- 优化aiomysql的实现方式使用层面不再区分reader和writer,而改为单一的session
- 在aiomysql的session中还是会区分reader和writer,对于读写采用即时更改是否自动提交的方式解决读数据还需要commit才是最新数据的问题

#### Changed 
- 去掉启动时自动设置SQLALCHEMY_BINDS的功能,如果没有设置则抛出异常.
- 去掉创建session时自动设置SQLALCHEMY_BINDS的功能,如果没有设置则抛出异常.
- 更改Pagination获取下一页时计数错误的问题
- 更改session的commit提交的时机
- 优化ping session是session或者scope session的写法
- 解决如果session过期ping后session会变为默认的session的问题
- 修复如果连接断开后使用了ping下次再使用会报错的问题
- 更改aiomysql中commit的方式去掉显式提交的方式改为上下文自动提交的方式
- 调整结构安装的时候可以选择安装异步或者同步SQL操作

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
- 对aiomysql类进行拆分为reader类和writer类,reader类会自动commit增加读取的效率
- 对session类也进行拆分为和reader writer对应的session reader和sessionwriter 

#### Changed 
- 优化所有代码中没有类型标注的地方,都改为typing中的类型标注
- 再次重构session和query类彻底把query和session分开
- 拆分aclients库和eclients中的和数据库相关的功能形成新的库
- 使用的时候建议直接使用reader类或者writer类
- 修改生成model的功能适配字段映射,model类字段增减等功能,适用于同一个model适配不同的库表
