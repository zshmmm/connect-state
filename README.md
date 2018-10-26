# connect-state

用 Python 实现了一个 Redis 请求过程消耗时间的小工具。主要用于检测一个 Redis 请求过程中各个阶段的消耗时间。

工具在 Python 2.7 中测试通过，由于使用了新的字符串格式化写法不支持在 Python 2.6 中运行。 
脚本中未使用第三方模块，使用 Python socket 模块实现了一个单进程网络阻塞模式的 Redis 客户端来执行 Redis 操作。

脚本保存到文件后，在文件末尾修改 Redis 地址，认证密码即可直接运行。

运行效果如下：

![image](https://github.com/zshmmm/connect-state/blob/master/images/redisstat.png)
