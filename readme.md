##将akshare获取的数据保存到本地数据库 
在main设置需要保存的表
程序根据要保存的表 将数据保存到指定mysql数据库

保存同花顺概念板块数据和概念日线数据
保存东财概念数据和概念成分股数据

注意事项：
1、没有写安装包 直接在 C:\Users\Administrator\akmysql-home 你的用户目录下建立目录即可，将config.json拷贝到该目录，设置好mysql地址账户
2、在mysql手工建立数据库 akmysql 字符集 utf8mb4 排序规则 utf8mb4_general_ci
3、执行main即可 ，批处理需要改成拟自己的路径 
4、批量执行容易被服务器封掉，尽量时间长些。
#感谢akshare团队提供的开源项目
python=3.9

