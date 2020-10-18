# HBaseAPI
java实现HBaseAPI，建表时循环新建列族、循环插入数据、删表、查表
代码在HBasePro文件夹中，依赖及插件配置在pom.xml


1.创建表
![创建表](https://github.com/JackFong/HBaseAPI/blob/main/hbaseAPI/%E5%88%9B%E5%BB%BA%E8%A1%A8.png)

2.插入数据
![插入数据](https://github.com/JackFong/HBaseAPI/blob/main/hbaseAPI/%E6%8F%92%E5%85%A5%E6%95%B0%E6%8D%AE.png)

3.查询一行数据
![查询数据（行）](https://github.com/JackFong/HBaseAPI/blob/main/hbaseAPI/%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE.png)

4.查询全表数据，可添加使用过滤器检索该表，如32行后可添加 scanner.setFilter(filter);
![查询数据（全表）](https://github.com/JackFong/HBaseAPI/blob/main/hbaseAPI/%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE2.png)

5.删除一张表
![删除表](https://github.com/JackFong/HBaseAPI/blob/main/hbaseAPI/%E5%88%A0%E9%99%A4%E8%A1%A8.png)
