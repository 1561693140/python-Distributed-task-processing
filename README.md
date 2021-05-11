# python-
python 分布式任务处理系统，提交任务交由各客户端处理

compute.py中定义了Work类，可以自定义getwork与dowork方法，这里getwork方法返回的是一堆数据，dowork方法去处理数据
![在这里插入图片描述](https://img-blog.csdnimg.cn/20210511205106124.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzM2NDEyMTk1,size_16,color_FFFFFF,t_70#pic_center)

Server.py顾名思义是用作服务器端，获取任务并将任务分发出去
![image](https://user-images.githubusercontent.com/40748509/117818587-54f5c500-b29b-11eb-8f80-ea5cc6887a52.png)

worker1.py用作客户端，从服务器获取任务并处理
![image](https://user-images.githubusercontent.com/40748509/117818466-37286000-b29b-11eb-8c04-610f9c2a6d06.png)

服务器端运行示意图
![image](https://user-images.githubusercontent.com/40748509/117818770-81114600-b29b-11eb-8146-2f01f2ba130c.png)

客户端运行示意图
![image](https://user-images.githubusercontent.com/40748509/117818839-90908f00-b29b-11eb-86f4-77e96e1037c5.png)

最终将计算结果写入文件
![image](https://user-images.githubusercontent.com/40748509/117818975-aef68a80-b29b-11eb-9f78-8ac92f69ad29.png)
