# ！/user/bin/python
# -*- coding:utf-8 -*-
# @Time: 2021/4 16:46
# @Author: Zhao
# @File: Server.py

import threading

from writestock2sql import writeStockInfo
import random, time, queue
from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support
import socket  # 导入 socket 模块
import time
import select
from compute import Worker

task_queue = queue.Queue()
result_queue = queue.Queue()
distribution_queue = queue.Queue()


class QueueManager(BaseManager):
    pass


def return_task_queue():
    global task_queue
    return task_queue  # 返回发送任务队列


def return_result_queue():
    global result_queue
    return result_queue  # 返回接收结果队列


def return_distribution_queue():
    global distribution_queue
    return distribution_queue


# 任务初始化
def init(host, port):
    QueueManager.register('get_task_queue', callable=return_task_queue)
    QueueManager.register('get_result_queue', callable=return_result_queue)
    QueueManager.register('get_distribution_queue', callable=return_distribution_queue)
    manager = QueueManager(address=(host, port), authkey=b'abc')
    manager.start()
    task = manager.get_task_queue()
    result = manager.get_result_queue()
    distribution_queue = manager.get_distribution_queue()
    return task, result, distribution_queue


# 任务日志类
class Tasks:
    def __init__(self):
        self.tasks = {}
        self.lock = threading.Lock()
        self.cache_tasks = []

    def get_max_task_id(self):
        self.lock.acquire()
        max_id = 0
        for key in self.tasks.keys():
            if key > max_id:
                max_id = key
        self.lock.release()
        return max_id

    def insert_task(self, task_items):
        self.lock.acquire()
        for item in task_items:
            self.tasks[item['task_id']] = {'task': item['task'], 'client_ip': '', 'isDeal': False, 'iSFinish': False,
                                           'deliver_time': int(time.time())}
        self.lock.release()

    def update_task_by_clients(self, task_id, client_ip):
        self.lock.acquire()
        self.tasks[int(task_id)]['client_ip'] = client_ip
        self.lock.release()

    def update_task_statue(self, task_id, isFinish):
        self.lock.acquire()
        self.tasks[task_id]['isDeal'] = True
        self.tasks[task_id]['isFinish'] = isFinish
        self.lock.release()

    def clear_undealed_task(self, client_ip):
        print(client_ip, '失去连接，清理其未完成的任务')
        self.lock.acquire()
        should_delete = []
        for task_id in self.tasks.keys():
            if self.tasks[task_id]['client_ip'] == client_ip:
                self.cache_tasks.append({'task_id': task_id, 'task': self.tasks[task_id]['task']})
                should_delete.append(task_id)
        for task_id in should_delete:
            self.tasks.pop(task_id)
        self.lock.release()

    def check_tasks(self):
        self.lock.acquire()
        flag = True
        should_delete = []
        for task_id in self.tasks.keys():
            if self.tasks[task_id]['client_ip'] == '':
                if not int(time.time()) - self.tasks[task_id]['deliver_time'] <= 20:
                    self.cache_tasks.append({'task_id': task_id, 'task': self.tasks[task_id]['task']})
                    should_delete.append(task_id)
                else:
                    flag = False
        for task_id in should_delete:
            self.tasks.pop(task_id)
        self.lock.release()
        return flag


# 客户端日志类
class Clients:
    def __init__(self):
        self.clients = {}
        self.spare_clients_num = 0
        self.lock = threading.Lock()

    def remove_clients(self, address):
        self.lock.acquire()
        self.clients.pop(address)
        self.lock.release()

    def add_client(self, address):
        self.lock.acquire()
        self.clients[address] = {'lastAccept': int(time.time()), 'statue': 1}
        self.lock.release()

    def update_last_accept(self, address, accept_time):
        self.lock.acquire()
        try:
            self.clients[address]['lastAccept'] = accept_time
            if self.clients[address]['statue'] == 0:
                self.clients[address]['statue'] = 1
            self.lock.release()
        except Exception as e:
            print(e)
            self.lock.release()

    def update_client_statue(self, address, statue):
        self.lock.acquire()
        self.clients[address]['statue'] = statue
        self.lock.release()

    def update_client_statue(self, address, statue):
        self.lock.acquire()
        self.clients[address]['statue'] = statue
        self.lock.release()


def get_and_distribute(clients, task_queue, tasks, instance):
    print('获取任务分配任务线程初始化')
    server = instance

    while True:
        cache_tasks = []
        tasks.lock.acquire()
        for item in tasks.cache_tasks:
            cache_tasks.append(item)
        tasks.lock.release()
        # 缓存不为空
        if not cache_tasks:
            taskes = []
            try:
                taskes = server.get_works()
                # print(len(taskes))
                print('本轮任务数有{}个'.format(len(taskes)))
                if not taskes:
                    continue
            except Exception as e:
                print('获取任务失败')
                continue

            works = []
            now_max_id = tasks.get_max_task_id()
            for index, t in enumerate(taskes):
                temp = {'task_id': index + 1 + now_max_id, 'task': t}
                works.append(temp)
        else:
            works = cache_tasks
            print('本轮任务数有{}个'.format(len(works)))

        tasks.insert_task(works)
        # 获取当前client数量,对任务进行分解
        while clients.spare_clients_num == 0:
            time.sleep(5)
            print('当前客户端数量为0，暂停任务划分')

        if clients.spare_clients_num != 0:
            workers_num = clients.spare_clients_num
            # workers_num = 2
            groups_num = len(works) // workers_num
            for i in range(workers_num):
                if i == workers_num - 1:
                    end = len(works)
                else:
                    end = (i + 1) * groups_num
                message = ''
                for j in range(i * groups_num, end):
                    if j != end - 1:
                        message += str(works[j]['task_id']) + ' '
                    else:
                        message += str(works[j]['task_id'])
                print('Put task id:{}'.format(message))
                task_queue.put(works[i * groups_num:end])
        print('检查任务分配情况')
        while True:
            if tasks.check_tasks():
                break
            time.sleep(5)


def get_distribution_response(tasks, distribution_queue, clients):
    print('获取分配结果线程初始化')
    while True:
        try:
            re = str(distribution_queue.get(timeout=5))
            re_list = re.split(' ')
            print(re_list)
            for i in range(1, len(re_list)):
                tasks.update_task_by_clients(int(re_list[i]), re_list[0])
            clients.update_client_statue(re_list[0], 2)
        except queue.Empty:
            continue


def get_result_response(tasks, result_queue, clients):
    print('获取任务执行结果线程初始化')
    success_tasks = []
    fail_tasks = []
    while True:
        try:
            r = str(result_queue.get(timeout=5))
            print('Result: %s' % r)
            l = r.split(' ')
            task_id = l[0]
            task_statue = l[1]
            task_last = l[2]
            client_address = l[3]

            if task_statue == 'yes':
                success_tasks.append(task_id)
                tasks.update_task_statue(int(task_id), True)
            else:
                fail_tasks.append(l[0], False)
                tasks.update_task_statue(int(task_id), False)
            if int(task_last) == 0:
                clients.update_client_statue(client_address, 1)

        except queue.Empty:
            continue


# def get_work_and_distribute(clients, host, port, task_queue, result_queue, distribution_queue):
#     task_cached = []
#     server = writeStockInfo()
#     while True:
#         print('获取任务')
#         if (task_cached == []):
#             taskes = []
#             try:
#                 taskes = server.get_works()
#                 print('本轮任务数有{}个'.format(len(taskes)))
#             except Exception as e:
#                 print('获取任务失败')
#
#             works = []
#             for index, task in enumerate(taskes):
#                 temp = {}
#                 temp['task_id'] = index + 1
#                 temp['task'] = task
#                 works.append(temp)
#         else:
#             works = task_cached
#             print('本轮任务数有{}个'.format(len(works)))
#
#         print('分配任务')
#         # 如果当前客户端数量不足，休息10s
#         while clients.spare_clients_num <= 0:
#             time.sleep(5)
#
#         # 获取当前client数量,对任务进行分解
#         workers_num = clients.spare_clients_num
#         # workers_num = 2
#         groups_num = len(works) // workers_num
#         for i in range(workers_num):
#             if i == workers_num - 1:
#                 end = len(works)
#             else:
#                 end = (i + 1) * groups_num
#             message = ''
#             for j in range(i * groups_num, end):
#                 if j != end - 1:
#                     message += str(works[j]['task_id']) + ' '
#                 else:
#                     message += str(works[j]['task_id'])
#             print('Put task id:{}'.format(message))
#             task_queue.put(works[i * groups_num:end])
#
#         print('判断各客户端获取到了哪些任务')
#         task_acceepted = {}
#         for item in works:
#             task_acceepted[item['task_id']] = ''
#             # 判断每个客户端分别获得了哪些任务
#         while True:
#             try:
#                 re = str(distribution_queue.get(timeout=5))
#                 re_list = re.split(' ')
#                 for i in range(1, len(re_list)):
#                     task_acceepted[re_list[i]] = re_list[0]
#
#             except queue.Empty:
#                 continue
#
#         print('等待任务执行结果')
#         success_tasks = []
#         fail_tasks = []
#         while True:
#             try:
#                 r = str(result_queue.get(timeout=5))
#                 print('Result: %s' % r)
#                 l = r.split(' ')
#                 if l[1] == 'yes':
#                     success_tasks.append(l[0])
#                 else:
#                     fail_tasks.append(l[0])
#
#             except queue.Empty:
#                 print('result queue is empty.')
#         time.sleep(15)


# 接受连接请求


# def get_accept(clients, host, port):
#     s = socket.socket()  # 创建 socket 对象
#     s.bind((host, port))  # 绑定端口
#     s.listen(20)  # 等待客户端连接
#     while True:
#         c, addr = s.accept()  # 建立客户端连接
#         # print('连接地址：', addr)
#         clients.clients_last_accept[addr[0]] = int(time.time())
#         c.close()


# 接受连接请求
def get_accept1(clients, host, port, tasks):
    server = socket.socket()  # 创建 socket 对象
    server.bind((host, port))  # 绑定端口
    server.listen(20)  # 等待客户端连接
    inputs = [server]
    outputs = []
    message_queues = {}
    while inputs:
        print('waiting for the next event')
        # 开始select 监听, 对input_list 中的服务器端server 进行监听
        # 一旦调用socket的send, recv函数，将会再次调用此模块
        readable, writable, exceptional = select.select(inputs, outputs, inputs)
        # Handle inputs
        # 循环判断是否有客户端连接进来, 当有客户端连接进来时select 将触发
        for s in readable:
            # 判断当前触发的是不是服务端对象, 当触发的对象是服务端对象时,说明有新客户端连接进来了
            # 表示有新用户来连接
            if s is server:
                # A "readable" socket is ready to accept a connection
                connection, client_address = s.accept()
                print('有新客户端加入:', client_address)
                clients.add_client(client_address[0])
                # this is connection not server
                connection.setblocking(0)
                # 将客户端对象也加入到监听的列表中, 当客户端发送消息时 select 将触发
                inputs.append(connection)
            else:
                address = s.getpeername()[0]
                # 有老用户发消息, 处理接受
                # 由于客户端连接进来时服务端接收客户端连接请求，将客户端加入到了监听列表中(input_list), 客户端发送消息将触发
                # 所以判断是否是客户端对象触发
                try:
                    data = s.recv(1024)
                    # 客户端未断开
                    clients.update_last_accept(address, int(time.time()))
                except Exception as e:
                    print(e)
                    inputs.remove(s)
                    clients.update_client_statue(address, False)
                    tasks.clear_undealed_task(address)
                    s.close()


# 检查客户端数量
def check_clients_status(client_log, tasks_log):
    while True:
        time.sleep(5)
        client_log.lock.acquire()
        n = 0
        for key in client_log.clients.keys():
            if client_log.clients[key]['statue'] != 0 and int(time.time()) - client_log.clients[key]['lastAccept'] > 6:
                client_log.clients[key]['statue'] = 0
                # 清理这个客户端没完成的任务
                tasks_log.clear_undealed_task(clients_log=client_log)
            elif client_log.clients[key]['statue'] == 1:
                n += 1
            else:
                continue
        print('当前空闲客户端数量', client_log.spare_clients_num)
        client_log.spare_clients_num = n
        client_log.lock.release()


if __name__ == '__main__':
    host = "127.0.0.1"
    heart_port = 12345
    task_port = 5000

    # 初始化任务队列，返回队列
    task, result, distribution_queue = init(host=host, port=task_port)

    # 初始化心跳线程辅助类
    clients = Clients()
    tasks = Tasks()
    # 线程t1负责接受客户端连接
    t1 = threading.Thread(target=get_accept1, args=(clients, host, heart_port, tasks))
    # 线程t2负责检查客户端数量
    t2 = threading.Thread(target=check_clients_status, args=(clients, tasks,))
    t1.start()
    t2.start()

    freeze_support()
    print('start!')
    # get_work_and_distribute(clients=clients, host=host, port=task_port, task_queue=task, result_queue=result,
    #                         distribution_queue=distribution_queue)
    instance = Worker()
    t3 = threading.Thread(target=get_and_distribute, args=(clients, task, tasks, instance))
    t4 = threading.Thread(target=get_distribution_response, args=(tasks, distribution_queue, clients,))
    t5 = threading.Thread(target=get_result_response, args=(tasks, result, clients))
    t3.start()
    t4.start()
    t5.start()

    t3.join()
    t4.join()
    t5.join()
    t1.join()
    t2.join()
