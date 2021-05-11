import threading
import time, sys, queue
from multiprocessing.managers import BaseManager
from writestock2sql import writeStockInfo
from multiprocessing import Queue, Process
from multiprocessing import cpu_count
import socket  # 导入 socket 模块
import time
from compute import Worker

# 创建类似的QueueManager:
class QueueManager(BaseManager):
    pass


def init(server_addr, port):
    # 由于这个QueueManager只从网络上获取Queue，所以注册时只提供名字:
    QueueManager.register('get_task_queue')
    QueueManager.register('get_result_queue')

    # 连接到服务器，也就是运行task_master.py的机器:
    print('Connect to server %s...' % server_addr)
    # 端口和验证码注意保持与task_master.py设置的完全一致:
    m = QueueManager(address=(server_addr, port), authkey=b'abc')
    # 从网络连接:
    m.connect()
    # 获取Queue的对象:
    task = m.get_task_queue()
    result = m.get_result_queue()
    # 从task队列取任务,并把结果写入result队列:
    work = writeStockInfo()
    work.load_info()
    return task, result, work


def process_task(queue, work, data):
    for item in data:
        try:
            work.doWork([item['task']])
            queue.put(str(item['task_id']) + ' yes')
        except Exception as e:
            queue.put(str(item['task_id'] + ' no'))
            continue


def get_work(server_addr, port, process_nums, my_ip,worker):
    while True:
        print('工作线程初始化')
        time.sleep(5)
        global isConnected
        if (not isConnected):
            print('未连接到服务器')
            continue
        # 由于这个QueueManager只从网络上获取Queue，所以注册时只提供名字:
        QueueManager.register('get_task_queue')
        QueueManager.register('get_result_queue')
        QueueManager.register('get_distribution_queue')
        try:
            # 端口和验证码注意保持与task_master.py设置的完全一致:
            m = QueueManager(address=(server_addr, port), authkey=b'abc')
            # 从网络连接:
            m.connect()
            # 获取Queue的对象:
            task_queue = m.get_task_queue()
            result = m.get_result_queue()
            distribution = m.get_distribution_queue()

        except:
            print('服务器连接失败')
            continue
        # 从task队列取任务,并把结果写入result队列:
        try:
            work = worker
        except:
            print('工作类初始化失败')
            return

        queue = Queue()
        if isConnected:
            flag = False
            print('开始获取任务')
            while True:
                try:
                    # print(task_queue.qsize())
                    data = task_queue.get(timeout=2)
                    tasks = [x['task_id'] for x in data]
                    break
                except Exception as e:
                    if isConnected:
                        print('获取任务失败，正在重试')
                        time.sleep(5)
                        continue
                    else:
                        print('与服务器断开')
                        flag = True
                        break

            print('报告获取到的任务')
            if flag:
                continue
            # 向server报告自己获取到的任务
            try:
                message = ''
                for index, task_id in enumerate(tasks):
                    if index != len(tasks)-1:
                        message += str(task_id) + ' '
                    else:
                        message += str(task_id)
                distribution.put(my_ip + ' ' + message)
            except Exception as e:
                print(e)
                continue

            # 分发任务
            print('分发任务到进程')
            n = process_nums
            for i in range(n):

                if i != n - 1:
                    work_process = Process(target=process_task,
                                           args=(
                                               queue, work, data[i * (len(data) // 4):(i + 1) * (len(data) // 4)],))
                else:
                    work_process = Process(target=process_task,
                                           args=(queue, work, data[i * (len(data) // 4):len(data)],))
                print('第', i + 1, '个进程启动')
                work_process.start()

            # 判断任务完成情况
            task_num = len(tasks)
            success_tasks = []
            fail_tasks = []
            while True:
                try:
                    response_message = ''
                    message = queue.get()
                    print(message)
                    l = message.split(' ')
                    task_num -= 1
                    if l[1] == 'yes':
                        success_tasks.append(l[0])
                        response_message = '' + l[0] + ' ' + 'yes' + ' ' + str(task_num) + ' ' + my_ip
                    else:
                        fail_tasks.append(l[0])
                        response_message = '' + l[0] + ' ' + 'no' + ' ' + str(task_num)
                    result.put(response_message)

                    if len(success_tasks) + len(fail_tasks) == len(tasks):
                        print('本轮任务已完成')
                        break
                except Exception as e:
                    if isConnected:
                        time.sleep(2)
                        continue
                    else:
                        print('返回结果失败，请检查网络连接')
                        flag = True
                        break
                if flag:
                    continue


        else:
            print('连接中断')
            continue


isConnected = False


# 维持心跳
def test_access(host, port):
    global isConnected
    while True:
        try:
            print('尝试连接客户端')
            s = socket.socket()
            s.connect((host, port))
            isConnected = True
        except Exception as e:
            isConnected = False
            time.sleep(5)
            continue
        while True:
            try:
                s.send(b'0')
                isConnected = True
                print('心跳包发送成功')
                time.sleep(5)
            except:
                # print('心跳包发送失败')
                isConnected = False
                time.sleep(5)
                break


if __name__ == '__main__':

    # 主机地址
    host = "127.0.0.1"
    # 心跳端口号
    heart_port = 12345
    # 任务端口号
    task_port = 5000
    # 客户端开启进程数量
    process_nums = 1
    # 当前主机cpu数
    cpu_nums = cpu_count()
    # 进程数量最大等于cpu数量
    if process_nums > cpu_nums:
        process_nums = cpu_nums
    my_ip = '127.0.0.1'

    # 开启心跳线程
    thread1 = threading.Thread(target=test_access, args=(host, 12345,))
    thread1.start()
    time.sleep(2)
    if (not isConnected):
        print('未连接到服务器')
    else:
        worker = Worker()
        # 获取任务并处理
        thread2 = threading.Thread(target=get_work, args=(host, task_port, process_nums, my_ip,worker))

        thread2.start()
        thread1.join()
        thread2.join()
        print('客户端异常结束')
