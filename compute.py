import random
class Worker:
    def get_works(self):
        l = []
        for i in range(20):
            l.append(random.random()*10)
        return l
    def doWork(self,item):
        with open('log.txt','a') as f:
            num = item[0]
            message = str(num)+' * '+str(num)+' = '+str(num*num)
            f.write(message+'\n')

