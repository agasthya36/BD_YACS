import json
import socket as s
import time
import random
import sys
import numpy
import threading as t
from copy import copy,deepcopy

#creating sockets



class master:
	def __init__(self):
		self.lock1=t.Lock()
		self.lock2=t.Lock()
		self.lock3=t.Lock()
		self.lock4=t.Lock()
		self.lock4.acquire()
		self.no_of_jobs=0
		self.lock4.release()

	def accept_message(self):
		while(True):
			connection,address=s1.accept()
			msg=connection.recv(1024)
			decoded_msg=msg.decode()
			f2=json.loads(decoded_msg)
			print(f2)
			connection.close()
			# map_tasks1={}
			# reduce_tasks={}
			self.lock4.acquire()
			self.no_of_jobs=self.no_of_jobs+1
			self.lock4.release()
			job_id=int(f2["job_id"])
			map_tasks1[job_id]=[]
			reduce_tasks[job_id]=[]
			for i in f2["map_tasks"]:
				self.lock2.acquire()
				# 	print(map_tasks)
				# 	map_tasks[job_id].append((f2['map_tasks']['task_id'],"m"))
				# except:
				# 	print(job_id)

				# print(i['task_id'])
		
				map_tasks1[job_id].append((i['task_id'],i["duration"],"m"))
				

				self.lock2.release()
			for i in f2["reduce_tasks"]:
				self.lock3.acquire()
				job_id=int(f2["job_id"])										
				reduce_tasks[job_id].append((i["task_id"],i["duration"],"r"))
				self.lock3.release()
				# print(map_tasks1,reduce_tasks)
				self.select(job_id,map_tasks1[job_id])

	def random(self,job_id,task):
		# print(task)
		for i in task:
			# print("random",i)
			worker_id=random.randint(1,3)
			while(1):
				# print(workers_list,worker_id)
				# print(workers_list[worker_id-1]['slots'])
				if(workers_list[worker_id-1]['slots']==0):
					worker_id=random.randint(0,3)
				else:
					break	
			# print(worker_id,workers_list[worker_id-1]["slots"])
			self.assigntasks(job_id,worker_id,i)

	def roundrobin(self,job_id,task):
		
		workers_list1=copy(workers_list)
		workers_list1.sort(key=lambda x:x['worker_id'])
		for i in task:
			worker_id=0
			while(1):
				if(workers_list[worker_id]['slots']==0):
					worker_id=(worker_id+1)%3
					workers_list1=copy(workers_list)
					worker_list1.sort(key=lambda x:x['worker_id'])
				else:
					worker_id=worker_id+1
					break
			# print(worker_id,workers_list[worker_id-1]["slots"])
			self.assigntasks(job_id,worker_id,i)
	def leastloaded(self,job_id,task):
		for i in task:
			max=workers_list[0]["slots"]
			while(1):
				for j in range(0,3):
					if(workers_list[j]["slots"])>max:
						max=workers_list[j]["slots"]
						k=j
				if(max==0):
					time.sleep(1)
				else:
					worker_id=k+1
					break
			# print(worker_id,workers_list[worker_id-1]['slots'])
			self.assigntasks(job_id,worker_id,i)
	def select(self,job_id,task):
		if(sys.argv[2]=="random"):
			#print("mptask",task)
			# print(sys.argv[2])
			self.random(job_id,task)
		elif(sys.argv[2]=="roundrobin"):
			self.roundrobin(job_id,task)
		elif(sys.argv[2]=="leastloaded"):
			self.leastloaded(job_id,task)

	def assigntasks(self,job_id,worker_id,i):

		if(worker_id==1):
			connection,address=worker1_socket.accept()
		if(worker_id==2):
			connection,address=worker2_socket.accept()
		if(worker_id==3):
			connection,address=worker3_socket.accept()
		self.lock1.acquire()
		print("i am in data[workers]")
		data["workers"][worker_id-1]["slots"]=data["workers"][worker_id-1]["slots"]-1
		self.lock1.release()
		selected_task['job_id']=job_id
		
		selected_task["task_id"]=i[0]
		
		selected_task["duration"]=i[1]
		selected_task["job_type"]=i[2]
		time1[i[0]]=(worker_id,time.time())
		message=json.dumps(selected_task)
		print("asmsg--",message)
		connection.send(message.encode())
		connection.close()

	def reducetasks(self):
		r_tasks=deepcopy(reduce_tasks)
		r_tasks1=[]
		while(True):
			for i,j in map_tasks1.items():
				# print(i,j)
				if(len(j)>0):
					if(i not in r_tasks1):
						r_tasks1.append(i)
						print("reduce_msg",reduce_tasks[i])
						self.select(i,reduce_tasks[i])
			time.sleep(1)
			r_tasks=deepcopy(reduce_tasks)
	def updatetasks(self):
		while(True):
			try:	
				connection,address=s2.accept()
			except:
				break
			received_msg=connection.recv(1024)
			received_msg=received_msg.decode()
			received_msg=json.loads(received_msg)
			print("rmsg",received_msg)
			
			self.lock1.acquire()
			worker_id=received_msg["work_id"]
			# print("slots",data["workers"][worker_id-1]["slots"])
			data["workers"][worker_id-1]["slots"]=data["workers"][worker_id-1]["slots"]+1
			self.lock1.release()
			start_time=received_msg["begin"]
			endtime=received_msg["end"]
			task_id=received_msg["task_id"]
			a=list(time1[task_id])
			a[1]=endtime-start_time
			time1[task_id]=tuple(a)
			# time1[task_id]=list(time1[task_id])
			# time1[task_id][1]=endtime-start_time
			# time1[task_id]=tuple(time1[task_id])
			t=received_msg["job_type"]
			job_id=received_msg["job_id"]

			if(t=="m"):
				self.lock2.acquire()
				for i in map_tasks1[job_id]:
					if task_id in i:
						map_tasks1[job_id].remove(i)
						print("m",map_tasks1)
				self.lock2.release()
			else:
				self.lock2.acquire()
				for i in reduce_tasks[job_id]:
					if task_id in i:
						reduce_tasks[job_id].remove(i)
				self.lock2.release()

			if(len(map_tasks1[job_id])==0 and len(reduce_tasks[job_id])==0):
				print("----------Job Complete----------")
				try:
					self.lock2.acquire()
					map_tasks1[job_id].pop()
					self.lock2.release()
					self.lock3.acquire()
					reducetasks["job_id"].pop()
					self.lock3.release()
					self.lock4.acquire()
					self.no_of_jobs=self.no_of_jobs-1
					self.lock4.release()
				except:
					pass
			connection.close()	


master1=master()


s1=s.socket(s.AF_INET,s.SOCK_STREAM)
s1.bind(("localhost",5000))
s2=s.socket(s.AF_INET,s.SOCK_STREAM)
s2.bind(("localhost",5001))
s1.listen(1)
s2.listen(3)

#create threads
a=open(sys.argv[1],"r")

data=json.load(a)
a.close()
workers_list=data["workers"]
map_tasks1={}
reduce_tasks={}
selected_task={}
time1={}
port1=workers_list[0]['port']
port2=workers_list[1]['port']
port3=workers_list[2]['port']
worker1_socket=s.socket(s.AF_INET,s.SOCK_STREAM)
worker1_socket.bind(("localhost",port1))
worker1_socket.listen(1)
worker2_socket=s.socket(s.AF_INET,s.SOCK_STREAM)
worker2_socket.bind(("localhost",port2))
worker2_socket.listen(1)
worker3_socket=s.socket(s.AF_INET,s.SOCK_STREAM)
worker3_socket.bind(("localhost",port3))
worker3_socket.listen(1)
thread1=t.Thread(target=master1.accept_message,name="1")
thread2=t.Thread(target=master1.updatetasks,name="2")

thread3=t.Thread(target=master1.reducetasks,name="3")

# master1.accept_message()


thread1.start()
thread2.start()
thread3.start()

thread1.join()
thread2.join()
thread3.killed=True
s1.close()
s2.close()
worker1_socket.close()
worker2_socket.close()



