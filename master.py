import json
import socket as s
import time
import random
import sys
import numpy as np
import threading as t
import datetime
from copy import copy




class master:
	def __init__(self):
		self.lock1=t.Lock()
		self.lock2=t.Lock()
		self.lock3=t.Lock()
		self.lock4=t.Lock()
		self.lock4.acquire()
		self.no_of_jobs=0
		self.lock4.release()
		self.task_sent=[]

	def accept_message(self):
		while(1):
			try:
				connection,address=s1.accept()
			# msg=connection.recv(1024)
			# decoded_msg=msg.decode()
			except:
			 	# print("1 error")
			 	break
				
			msg = connection.recv(1024)						
			msg1 = ""
			while msg:							
				msg1 += msg.decode()
				msg= connection.recv(1024)
			f2=json.loads(msg1)
			# print(f2)
			connection.close()
			# map_tasks1={}
			# reduce_tasks={}
			self.lock4.acquire()
			self.no_of_jobs=self.no_of_jobs+1
			self.lock4.release()
			job_id=int(f2["job_id"])
			map_tasks1[job_id]=[]
			reduce_tasks[job_id]=[]
			job_time[job_id]=time.time()
			for i in f2["map_tasks"]:
				
				# 	print(map_tasks)
				# 	map_tasks[job_id].append((f2['map_tasks']['task_id'],"m"))
				# except:
				# 	print(job_id)

				# print(i['task_id'])
				self.lock2.acquire()
				map_tasks1[job_id].append((i['task_id'],i["duration"],"m"))
				

				self.lock2.release()
			for i in f2["reduce_tasks"]:
				self.lock3.acquire()
													
				reduce_tasks[job_id].append((i["task_id"],i["duration"],"r",f2["reduce_tasks"]))
				self.lock3.release()
				# print(map_tasks1,reduce_tasks)
			# print("tasks",map_tasks1)
			self.select(job_id,f2["map_tasks"],map_tasks1)

	def random(self,job_id,task,map_tasks1):
		# print(task)
		for i in task:
			 
				# print(i)
				# print("random",i)
			worker_id=random.randint(1,3)
			while(1):
					# print(workers_list,worker_id)
					# print(workers_list[worker_id-1]['slots'])
				if(workers_list[worker_id-1]['slots']==0):
					worker_id=random.randint(1,3)
				else:
					break
			print("The task with task id",i["task_id"],"is alloted to the worker with ",worker_id)

				# print(worker_id,workers_list[worker_id-1]["slots"])
			self.assigntasks(job_id,worker_id,i,map_tasks1)

	def roundrobin(self,job_id,task,map_tasks1):
		
		workers_list1=copy(workers_list)
		workers_list1.sort(key=lambda x:x['worker_id'])
		for i in task:
			worker_id=0
			while(1):
				if(workers_list[worker_id]['slots']==0):
					worker_id=(worker_id+1)%3
				else:
					worker_id=worker_id+1
					break
			print("The task with task id",i["task_id"],"is alloted to the worker with ",worker_id)
			# print(worker_id,workers_list[worker_id-1]["slots"])
			self.assigntasks(job_id,worker_id,i,map_tasks1)
	def leastloaded(self,job_id,task,map_tasks1):
		k=0
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
			print("The task with task id",i["task_id"],"is alloted to the worker with ",worker_id)
			# print(worker_id,workers_list[worker_id-1]['slots'])
			self.assigntasks(job_id,worker_id,i,map_tasks1)
	def select(self,job_id,task,map_tasks1):
		if(sys.argv[2]=="random"):
			# print("mptask",task)
			# print(sys.argv[2])
			self.random(job_id,task,map_tasks1)
		elif(sys.argv[2]=="roundrobin"):
			self.roundrobin(job_id,task,map_tasks1)
		elif(sys.argv[2]=="leastloaded"):
			self.leastloaded(job_id,task,map_tasks1)

	def assigntasks(self,job_id,worker_id,i,map_tasks1):
		
		self.lock1.acquire()
		data["workers"][worker_id-1]["slots"]=data["workers"][worker_id-1]["slots"]-1
		self.lock1.release()

		# print("config is",data["workers"])

		if(worker_id==1):
			# print("w_id",worker_id)

			connection,address=worker1_socket.accept()
			# print("c1",connection,address)

		if(worker_id==2):
			# print("w_id",worker_id)
			connection,address=worker2_socket.accept()
			# print("c2",connection,address)
		
		if(worker_id==3):
			# print("w_id",worker_id)
			connection,address=worker3_socket.accept()
			# print("c3",connection,address)

			
		
		
		# selected_task["worker_id"]=worker_id
		# time1[i["task_id"]]=(worker_id,time.time())
		
		# print("start",i["task_id"],i["task_id"][1])s
		if i["task_id"] not in self.task_sent:
			# print(worker_id,data["workers"][worker_id-1]["slots"])
			selected_task['job_id']=job_id
		
			selected_task["task_id"]=i["task_id"]
		
			selected_task["duration"]=i["duration"]
			for j in map_tasks1[job_id]:
				if(j[0]==i["task_id"]):
					selected_task["job_type"]=j[2]
					# print(j[2])
					break
			x=datetime.datetime.now()
			if(len(time2[worker_id])==0):

				time2[worker_id]=[(x.strftime("%T"),1)]
			else:
				previous_count=time2[worker_id][-1][1]

				time2[worker_id].append((x.strftime("%T"),previous_count+1))
			message=json.dumps(selected_task)
		
			print("\nasmsg--",message,"\n")
			connection.send(message.encode())
			self.task_sent.append(i["task_id"])
			connection.close()

	def reducetasks(self):
		r_tasks=copy(reduce_tasks)
		map_tasks2=copy(map_tasks1)
		r_tasks1=[]
		while(True):
			for i,j in map_tasks2.items():
				# print(i,j)
				if(len(j)==0 and i not in r_tasks1):
					r_tasks1.append(i)
					# print("reduce_msg",reduce_tasks[i])
					r3={}
					r3[i]=r_tasks[i]
					# print("reduce", r3)
					# print("check",reduce_tasks[i][0][3])
					# print("ch",reduce_tasks[i][0][3])
					# print(r3)
					self.select(i,reduce_tasks[i][0][3],r3)
			
			r_tasks=copy(reduce_tasks)
			map_tasks2=copy(map_tasks1)
	def updatetasks(self):
		while(True):
			try:	
				connection,address=s2.accept()
				# print(address)
			except:
				# print("2 error")
				break
			received_msg=connection.recv(1024)
			received_msg=received_msg.decode()
			updateted_msg = ""
			while((received_msg)):
				updateted_msg += received_msg
				received_msg = connection.recv(1024).decode()
		
		
			received_msg=json.loads(updateted_msg)
			# print("rmsg",received_msg)
			worker_id=received_msg["work_id"]
			
			
			# print("slots",data["workers"][worker_id-1]["slots"])
			# print("previous",worker_id,data["workers"][worker_id-1]["slots"])
			self.lock1.acquire()
			data["workers"][worker_id-1]["slots"]=data["workers"][worker_id-1]["slots"]+1
			self.lock1.release()
			print(received_msg["task_id"]," freed from",data["workers"][worker_id-1]["worker_id"])
			print("config is",data["workers"])
			# print("after",worker_id,data["workers"][worker_id-1]["slots"])
			start_time=received_msg["begin"]
			endtime=received_msg["end"]
			task_id=received_msg["task_id"]
			time1[task_id]=(received_msg["work_id"],endtime-start_time)
			# time1[task_id]=list(time1[task_id])
			# time1[task_id][1]=endtime-start_time
			# time1[task_id]=tuple(time1[task_id])
			previous_count1=0
			previous_count1=time2[received_msg["work_id"]][-1][1]
			y=datetime.datetime.now()
			time2[received_msg["work_id"]].append((y.strftime("%T"),previous_count1-1))
			print("time2====================================]]]]]]]]]]]]]]]]]]]\n\n\n",time2)
			print("start_time",start_time,"endtime",endtime,"task",time1[task_id])
			t=received_msg["job_type"]
			# print(t)
			job_id=received_msg["job_id"]

			if(t=="m"):
					
					for i in map_tasks1[job_id]:
						if task_id in i:
							self.lock2.acquire()
							map_tasks1[job_id].remove(i)
							self.lock2.release()	
							# print("m",map_tasks1)
							break
					
			if(t=="r"):
				
				for i in reduce_tasks[job_id]:
					if task_id in i:
						self.lock2.acquire()
						reduce_tasks[job_id].remove(i)
						self.lock2.release()
						# print("r",reduce_tasks)
						break
				

			if(len(map_tasks1[job_id])==0 and len(reduce_tasks[job_id])==0):
				print("\n=====================Job Complete-",job_id,"=====================\n")
				cur = time.time()
				job_time[job_id] = cur-job_time[job_id]
				print("job id",job_id,"time taken",job_time[job_id])
				# print("mp",map_tasks1)
				# print("rt",reduce_tasks)
				# print(rt,reduce_tasks)
					# self.lock2.acquire()
					# map_tasks1[job_id].pop()
					# self.lock2.release()
					# self.lock3.acquire()
					# reducetasks[job_id].pop()
					# self.lock3.release()
				self.lock4.acquire()
				self.no_of_jobs=self.no_of_jobs-1
				self.lock4.release()
				if self.no_of_jobs==0:
					if (sys.argv[2]=='random'):
						f1 = open("random.csv","w")
						f2 = open("randomjob.csv","w")
						f3 = open("randomw_time.csv","w")
					elif ((sys.argv[2])=='roundrobin'):
						f1 = open("roundrobin.csv","w")
						f2 = open("roundrobinjob.csv","w")
						f3 = open("roundrobinw_time.csv","w")
					elif ((sys.argv[2])=="leastloaded"):
						f1=open("leastloaded.csv","w")
						f2=open("leastloadedjob.csv","w")
						f3=open("leastloadedw_time.csv","w")

					# f1.write("================================================="+str(sys.argv[2])+"=================================================")
					# f1.write("\njob times\n")
					l1=[]
					l2=[]
					for i,j in job_time.items():
						l1.append(j)
						f2.write(str(i)+","+str(j)+'\n')
					# f1.write('\n')
					# f1.write(time1.values()[1])
					for i,j in time1.items():
						l2.append(j[1])
						f1.write(str(i)+","+str(j[0])+","+str(j[1])+'\n')
					jmean = np.mean(l1)
					tmean = np.mean(l2)

					for i,j in time2.items():
						for k in j:
							f3.write(str(i)+","+str(k[0])+","+str(k[1])+"\n")
							#print("==========",k[0],k[1])


					jmed = np.median(l1)
					tmed = np.median(l2)
					print("\nSchedule type",sys.argv[2])
					print("job mean",jmean," ","job medain",jmed)
					print("task mean",tmean," ","task median",tmed)
					f1.close()
					f2.close()
					f3.close()
			
			connection.close()	


master1=master()

job_time={}
s1=s.socket(s.AF_INET,s.SOCK_STREAM)
#s1.settimeout(20.0)
s1.bind(("localhost",5000))
s1.listen(1)

s2=s.socket(s.AF_INET,s.SOCK_STREAM)
#s1.settimeout(30.0)
s2.bind(("localhost",5001))

s2.listen(3)

#create threads
data=json.load(open(sys.argv[1]))
workers_list=data["workers"]
map_tasks1={}
reduce_tasks={}
selected_task={}
time1={}
time2={i:[] for i in range(1,4)}

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
thread3=t.Thread(target=master1.reducetasks,name="3",daemon=True)

# master1.accept_message()


thread1.start()
thread2.start()
thread3.start()

thread1.join()
thread2.join()
thread3.killed = True

# s1.close()
# s2.close()
# worker1_socket.close()
# worker2_socket.close()
# worker3_socket.close()
s1.shutdown()
s2.shutdown()
worker1_socket.shutdown()
worker2_socket.shutdown()
worker3_socket.shutdown()
