import json
import socket
import time
import sys
import threading

exepool = []	#execution pool
lock = threading.Lock()		#lock for synchronisation
in1 = sys.argv[1]	#inputs from command line
in2 = sys.argv[2]

'''=================listen to task launch messages from master================='''
def worker(port_no,work_id):
	while True:
		message = ""
		tlm = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		'''tlm -> task launch messages'''
		tlm.connect(('localhost',port_no))
		while True:
			rec = tlm.recv(1024)	#recieve message
			if rec:
				message += rec.decode()		#decode the recieved message
			else:
				break		#if no more messages
		if message:
			req_mes = json.loads(message)
			# print("lt",work_id," ",req_mes)
			begin = time.time()
			# lock.acquire()
			req_mes['begin'] = begin 
			time.sleep(0.1)
			lock.acquire()		#locked
			# print("locked")
			# if (work_id,req_mes) not in exepool:	
			exepool.append([work_id,req_mes])	#appending to the execution pool
			lock.release()		#unlocked
		else:
			break
		tlm.close()		#close the connection


'''=================Simulate Execution================='''
def simulate_execute():
	count = 10
	# global exepool
	while True:
		tr=[]
		if (len(exepool)==0):
			time.sleep(0.5)
		# lock.acquire()
		# print(exepool)
		
		for i in exepool:
			i[1]['duration'] = i[1]['duration'] - 1
			cur = time.time()
			time.sleep(1)
			if ((i[1]['duration'])==0):
				# i[1]['end'] = cur
				work_id = i[0]
				j_type = i[1]['job_type']
				t_id = i[1]['task_id']
				j_id = i[1]['job_id']
				
				print("in exe",j_type)

				lock.acquire()
				tr.append(i)
				# exepool.remove(i)
				# print("released")
				lock.release()

				start = i[1]['begin']
				end = cur
				msg = {'job_id': j_id, 'job_type': j_type, 'task_id': t_id, 'work_id': work_id, 'begin': start, 'end': end}
				sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
				sock.connect(('localhost',5001))	#connect to master
				send = json.dumps(msg)
				sock.send(send.encode())
				print("=================msg sent >>>",t_id)
				sock.close()
		# time.sleep(1)
		# if len(exepool)==0:
			# count-=1
			# break	
		lock.acquire()
		for i in tr:
			exepool.remove(i)
		lock.release()

se = threading.Thread(name = 'Thread 2',target = simulate_execute)

#listening and simulating execution threads
lt = threading.Thread(args = (int(in1),int(in2)),target = worker)

lt.start()
se.start()

lt.join()
se.join()