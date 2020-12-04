#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import datetime
import time


# In[2]:


#llj=pd.read_csv("/home/vishwas/Desktop/bd/leastloadedjob.csv")
llj=pd.read_csv("/home/vishwas/Desktop/bd/leastloadedjob.csv")
llj.columns=["Job_id","time"]
#print(llj)


# In[3]:


rrj=pd.read_csv("/home/vishwas/Desktop/bd/roundrobinjob.csv")
rrj.columns=["Job_id","time"]
##print(rrj)


# In[4]:


rj=pd.read_csv("/home/vishwas/Desktop/bd/randomjob.csv")
rj.columns=["Job_id","time"]
#print(rj)


# In[5]:


# llt=pd.read_csv("/home/vishwas/Desktop/bd/leastloaded.csv")
llt=pd.read_csv("/home/vishwas/Desktop/bd/leastloaded.csv")
llt.columns=["Task_id","worker","time"]
print(llt)


# In[6]:


rrt=pd.read_csv("/home/vishwas/Desktop/bd/roundrobin.csv")
rrt.columns=["Task_id","worker","time"]


# In[7]:


rt=pd.read_csv("/home/vishwas/Desktop/bd/random.csv")
rt.columns=["Task_id","worker","time"]


# In[8]:


llj_mean=llj["time"].mean()
rrj_mean=rrj["time"].mean()
rj_mean=rj["time"].mean()
rj_median=rj["time"].median()
rrj_median=rrj["time"].median()
llj_median=llj["time"].median()

llt_mean=llt["time"].mean()
rrt_mean=rrt["time"].mean()
rt_mean=rt["time"].mean()
rt_median=rt["time"].median()
rrt_median=rrt["time"].median()
llt_median=llt["time"].median()

rnd=[rj_mean,rj_median,rt_mean,rt_median]
ll=[llj_mean,llj_median,llt_mean,llt_median]
rr=[rrj_mean,rrj_median,rrt_mean,rrt_median]

# job_list_mean=[llj_mean,rrj_mean,rj_mean]
# job_list_median=[llj_median,rrj_median,rj_median]
# task_list_mean=[llt_mean,rrt_mean,rt_mean]
# task_list_median=[llt_median,rrt_median,rt_median]

cols=["job_mean","job_median","task_mean","task_median"]
print(llj_mean)


# In[9]:


plt.bar(cols,rnd)
#plt.plot()
plt.ylabel("time taken")
plt.show()


# In[10]:


plt.bar(cols,ll)
plt.ylabel("time taken")
plt.show()


# In[11]:


plt.bar(cols,rr)
plt.ylabel("time taken")
plt.show()


# In[16]:


pd.options.mode.chained_assignment = None
rrw=pd.read_csv("/home/vishwas/Desktop/bd/roundrobinw_time.csv")
rrw.columns=["work_id","worker_time","count"]
rw=pd.read_csv("/home/vishwas/Desktop/bd/randomw_time.csv")
rw.columns=["work_id","worker_time","count"]
llw=pd.read_csv("/home/vishwas/Desktop/bd/leastloadedw_time.csv")
llw.columns=["work_id","worker_time","count"]
import time
def get_sec(time_str):
    time_obj = datetime.datetime.strptime(time_str, '%H:%M:%S')
    time_obj1=time_obj.strftime("%M.%S")
    return float(time_obj1)

rw1=rw[rw["work_id"]==1]
rw2=rw[rw["work_id"]==2]
rw3=rw[rw["work_id"]==3]

llw1=llw[llw["work_id"]==1]
llw2=llw[llw["work_id"]==2]
llw3=llw[llw["work_id"]==3]

rrw1=rrw[rrw["work_id"]==1]
rrw2=rrw[rrw["work_id"]==2]
rrw3=rrw[rrw["work_id"]==3]
print(rw1)
rw1["worker_time"]=rw1["worker_time"].apply(lambda x:get_sec(x))
rw2["worker_time"]=rw2["worker_time"].apply(lambda x:get_sec(x))
rw3["worker_time"]=rw3["worker_time"].apply(lambda x:get_sec(x))

rrw1["worker_time"]=rrw1["worker_time"].apply(lambda x:get_sec(x))
rrw2["worker_time"]=rrw2["worker_time"].apply(lambda x:get_sec(x))
rrw3["worker_time"]=rrw3["worker_time"].apply(lambda x:get_sec(x))

llw1["worker_time"]=llw1["worker_time"].apply(lambda x:get_sec(x))
llw2["worker_time"]=llw2["worker_time"].apply(lambda x:get_sec(x))
llw3["worker_time"]=llw3["worker_time"].apply(lambda x:get_sec(x))



# for i in rw1["worker_time"]:
#     x = time.strptime(i.split(',')[0],'%H:%M:%S')
#     rw1["worker_time1"]=datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
#print(rw1)
fig, ax = plt.subplots(figsize=(10, 10))
print(type(rw1["worker_time"][0]))
# dates1 = matplotlib.dates.date2num(rw1["worker_time"])
# dates2 = matplotlib.dates.date2num(rw2["worker_time"])
# dates3 = matplotlib.dates.date2num(rw3["worker_time"])
# matplotlib.pyplot.plot_date(dates1, rw1["count"],color="red")
# matplotlib.pyplot.plot_date(dates2, rw2["count"],color="grenn")
# matplotlib.pyplot.plot_date(dates3, rw3["count"],color="blue")
ax.plot(rw1["worker_time"],rw1["count"],color="red")
ax.plot(rw2["worker_time"],rw2["count"],color="blue")
ax.plot(rw3["worker_time"],rw3["count"],color="green")
# ax.legend()


# In[13]:


fig, ax = plt.subplots(figsize=(10, 10))
ax.plot(llw1["worker_time"],llw1["count"],color="red")
ax.plot(llw2["worker_time"],llw2["count"],color="blue")
ax.plot(llw3["worker_time"],llw3["count"],color="green")
ax.legend()


# In[14]:


fig, ax = plt.subplots(figsize=(10, 10))
ax.plot(rrw1["worker_time"],rrw1["count"],color="red")
ax.plot(rrw2["worker_time"],rrw2["count"],color="blue")
ax.plot(rrw3["worker_time"],rrw3["count"],color="green")
ax.legend()


# In[15]:


ax=plt.subplot(111)
ax.bar(rrw1["worker_time"],rrw1["count"],color="red",width=0.5,align="center")
ax.bar(rrw2["worker_time"],rrw2["count"],color="blue",width=0.5,align="center")
ax.bar(rrw3["worker_time"],rrw3["count"],color="green",width=0.5,align="center")

