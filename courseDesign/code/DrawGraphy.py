import matplotlib.pyplot as plt
import string

data_path = '..\\data\\data-30.txt'
res_path = '..\\data\\res-30.txt'

# 读入数据
data_file = open(data_path, 'r')
lines = data_file.readlines()
data_id = 0
datas = {}
for line in lines:
	datas[data_id] = []
	line = line.strip('\n')
	item = line.split(' ')
	today_open = float(item[0])
	for x in item:
		datas[data_id].append((float(x)-today_open) / today_open)
	data_id = data_id + 1


# 读入聚类结果
res_file = open(res_path, 'r')
res = res_file.readlines()
cluster = {}
for line in res:
	line = line.strip('\n')
	item = line.split(' ')
	print(item)
	key = item[1]
	value = int(item[0])
	if key not in cluster.keys():
		cluster[key] = []
	cluster[key].append(value)


cnt = 0
tot = 0
for key in cluster.keys():
	cnt = cnt+1
	if(len(cluster[key]) == 1):
		tot = tot+1
	print(cluster[key])
print(cnt)
print(tot)

# 画图
for key in cluster.keys():
	cur_cluster = cluster[key]
	for data_id in cur_cluster:
		data = datas[data_id]
		time = 0
		x = []
		y = []
		for item in data:
			x.append(time)
			y.append(item)
			time = time + 1
		plt.plot(x, y, label = data_id)
		#plt.plot(x2, y2)
	plt.title("cluster")
	plt.xlabel("time")
	plt.ylabel("pctchange")
	plt.legend()
	plt.show()

data_file.close()
res_file.close()