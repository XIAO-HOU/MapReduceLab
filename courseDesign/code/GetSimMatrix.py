import numpy as np

# cal_dist计算两个向量间的距离，采用DTW算法
def cal_dist(a, b):
    n = len(a)
    d = np.zeros((n, n), dtype=float)
    dp = np.zeros((n, n), dtype=float) + np.inf

    for i in range(n):
        for j in range(n):
            d[i][j] = np.sqrt(np.square(a[i]-b[j]))
    dp[0][0] = d[0][0]
    for i in range(n):
        for j in range(n):
            if i-1 >= 0:
                dp[i][j] = min(dp[i][j], dp[i-1][j]+d[i][j])
                if j-1 >= 0:
                    dp[i][j] = min(dp[i][j], dp[i-1][j-1]+2*d[i][j])
            if j-1 >= 0:
                dp[i][j] = min(dp[i][j], dp[i][j-1]+d[i][j])
    return dp[n-1][n-1]/(2*n)

data_path = "..\\data\\data-30.txt"
sim_path = "..\\data\\sim-30.txt"

data = np.loadtxt(data_path, delimiter=' ')
s = []
l = len(data)

# 预处理数据，将不同时刻的股价转化为相对初始时刻股价的变化率
for i in range(l):
    data[i] = (data[i] - data[i][0])/data[i][0]

# getMedian用来存储所有相似度，方便排序后得到中位数
getMedian = []

# 计算l组数据之间的相似度，存储在s矩阵内
for i in range(l):
    temp = []
    for j in range(l):
        dist = cal_dist(data[i], data[j])
        temp.append(-dist)
        if i != j:
            getMedian.append(-dist)
    s.append(temp)

# 取所有相似度的中位数
getMedian.sort()
Median = getMedian[int(len(getMedian)/2)]
# 有偶数组数据时，中位数取中间两个数据的平均值
if l % 2 == 0:
    Median = (getMedian[int(len(getMedian)/2)-1]+Median)/2
for i in range(l):
    s[i][i] = Median

# 格式化数据输出，每行数据形式为“行号i@0#s[i][0]#0 0#s[i][1]#0 0#s[i][2]#0...”
f = open(sim_path, 'w')
for i in range(len(s)):
    temp = str(i) + '@'
    for j in range(len(s[i])):
        if j:
            temp += ' '
        temp += '0#' + str(s[i][j]) + '#0'
    temp += '\n'
    f.write(temp)

data.close()
f.close()
