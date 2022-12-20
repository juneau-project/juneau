# load matplotlib
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# import bqplot
# import seaborn as sns

# matplotlib.style.use('fivethirtyeight')
# data set
x = ['BL-4', 'BL-3', 'BL-2', 'NPS', 'SJ']

# q2-t5#
plot_dict = {}
oy1 = np.array([3.3892, 3.26779, 2.9353, 3.2224, 1.2882])
oy2 = np.array([0.003, 0.002, 0.002, 0.0017, 0.0017])
oy3 = np.array([2.117327735, 2.196641154, 2.196641154, 2.214041886, 0.932263764])
plot_dict['q2_t5'] = [oy1, oy2, oy3]

# q2-t20#
oy1 = np.array([4.1085, 3.4916, 3.34, 3.4873, 2.0998])
oy3 = np.array([2.134342923, 2.134342923, 2.166589947, 2.328514327,1.092597014])
oy2 = np.array([0.018819353, 0.017425302, 0.005207488, 0.065457462, 0.053202713])
plot_dict['q2_t20'] = [oy1, oy2, oy3]

# q3-t5
oy1 = [5.578, 5.323, 4.257, 4.047, 2.610]
oy2 = [0.0048, 0.023, 0.007, 0.034, 0.033]
oy3 = [3.059, 3.16, 2.702, 2.744, 1.072]
plot_dict['q3_t5'] = [oy1, oy2, oy3]

# q3-t20
oy1 = [8.283, 6.765, 5.524, 4.102, 2.896]
oy2 = [2.772, 1.283, 0.416, 0.101, 0.089]
oy3 = [3.129, 3.25, 2.885, 2.782, 1.148]
plot_dict['q3_t20'] = [oy1, oy2, oy3]

# q4-t5
oy1= [11.910, 6.350, 5.631, 8.833, 5.744]
oy3 = [3.08, 2.921, 3.299, 3.171, 1.178]
oy2 = [7.289, 1.76, 0.174, 1.682, 1.632]
plot_dict['q4_t5'] = [oy1, oy2, oy3]


# q4-t20
oy1 = [263.61, 86.748, 19.903, 8.869, 6.218]
oy2 = [246.6, 77.66, 15.6, 1.922, 1.811]
oy3 = [3.263, 3.081, 3.208, 3.196, 1.32]
plot_dict['q4_t20'] = [oy1, oy2, oy3]


N = 5
ind = np.arange(N)
width = 0.25

for k in plot_dict:
    oy1, oy2, oy3 = plot_dict[k]

    bar1 = plt.bar(ind, oy1, width, color="#003f5c")

    bar2 = plt.bar(ind + width, oy3, width, color="#bc5090")

    bar3 = plt.bar(ind + width * 2, oy2, width, color="#ffa600")

    plt.xticks(ind + width, x, fontsize=14)
    plt.yticks(fontsize=14)
    # plt.legend( (bar1, bar2, bar3), ('Player1', 'Player2', 'Player3') )

    # plotdata = pd.DataFrame({"top-k part (%)":y2, "sketch part (%)":y3}, index = x)
    # plotdata.plot.py(kind = 'bar', stacked = True, color = ["#003f5c", "#bc5090","#ffa600"], grid = True, fontsize = 14, figsize = [7,7])
    plt.title(f'Perf. Comp. of Top-{k.split("t")[1]} Results of Query View (Size={k[1]})', fontsize=16)
    plt.legend((bar1, bar2, bar3), ['total time', 'sketch time', 'top-k'], fontsize=16)
    plt.xlabel("Methods", fontsize=16)
    plt.ylabel("Time (sec)", fontsize=16)
    plt.grid(axis='y', color='#DDDDDD', linestyle=':', linewidth=0.5)
    # plot.py stacked bar chart
    # plt.bar(x, y1, color='g')
    # plt.bar(x, y2, bottom=y1, color='y')
    # plt.ylim((0,265))
    plt.savefig(f'{k}.png', dpi=300)
