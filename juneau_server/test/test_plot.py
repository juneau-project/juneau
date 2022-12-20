import matplotlib
import matplotlib.pyplot as plt
import numpy as np

# create data
x = [1, 2, 3, 4, 5]
y1 = [0.3125, 0.4375, 0.5, 0.5625, 0.6875]
y2 = [0.3125, 0.375, 0.4375, 0.4375, 0.4375]

matplotlib.rcParams.update({'font.size': 14})

# plot lines
plt.plot(x, y2, label="BL")
plt.plot(x, y1, label="SJ")

plt.xlabel('Top-k')
plt.ylabel('mean recall')
plt.legend()
# plt.show()
plt.savefig('mean-recall.png')
