import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def vypocet(i, j):
    return 3 * i + 2 * j


my_matice = np.zeros([199, 199], dtype=float)
for row in range(199):
    for column in range(199):
        if column == row:
            my_matice[row, column] = 0
        elif column >= row:
            my_matice[row, column] = vypocet(row, column)
            my_matice[column, row] = my_matice[row, column]

my_panda = pd.DataFrame(my_matice)
plt.pcolor(my_panda)
plt.show()
