#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np
import sys

from query1 import *
from query2 import *
from query3 import *
from query4 import *
from query5 import *
from query1_rdd import *
from query2_rdd import *
from query3_rdd import *
from query4_rdd import *
from query5_rdd import *

def plot_results(data):
    x = np.arange(2)
    width = 0.35 

    # Figure
    fig, ax = plt.subplots()
    ax.bar(x, [data[0], 0], width, label='no-optimizer')
    ax.bar(x, [0,data[1]], width, label='with-optimizer')
    ax.set_ylabel('Execution time')
    ax.set_xlabel('Queries')
    ax.set_title('Execution Times')
    ax.set_xticks(x)
    ax.legend()
    fig.tight_layout()
    # Save figure
    plt.savefig("../results/plot_join_optimizer.png",dpi=300)


if __name__ == "__main__":

    with open("../results/test_joins.txt", "rt") as f:
        data = []
        for i in range(2):
            t = float(f.readline())
            data.append(t)             
    plot_results(data)
