#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np
import sys

from query1 import *
from query2 import *
from query3 import *
from query4 import *
from query5 import *

def plot_results(x1, x2):

    labels = ["Q1","Q2","Q3","Q4","Q5"]
    x = np.arange(len(labels))
    width = 0.35 

    # Figure
    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, x1, width, label='csv')
    rects2 = ax.bar(x + width/2, x2, width, label='parquet')
    ax.set_ylabel('Execution time')
    ax.set_xlabel('Queries')
    ax.set_title('Execution Times per query & technology')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    fig.tight_layout()
    # Save figure
    plt.savefig("../results/plot.png",dpi=300)

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Please provide number of iterations for each query")
        exit()

    tests = {"csv":[], "parquet":[],"rdd":[]}

    for i in range(1,6):
        print("== Query {} ==".format(i))
        for f in ["csv", "parquet"]:
            t_sum = 0
            print("Executing Q{} for {} format...".format(i,f))
            for n in range(int(sys.argv[1])):
                t_sum += locals()["query{}".format(i)](f,showOutput=False)
            t = t_sum / (n + 1)
            tests[f].append(t)
        print()
         
    plot_results(tests["csv"],tests["parquet"])
