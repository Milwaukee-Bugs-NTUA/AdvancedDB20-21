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

def plot_results(x1, x2, x3):

    labels = ["Q1","Q2","Q3","Q4","Q5"]
    x = np.arange(len(labels))
    width = 0.20 

    # Figure
    fig, ax = plt.subplots()
    ax.bar(x - width, x1, width, label='csv')
    ax.bar(x, x2, width, label='parquet')
    ax.bar(x + width, x1, width, label='rdd')
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
    if int(sys.argv[1]) <= 0:
        print("Please provide a non negative number of iterations for each query")
        exit()
    tests = {"csv":[], "parquet":[],"rdd":[]}

    for i in range(1,6):
        print("== Query {} ==".format(i))
        for f in ["csv", "parquet"]:
            t_sum = 0
            print("Executing Q{} for {} format...".format(i,f))
            for n in range(int(sys.argv[1])):
                t_sum += locals()["query{}".format(i)](f)
            t = t_sum / (n + 1)
            tests[f].append(t)
        t_sum = 0
        print("Executing Q{} with rdd...".format(i))
        for n in range(int(sys.argv[1])):
            t_sum += locals()["query{}_rdd".format(i)]()
        t = t_sum / (n + 1)
        tests["rdd"].append(t)
        print()


    # Save output to disk
    with open("../results/execution_time.txt", "wt") as f:
        print("Q1\tQ2\tQ3\tQ4\tQ5\t",file=f)
        print(*tests["csv"],file=f)
        print(*tests["parquet"],file=f)
        print(*tests["rdd"],file=f)
             
    plot_results(tests["csv"],tests["parquet"],tests["rdd"])
