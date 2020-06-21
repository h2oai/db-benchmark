#!/usr/bin/env python

# we put whole test code inside main guard because processes started by distributed client would start that file as well and raise errors
if __name__ == "__main__":
    exec(open("./dask/groupby-dask2.py").read())

