#!/usr/bin/env python
import os
import datatable as dt

# `BenchmarkTask` class will be loaded from the parent directory
BenchmarkTask = None
if os.path.isfile("helpers.py"):
    exec(open("helpers.py").read())
elif os.path.isfile("../helpers.py"):
    exec(open("../helpers.py").read())
else:
    raise SystemExit("Cannot find helpers.py; current directory is: %s"
                     % os.path.abspath("."))


# Declare which symbols are exported
__all__ = ("DatatableTask",)


class DatatableTask(BenchmarkTask):
    solution = "pydatatable"
    version = dt.__version__
    gitver = dt.__git_revision__
    cache = "TRUE"
