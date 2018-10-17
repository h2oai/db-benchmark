#!/usr/bin/env python
import os
import datatable as dt
from datatable import f, sum, mean, by
from utils import DatatableTask


class GroupbyTask(DatatableTask):
    task = "groupby"
    fun = "[.datatable"


class Question1(GroupbyTask):
    question = "sum v1 by id1"

    def compute(self, x):
        return x[:, {"v1": sum(f.v1)}, by(f.id1)]

    def verify(self, ans):
        return ans[:, sum(f.v1)]


class Question2(GroupbyTask):
    question = "sum v1 by id1:id2"

    def compute(self, x):
        return x[:, {"v1": sum(f.v1)}, by(f.id1, f.id2)]

    def verify(self, ans):
        return ans[:, sum(f.v1)]


class Question3(GroupbyTask):
    question = "sum v1 mean v3 by id3"

    def compute(self, x):
        return x[:, {"v1": sum(f.v1), "v3": mean(f.v3)}, by(f.id3)]

    def verify(self, ans):
        return ans[:, [sum(f.v1), sum(f.v3)]]


class Question4(GroupbyTask):
    question = "mean v1:v3 by id4"

    def compute(self, x):
        return x[:, {k: mean(f[k]) for k in ["v1", "v2", "v3"]}, by(f.id4)]

    def verify(self, ans):
        return ans[:, [sum(f.v1), sum(f.v2), sum(f.v3)]]


class Question5(GroupbyTask):
    question = "sum v1:v3 by id6"

    def compute(self, x):
        return x[:, {k: sum(f[k]) for k in ["v1", "v2", "v3"]}, by(f.id6)]

    def verify(self, ans):
        return ans[:, [sum(f.v1), sum(f.v2), sum(f.v3)]]


print("# groupby-pydatatable.py")
src_grp = os.environ['SRC_GRP_LOCAL']
filename = os.path.basename(src_grp)
if not os.path.isfile(filename):
    filename = src_grp
print("Loading dataset %s..." % filename)
x = dt.fread(filename)
print("  done. Shape: %r" % (x.shape,))
print()

Question1(x, filename).run()
Question2(x, filename).run()
Question3(x, filename).run()
Question4(x, filename).run()
Question5(x, filename).run()
