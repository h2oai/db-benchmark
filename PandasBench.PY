
wget -O- http://neuro.debian.net/lists/trusty.us-ca.libre | sudo tee /etc/apt/sources.list.d/neurodebian.sources.list
sudo apt-key adv --recv-keys --keyserver hkp://pgp.mit.edu:80 2649A5A9
sudo apt-get update
sudo apt-get install python3-pandas python3-dev
python3-config --cflags

python3
import pandas as pd
import numpy as np
import timeit
pd.__version__

def randChar(f, numGrp, N) :
   things = [f%x for x in range(numGrp)]
   return [things[x] for x in np.random.choice(numGrp, N)]
   
#def randChar(f, numGrp, N):
#   for i in xrange(N):
#      yield f%np.random.choice(numGrp,1)

def randFloat(numGrp, N) :
   things = [round(100*np.random.random(),4) for x in range(numGrp)]
   return [things[x] for x in np.random.choice(numGrp, N)]

N=int(1e7)
K=100
DF = pd.DataFrame({
  'id1' : randChar("id%03d", K, N),       # large groups (char)
  'id2' : randChar("id%03d", K, N),       # large groups (char)
  'id3' : randChar("id%010d", N//K, N),   # small groups (char)
  'id4' : np.random.choice(K, N),         # large groups (int)
  'id5' : np.random.choice(K, N),         # large groups (int)
  'id6' : np.random.choice(N//K, N),      # small groups (int)
  'v1' :  np.random.choice(5, N),         # int in range [1,5]
  'v2' :  np.random.choice(5, N),         # int in range [1,5]
  'v3' :  randFloat(100,N)                # numeric e.g. 23.5749
})
timeit.Timer("DF.groupby(['id1']).agg({'v1':'sum'})"                            ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id1']).agg({'v1':'sum'})"                            ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id1','id2']).agg({'v1':'sum'})"                      ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id1','id2']).agg({'v1':'sum'})"                      ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})"               ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})"               ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})" ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})" ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"    ,"from __main__ import DF").timeit(1)
timeit.Timer("DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"    ,"from __main__ import DF").timeit(1)



# comment I can't believe in Python there is no automatic coercion
# comment random.uniform just gives you one!
# how do you generate 1e6 random numbers?  If it's a for loop in Python, then that's in C in R.
# Why can't timeit just accept the command and run it?  Needs "from __main__ import DF" .Timer() and .timeit()

# df2.groupby(['X'], sort=True).sum()  # sort=TRUE corresponds to keyby=  by default it seems.

##  Comment: It returns different number of columns, depending on the function applied?!
##>>> df.groupby('A').sum()
##            C         D
##A                      
##bar -0.741773 -1.891930
##foo  1.342730  2.478295
##>>> df.groupby('A').count()
##     A  B  C  D
##A              
##bar  3  3  3  3
##foo  5  5  5  5
##>>> 


# 1E7
>>> timeit.Timer("DF.groupby(['id1']).agg({'v1':'sum'})"                            ,"from __main__ import DF").timeit(1)
0.5485171349992015
>>> timeit.Timer("DF.groupby(['id1']).agg({'v1':'sum'})"                            ,"from __main__ import DF").timeit(1)
0.5410999770010676
>>> timeit.Timer("DF.groupby(['id1','id2']).agg({'v1':'sum'})"                      ,"from __main__ import DF").timeit(1)
1.3937263370007713
>>> timeit.Timer("DF.groupby(['id1','id2']).agg({'v1':'sum'})"                      ,"from __main__ import DF").timeit(1)
1.392717992001053
>>> timeit.Timer("DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})"               ,"from __main__ import DF").timeit(1)
1.3823172709999199
>>> timeit.Timer("DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})"               ,"from __main__ import DF").timeit(1)
1.3713099260003219
>>> timeit.Timer("DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})" ,"from __main__ import DF").timeit(1)
0.29910559999916586
>>> timeit.Timer("DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})" ,"from __main__ import DF").timeit(1)
0.2982541080000374
>>> timeit.Timer("DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"    ,"from __main__ import DF").timeit(1)
0.8159747280005831
>>> timeit.Timer("DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"    ,"from __main__ import DF").timeit(1)
0.80791287700049


# 1E8
>>> timeit.Timer("DF.groupby(['id1']).agg({'v1':'sum'})"                            ,"from __main__ import DF").timeit(1)
5.604133386000285
>>> timeit.Timer("DF.groupby(['id1']).agg({'v1':'sum'})"                            ,"from __main__ import DF").timeit(1)
5.505057081000359
>>> timeit.Timer("DF.groupby(['id1','id2']).agg({'v1':'sum'})"                      ,"from __main__ import DF").timeit(1)
14.232032927000091
>>> timeit.Timer("DF.groupby(['id1','id2']).agg({'v1':'sum'})"                      ,"from __main__ import DF").timeit(1)
14.242601240999647
>>> timeit.Timer("DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})"               ,"from __main__ import DF").timeit(1)
22.87025260900009
>>> timeit.Timer("DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})"               ,"from __main__ import DF").timeit(1)
22.393589012999655
>>> timeit.Timer("DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})" ,"from __main__ import DF").timeit(1)
2.9725865330001398
>>> timeit.Timer("DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})" ,"from __main__ import DF").timeit(1)
2.9683854739996605
>>> timeit.Timer("DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"    ,"from __main__ import DF").timeit(1)
11.357687278999947
>>> timeit.Timer("DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"    ,"from __main__ import DF").timeit(1)
11.235407075000694


#1E9
>>> timeit.Timer("DF.groupby(['id1']).agg({'v1':'sum'})"                            ,"from __main__ import DF").timeit(1)
94.83129956100038
>>> timeit.Timer("DF.groupby(['id1']).agg({'v1':'sum'})"                            ,"from __main__ import DF").timeit(1)
92.57436498000061
>>> timeit.Timer("DF.groupby(['id1','id2']).agg({'v1':'sum'})"                      ,"from __main__ import DF").timeit(1)
214.5313569709997
>>> timeit.Timer("DF.groupby(['id1','id2']).agg({'v1':'sum'})"                      ,"from __main__ import DF").timeit(1)
215.42892127700088
>>> timeit.Timer("DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})"               ,"from __main__ import DF").timeit(1)
340.40721210799984
>>> timeit.Timer("DF.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})"               ,"from __main__ import DF").timeit(1)
339.9406450360002
>>> timeit.Timer("DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})" ,"from __main__ import DF").timeit(1)
30.89141360099984
>>> timeit.Timer("DF.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})" ,"from __main__ import DF").timeit(1)
30.161979088000408
>>> timeit.Timer("DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"    ,"from __main__ import DF").timeit(1)
237.9080805019994
>>> timeit.Timer("DF.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})"    ,"from __main__ import DF").timeit(1)
234.95511304699994

#1.5E9
>>> N = int(1.5e9)
>>> DF = pd.DataFrame({
...   'id1' : randChar("id%03d", K, N),       # large groups (char)
...   'id2' : randChar("id%03d", K, N),       # large groups (char)
...   'id3' : randChar("id%010d", N//K, N),   # small groups (char)
...   'id4' : np.random.choice(K, N),         # large groups (int)
...   'id5' : np.random.choice(K, N),         # large groups (int)
...   'id6' : np.random.choice(N//K, N),      # small groups (int)            
...   'v1' :  np.random.choice(5, N),         # int in range [1,5]
...   'v2' :  np.random.choice(int(1e6), N),  # int in range [1,1e6]
...   'v3' :  randFloat(100,N)                # numeric e.g. 23.5749
... })
Traceback (most recent call last):
  File "<stdin>", line 10, in <module>
  File "/usr/lib/python3/dist-packages/pandas/core/frame.py", line 203, in __init__
    mgr = self._init_dict(data, index, columns, dtype=dtype)
  File "/usr/lib/python3/dist-packages/pandas/core/frame.py", line 327, in _init_dict
    dtype=dtype)
  File "/usr/lib/python3/dist-packages/pandas/core/frame.py", line 4630, in _arrays_to_mgr
    return create_block_manager_from_arrays(arrays, arr_names, axes)
  File "/usr/lib/python3/dist-packages/pandas/core/internals.py", line 3235, in create_block_manager_from_arrays
    blocks = form_blocks(arrays, names, axes)
  File "/usr/lib/python3/dist-packages/pandas/core/internals.py", line 3322, in form_blocks
    object_items, np.object_)
  File "/usr/lib/python3/dist-packages/pandas/core/internals.py", line 3346, in _simple_blockify
    values, placement = _stack_arrays(tuples, dtype)
  File "/usr/lib/python3/dist-packages/pandas/core/internals.py", line 3410, in _stack_arrays
    stacked = np.empty(shape, dtype=dtype)
MemoryError
>>> 


#2E9
>>> N=int(2e9)
>>> K=100
>>> DF = pd.DataFrame({
...   'id1' : randChar("id%03d", K, N),       # large groups (char)
...   'id2' : randChar("id%03d", K, N),       # large groups (char)
...   'id3' : randChar("id%010d", N//K, N),   # small groups (char)
...   'id4' : np.random.choice(K, N),         # large groups (int)
...   'id5' : np.random.choice(K, N),         # large groups (int)
...   'id6' : np.random.choice(N//K, N),      # small groups (int)            
...   'v1' :  np.random.choice(5, N),         # int in range [1,5]
...   'v2' :  np.random.choice(int(1e6), N),  # int in range [1,1e6]
...   'v3' :  randFloat(100,N)                # numeric e.g. 23.5749
... })
Traceback (most recent call last):
  File "<stdin>", line 10, in <module>
  File "/usr/lib/python3/dist-packages/pandas/core/frame.py", line 203, in __init__
    mgr = self._init_dict(data, index, columns, dtype=dtype)
  File "/usr/lib/python3/dist-packages/pandas/core/frame.py", line 327, in _init_dict
    dtype=dtype)
  File "/usr/lib/python3/dist-packages/pandas/core/frame.py", line 4625, in _arrays_to_mgr
    arrays = _homogenize(arrays, index, dtype)
  File "/usr/lib/python3/dist-packages/pandas/core/frame.py", line 4934, in _homogenize
    raise_cast_failure=False)
  File "/usr/lib/python3/dist-packages/pandas/core/series.py", line 2468, in _sanitize_array
    subarr = _possibly_convert_platform(data)
  File "/usr/lib/python3/dist-packages/pandas/core/common.py", line 1773, in _possibly_convert_platform
    values = lib.maybe_convert_objects(values)
  File "inference.pyx", line 585, in pandas.lib.maybe_convert_objects (pandas/lib.c:45886)
MemoryError
>>> 







