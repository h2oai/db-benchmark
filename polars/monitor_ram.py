import psutil
import time
import sys

solution = str(sys.argv[1])
data_name = str(sys.argv[2])
pid_of_parent = int(sys.argv[3])

max_loops = 720
file_name = f"{solution}-ram-{data_name}.txt"
i = 0
f = open(file_name, "w")
f.close()
while i < max_loops:
    # Get the current RAM usage and RSS
    process = psutil.Process(pid_of_parent)
    rss_usage = process.memory_info().rss >> 30
    ram_usage = psutil.virtual_memory().available >> 30

    # Print the results
    f = open(file_name, "a")
    f.write(f"RAM usage: {ram_usage} GB \n")
    f.write(f"RSS usage: {rss_usage} GB \n \n")
    f.close()
    
    # Wait for 10 seconds before polling again
    time.sleep(5)
    i += 1
