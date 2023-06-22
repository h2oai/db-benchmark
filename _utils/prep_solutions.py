import sys
import os
import csv

SOLUTIONS_FILENAME = "_control/solutions.csv"
RUN_CONF_FILENAME = "run.conf"

SKIPPED_SOLUTIONS = ["clickhouse", "dask", "juliadf", "juliads"]


def print_usage():
    print("Usage: python3 _utils/prep_solutions.py --task=[groupby|join]")
    exit(1)

def parse_args():
    task = None
    for arg in sys.argv[1:]:
        if arg.startswith("--task="):
            task = arg.replace("--task=", "")
        else:
            print_usage()
    if task == None or (task != "groupby" and task != "join"):
        print_usage()
    return task

def main():
    task = parse_args()
    solution_name_list = get_solutions(task)
    update_run_conf_solutions(solution_name_list, task)

def update_run_conf_solutions(solution_name_list, task):
    # change what solutions are run in run.conf
    os.system(f"sed 's/export RUN_SOLUTIONS=.*/export RUN_SOLUTIONS=\"{solution_name_list}\"/g' run.conf > tmp_run.conf")
    os.system(f"sed 's/export RUN_TASKS=.*/export RUN_TASKS=\"{task}\"/g' tmp_run.conf > run.conf")

def get_solutions(task):
    solutions_for_task = ""
    with open(SOLUTIONS_FILENAME, newline="") as solutions_file:
        solutions = csv.DictReader(solutions_file, delimiter=',')
        for row in solutions:
            if row['task'] == task and row['solution'] not in SKIPPED_SOLUTIONS:
                solutions_for_task += row['solution'] + " "
    return solutions_for_task.strip()


if __name__ == "__main__":
    main()