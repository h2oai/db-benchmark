import sys
import os
import csv

SOLUTIONS_FILENAME = "_control/solutions.csv"
RUN_CONF_FILENAME = "run.conf"


def print_usage():
    print("Usage: python3 _utils/prep_solutions.py --task=[groupby|join]")
    exit(1)


def main():
    install = sys.argv[1]
    task = sys.argv[2]
    if task == "":
        print_usage()

    solution_name_list = get_solutions(task)
    update_run_conf_solutions(solution_name_list, task)


def update_run_conf_solutions(solution_name_list, task):
    # change what solutions are run in run.conf
    os.execute(f"sed -i 's/export RUN_SOLUTIONS=.*/export RUN_SOLUTIONS={solutions_name_list}/g' run.conf ")
    os.execute(f"sed -i 's/export RUN_TASKS=.*/export RUN_TASKS={task}/g' run.conf")


def get_solutions(task):
    solutions_for_task = ""
    with open(SOLUTIONS_FILENAME, newline="") as solutions_file:
        solutions = csv.reader(solutions_file, delimiter=',')
        for sol in solutions:
            if row[1] == task:
                solutions_for_task += row[0] + " "
    return solutions_for_task.strip()


if __name__ == "__main__":
    main()