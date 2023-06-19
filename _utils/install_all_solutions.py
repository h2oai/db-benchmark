import sys
import os
import csv

SOLUTIONS_FILENAME = "_control/solutions.csv"


# based on the name of the solution, run the {{solution}}/min-setup-{{solution}}.sh file.
# if there is no min-setup-{{solution}}.sh, then run setup-{{solution}}.sh.
# if error, exit with an error
# else don't
def install_solutions():
    install_solutions = set()
    with open(SOLUTIONS_FILENAME, newline="") as solutions_file:
        solutions = csv.reader(solutions_file, delimiter=',')
        for sol in solutions:
            install_solutions.add(row[0])
    for solution in install_solutions:
        min_setup_file_name = f"./{solution}/min-setup-{solution}.sh"
        setup_file_name = f"./{solution}/setup-{solution}.sh"
        if os.fileexists(min_setup_file_name):
            os.execute(min_setup_file_name)
        elif os.fileexists(setup_file_name)
            os.execute(setup_file_name)
        else:
            raise Exception(f"No script to install {solution}")

install_solutions()
