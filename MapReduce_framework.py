import multiprocessing
import os


def divide_input(filename, n_mapper):
    fin = open(filename, "r")

    total_lines = 0
    with open(filename) as f:
        for _ in f:
            total_lines += 1

    line_per_file = int(total_lines / n_mapper)
    for i in range(n_mapper):
        fout = open("test" + str(i) + ".txt", "w")

        if i == n_mapper - 1:
            while True:
                temp = fin.readline()
                if temp != "":
                    fout.write(temp)
                else:
                    break
        else:
            for _ in range(line_per_file):
                temp = fin.readline()
                fout.write(temp)

        fout.close()
    fin.close()


def shuffle(n_mapper, n_reducer):
    key_and_reducer = []
    for x in range(n_reducer):
        key_and_reducer.append([])

    for x in range(n_mapper):
        fin = open("mapping_output_" + str(x) + "_combined.txt", "r")
        for line in fin:
            key_value = line.split()
            index = hash(key_value[0]) % n_reducer
            key_and_reducer[index].append(line)
        fin.close()

    for x in range(n_reducer):
        key_and_reducer[x].sort()

        fout = open("reducer_input_" + str(x) + ".txt", "w")
        for key_value in key_and_reducer[x]:
            fout.write(key_value)
        fout.close()


def adder(number):
    key_values = []

    for x in range(number):
        fin = open("reducing_output_" + str(x) + ".txt", "r")
        for line in fin:
            key_values.append(line)
        fin.close()

    key_values.sort()

    fout = open("final_result.txt", "w")
    for key_value in key_values:
        fout.write(key_value)
    fout.close()


def mr_framework(f_mapper, f_reducer, path, n_mapper, n_reducer, f_combiner):
    divide_input(path, n_mapper)

    # mapping stage
    processes_m = []
    for i in range(n_mapper):
        p = multiprocessing.Process(
            target=f_mapper, args=("test" + str(i) + ".txt", str(i)))
        p.start()
        os.system("echo " + str(p.pid) + " >> ./mem_reports/mapper_pids.txt")
        processes_m.append(p)

    for process in processes_m:
        process.join()

    # combining stage
    processes_c = []
    for i in range(n_mapper):
        p = multiprocessing.Process(
            target=f_combiner, args=("mapping_output_" + str(i) + ".txt", str(i)))
        p.start()
        os.system("echo " + str(p.pid) + " >> ./mem_reports/combiner_pids.txt")
        processes_c.append(p)

    for process in processes_c:
        process.join()

    # shuffle stage
    shuffle(n_mapper, n_reducer)

    # reducing stage
    processes_r = []
    for i in range(n_reducer):
        p = multiprocessing.Process(
            target=f_reducer, args=("reducer_input_" + str(i) + ".txt", str(i)))
        p.start()
        os.system("echo " + str(p.pid) + " >> ./mem_reports/reducer_pids.txt")
        processes_r.append(p)

    for process in processes_r:
        process.join()

    # combine results from reducers into one final result
    adder(n_reducer)

    return 0

