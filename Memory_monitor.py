from concurrent.futures import ThreadPoolExecutor
import MapReduce_framework
import time
import sys
import os
import glob


def delete_files():
    for file in glob.glob("*.txt"):
        os.remove(file)

    os.system("rm -rf ./mem_reports")
    os.system("mkdir ./mem_reports")


class MaxMemory:
    def __init__(self):
        self.keep_monitoring = True

    def memory_monitor(self, n_mapper, n_reducer):
        mapper_pids = []

        max_mem_mapper = 0
        max_mem_combiner = 0
        max_mem_reducer = 0

        while self.keep_monitoring:
            try:
                with open("./mem_reports/mapper_pids.txt", "r") as fin:
                    mapper_pid = fin.readline().strip()

                if os.path.isdir("/proc/" + mapper_pid):
                    os.system(
                        "echo $(cat /proc/" + mapper_pid + "/status | grep VmRSS -s | awk '{print $2}') > "
                                                           "./mem_reports/mem_mapper.txt")
                    os.system(
                        "echo $(cat /proc/" + mapper_pid + "/status | grep VmRSS -s | awk '{print $2}') $(date "
                                                           "+%T.%N) >> ./mem_reports/mem_mapper_dynamic.txt")

                    with open("./mem_reports/mem_mapper.txt", "r") as f_data:
                        temp_mem = int(f_data.readline().strip())
                        if temp_mem > max_mem_mapper:
                            max_mem_mapper = temp_mem
            except:
                pass

            try:
                with open("./mem_reports/combiner_pids.txt", "r") as fin:
                    combiner_pid = fin.readline().strip()

                if os.path.isdir("/proc/" + combiner_pid):
                    os.system(
                        "echo $(cat /proc/" + combiner_pid + "/status | grep VmRSS -s | awk '{print $2}') > "
                                                             "./mem_reports/mem_combiner.txt")
                    os.system(
                        "echo $(cat /proc/" + combiner_pid + "/status | grep VmRSS -s | awk '{print $2}') $(date "
                                                             "+%T.%N) >> ./mem_reports/mem_combiner_dynamic.txt")

                    with open("./mem_reports/mem_combiner.txt", "r") as f_data:
                        temp_mem = int(f_data.readline().strip())
                        if temp_mem > max_mem_combiner:
                            max_mem_combiner = temp_mem
            except:
                pass

            try:
                with open("./mem_reports/reducer_pids.txt", "r") as fin:
                    reducer_pid = fin.readline().strip()

                if os.path.isdir("/proc/" + reducer_pid):
                    os.system(
                        "echo $(cat /proc/" + reducer_pid + "/status | grep VmRSS -s | awk '{print $2}') > "
                                                            "./mem_reports/mem_reducer.txt")
                    os.system(
                        "echo $(cat /proc/" + reducer_pid + "/status | grep VmRSS -s | awk '{print $2}') $(date "
                                                            "+%T.%N) >> ./mem_reports/mem_reducer_dynamic.txt")

                    with open("./mem_reports/mem_reducer.txt", "r") as f_data:
                        temp_mem = int(f_data.readline().strip())
                        if temp_mem > max_mem_reducer:
                            max_mem_reducer = temp_mem
            except:
                pass

            time.sleep(0.0001)

        print("Mapper pid: " + mapper_pid + " Memory usage (in MB): " + str(max_mem_mapper/ 1024))
        print("Combiner pid: " + combiner_pid + " Memory usage (in MB): " + str(max_mem_combiner / 1024))
        print("Reducer pid: " + reducer_pid + " Memory usage (in MB): " + str(max_mem_reducer / 1024))


def memory_monitor_framework(f_mapper, f_reducer, filename, n_mapper, n_reducer, f_combiner):
    start_time = time.time()
    delete_files()

    with ThreadPoolExecutor(2) as executor:
        memory_monitor = MaxMemory()
        monitor_thread = executor.submit(memory_monitor.memory_monitor, n_mapper, n_reducer)
        try:
            framework_thread = executor.submit(MapReduce_framework.mr_framework, f_mapper, f_reducer, filename,
                                               n_mapper, n_reducer, f_combiner)
            framework_thread.result()

        finally:
            memory_monitor.keep_monitoring = False
            max_mem_b = monitor_thread.result()

    with open("/sys/fs/cgroup/memory/myGroup/memory.memsw.max_usage_in_bytes", "r") as f:
        max_mem_b = int(f.readline())

    max_mem_mb = max_mem_b / (1024 * 1024)

    end_time = time.time()
    print("Execution time (in sec): " + str(end_time - start_time))

    print("Max total memory usage (in MB): " + str(max_mem_mb))
