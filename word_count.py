import MapReduce_framework
import Memory_monitor
import sys
import operator
from memory_profiler import profile


def mapper(filename, counter):
    # extract words from file
    words_in_file = []

    with open(filename, "r") as fin:
        for line in fin:
            words = line.split()
            for word in words:
                word = word.lower()
                words_in_file.append(word)

    with open("mapping_output_" + counter + ".txt", "w") as fout:
        for word in words_in_file:
            fout.write(word + " 1\n")


def combiner(filename, counter):
    words_in_file = []
    with open(filename, "r") as fin:
        for line in fin:
            words = line.split()
            words_in_file.append(words[0])

    words_in_file.sort(key=operator.itemgetter(0))
    word_count_in_file = []

    counter1 = 0
    counter2 = 0
    for word in words_in_file:
        if counter1 == 0 and counter2 == 0:
            word_count_in_file.append([word, 1])
        else:
            if word != word_count_in_file[counter2][0]:
                word_count_in_file.append([word, 1])
                counter2 += 1
            else:
                word_count_in_file[counter2][1] += 1

        counter1 += 1

    with open("mapping_output_" + counter + "_combined.txt", "w") as fout:
        for word_count in word_count_in_file:
            fout.write(word_count[0] + " " + str(word_count[1]) + "\n")


def reducer(filename, counter_reduce):

    with open(filename, "r") as fin:
        word_counts = []

        for line in fin:
            word1, count1 = line.split(" ")
            count1_num = int(count1)

            if len(word_counts) == 0:
                word_counts.append([word1, count1_num])
                continue

            if word_counts[len(word_counts) - 1][0] == word1:
                word_counts[len(word_counts) - 1][1] += count1_num
            else:
                word_counts.append([word1, count1_num])

        fout = open("reducing_output_" + str(counter_reduce) + ".txt", "w")
        for word_count in word_counts:
            fout.write(word_count[0])
            fout.write(" ")
            fout.write(str(word_count[1]) + "\n")

        fin.close()
        fout.close()


if __name__ == "__main__":
    # MapReduce_framework.mr_framework(mapper, reducer, sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), combiner)
    Memory_monitor.memory_monitor_framework(mapper, reducer, sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), combiner)
