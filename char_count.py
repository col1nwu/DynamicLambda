import MapReduce_framework
import Memory_monitor
import sys
import operator


def mapper(filename, counter):
    # extract words from file
    char_count_in_file = []

    with open(filename, "r") as fin:
        for line in fin:
            words = line.split()
            for word in words:
                char_count = len(word)
                char_count_in_file.append(char_count)

    fin.close()

    # combine results
    char_count_in_file.sort()
    char_counts_in_file = []

    counter1 = 0
    counter2 = 0
    for char_count in char_count_in_file:
        if counter1 == 0 and counter2 == 0:
            char_counts_in_file.append([char_count, 1])
        else:
            if char_count != char_counts_in_file[counter2][0]:
                char_counts_in_file.append([char_count, 1])
                counter2 += 1
            else:
                char_counts_in_file[counter2][1] += 1

        counter1 += 1

    fout = open("mapping_output_" + counter + ".txt", "w")
    for char_count in char_counts_in_file:
        fout.write(str(char_count[0]) + " " + str(char_count[1]) + "\n")

    fout.close()


def reducer(filename, counter_reduce):

    with open(filename, "r") as fin:
        char_counts = []

        for line in fin:
            char1, count1 = line.split(" ")
            count1_num = int(count1)

            if len(char_counts) == 0:
                char_counts.append([char1, count1_num])
                continue

            if char_counts[len(char_counts) - 1][0] == char1:
                char_counts[len(char_counts) - 1][1] += count1_num
            else:
                char_counts.append([char1, count1_num])

        fout = open("reducing_output_" + str(counter_reduce) + ".txt", "w")
        for char_count in char_counts:
            fout.write(char_count[0])
            fout.write(" ")
            fout.write(str(char_count[1]) + "\n")

        fin.close()
        fout.close()


def combiner(number):
    key_values = []

    for x in range(number):
        fin = open("reducing_output_" + str(x) + ".txt", "r")
        for line in fin:
            char, count = line.split(" ")
            char_count = [int(char), count]
            key_values.append(char_count)
        fin.close()

    key_values.sort(key=operator.itemgetter(0))

    fout = open("final_result.txt", "w")
    for key_value in key_values:
        fout.write(str(key_value[0]) + " " + key_value[1])
    fout.close()


if __name__ == "__main__":
    MapReduce_framework.mr_framework(mapper, reducer, combiner, sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
