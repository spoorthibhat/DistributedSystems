import csv
import hashlib


def print_data(filename):
    with open(filename) as data_file:
        read_file = csv.reader(data_file, delimiter=',')
        next(read_file)
        keys = {}
        for row in read_file:
            each_key = row[0] + row[3]
            key_identifier = int(hashlib.sha1(each_key.encode()).hexdigest(), 16)
            keys[key_identifier] = row

        for key, value in keys.items():
            if key == 1157937609317320406266672639946413401805313309607:
                print('{} -> {}'.format(key, value))


print_data('Career_Stats_Passing.csv')