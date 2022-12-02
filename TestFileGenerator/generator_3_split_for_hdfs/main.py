'''
---------------------------------------------------------------------------
Description:
Split test file data of generator_2
For HDFS use
---------------------------------------------------------------------------
Author: Warren Liu
Date: 12/02/2022
---------------------------------------------------------------------------
'''

import os

INPUT_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'input',
    'data.csv'
)

OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'output'
)

HDFS_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'hdfs'
)


def split_test_node_and_train_node(input_file: str, output_path: str):
    fin =  open(input_file, 'r', 1, 'utf8')
    fout_test = open(os.path.join(output_path, 'test_node.csv'), 'w+', 1, 'utf8')
    fout_train = open(os.path.join(output_path, 'train_node.csv'), 'w+', 1, 'utf8')


    cnt = 0
    for line in fin.readlines()[1:]:
        if cnt < 1000:
            fout_test.write(line)
        else:
            fout_train.write(line)
        
        cnt += 1

    fin.close()
    fout_test.close()
    fout_train.close()


def make_hdfs_file(input_path: str, output_path: str):
    fin_test = open(os.path.join(input_path, 'test_node.csv'), 'r', 1, 'utf8')
    fin_train = open(os.path.join(input_path, 'train_node.csv'), 'r', 1, 'utf8')
    fin_train_lines = fin_train.readlines()

    for line in fin_test.readlines()[:40]:
        print(line)
        fout = open(os.path.join(output_path, line.strip()), 'w+', 1, 'utf8')
        fout.writelines(fin_train_lines)
        fout.close()

    fin_test.close()
    fin_train.close()

if __name__ == '__main__':
    # split_test_node_and_train_node(
    #     input_file=INPUT_FILE_PATH,
    #     output_path=OUTPUT_PATH
    # )

    make_hdfs_file(
        input_path=OUTPUT_PATH,
        output_path=HDFS_FILE_PATH
    )