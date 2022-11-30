'''
-----------------------------------------------------------------------------------------------
Description:To find KNN in following test files using sequential algorithm (O(n log n)),
            Just for generating test files and answer files in a short time.
-----------------------------------------------------------------------------------------------
Author: Warren Liu
Date: 11/21/2022
-----------------------------------------------------------------------------------------------
'''

import math
import os
from datetime import datetime
# import matplotlib.pyplot as plt


FILE_LIST = ['100.txt', '1000.txt', '10000.txt', '25000.txt',
             '50000.txt', '75000.txt', '100000.txt', '200000.txt', '250000.txt']
SIZE = 50


class Node:
    '''
    Class to store node x coordinate, y coordinate, and its distance to the target_node.
    '''

    def __init__(self, x, y, distance_to_target=0.0) -> None:
        self.x = x
        self.y = y
        self.distance_to_target = distance_to_target


def load_nodes(file_name: str):
    '''
    Load all nodes from file. 
    The fist line is the count of total nodes,
    The second line is the target_node,
    rest are test nodes.

    Args:
        `str` file_name: input file name.

    Returns:
        `Node` target_node, `Node List` coor_list
    '''
    with open(file_name, 'r') as f:
        coor_list = []
        line_cnt = 0
        target_node = None
        lines = f.readlines()

        for line in lines:
            line = line.split(' ', 1)

            # First line is the total count of coordinates in the file
            # It's not used here
            if line_cnt == 0:
                line_cnt += 1
                continue

            # Second line is the target_node
            elif line_cnt == 1:
                line_cnt += 1
                target_node = Node(float(line[0]), float(line[1]))

            # Rest are the test nodes
            else:
                line_cnt += 1
                coor_list.append(Node(float(line[0]), float(line[1])))

    return target_node, coor_list


def distance(node_1: Node, node_2: Node):
    '''
    Calculate Euclidean distance between 2 nodes.

    Args:
        `Node` node_1: the first node.
        `Node` node_2: the second node.

    Returns:
        `Float` distance: The distance between two nodes.
    '''
    return math.dist(list((node_1.x, node_1.y)), list((node_2.x, node_2.y)))


def knn(target_node: Node, coor_list: list[Node]):
    '''
    Find the knn in O(n log n) time: 
    loop each node and calculate its distance to the target_node.

    Args:
        `Node` target_node.
        `Node List` coor_list: the list containing all test nodes.

    Returns:
        `Node List` knn_list: the list containing top {SIZE} knn.
        `Node List` not_knn_list: the list containing other-than-knn nodes, 
        for validating the result.

    '''
    size = int(len(coor_list) / 10)
    knn_list = []
    not_knn_list = []

    # Loop each node, calculate distance
    for node in coor_list:
        node.distance_to_target = distance(target_node, node)
        not_knn_list.append(node)
    
    # sort
    not_knn_list = sorted(not_knn_list, key=lambda x: x.distance_to_target)

    # get the top K
    for _ in range(size):
        knn_list.append(not_knn_list.pop(0))

    return knn_list, not_knn_list


def knn_v2(target_node: Node, coor_list: list[Node]):
    '''
    KNN v2
    Instead sort the whole list by distance_to_target in the end,
    maintain a list that contains the top K,
    and update the top_k list in each iteration.
    To compare the excution time with the v1 to see if the performance is improved.
    ***ANS: NO, IT'S NOT, WAY MUCH SLOWER***

    Args:
        `Node` target_node.
        `Node List` coor_list: the list containing all test nodes.

    Returns:
        `Node List` knn_list: the list containing top {SIZE} knn.
        `Node List` not_knn_list: the list containing other-than-knn nodes, 
        for validating the result.
    
    '''
    size = int(len(coor_list) / 10)
    knn_list = []
    knn_list_max = None
    not_knn_list = []

    # loop each node
    for node in coor_list:
        # calculate distance
        node.distance_to_target = distance(target_node, node)

        # if knn list is not full, simply append to knn list
        if len(knn_list) < size:
            knn_list.append(node)
            if knn_list_max is None:
                knn_list_max = node.distance_to_target
            elif node.distance_to_target > knn_list_max:
                knn_list_max = node.distance_to_target
            
            # when the knn list is full at the first time,
            # sort the knn list, so the max node is in the end of the list
            if len(knn_list) == size:
                knn_list = sorted(knn_list, key=lambda x: x.distance_to_target)
        
        # if knn list is full
        else:
            # if this node's distance is greater than max,
            # simply add to not knn list directly
            if node.distance_to_target > knn_list_max:
                not_knn_list.append(node)
            
            # if this node's distance is smaller then max
            
            else:
                # the max is at the end of the knn_list,
                # move knn_list[-1] to not_knn_list, replace it with current node
                not_knn_list.append(knn_list[-1])
                knn_list[-1] = node
                # Sort the knn_list again, update the max
                knn_list = sorted(knn_list, key=lambda x: x.distance_to_target)
                knn_list_max = knn_list[-1].distance_to_target

    return knn_list, not_knn_list


def write_ans(file_name: str, target_node: Node, knn_list: list[Node]):
    '''
    Function to write the result to file. For validating other programs' result later.

    Args:
        `str` file_name: the input file name.
        `Node` target_node: the target node.
        `Node List` knn_list: the result list got from other functions.
    '''
    # Sort the result list by its distance to the target_node
    knn_list = sorted(knn_list, key=lambda x: x.distance_to_target)

    # The output file name
    ans_file_name = file_name.split('.', 1)[0] + '_ans.txt'

    with open(ans_file_name, 'w+') as f:
        f.write(f'Target_Node: {target_node.x} {target_node.y}')
        f.write('\n')
        for node in knn_list:
            f.write(f'{node.x}, {node.y}, {node.distance_to_target}')
            f.write('\n')


def validate_res_list(knn_list: list[Node], not_knn_list: list[Node]):
    '''
    Function to validate the answer. The max of knn_list should
    less or equal to the min of not_knn_list.

    Args:
        `Node List` knn_list: result list.
        `Node List` not_knn_list: the list containing all nodes not belong to result list.
    
    Returns:
        `bool`: True if the max of knn_list is less than or equal to the min of not_knn_list.
        False otherwise.
    '''
    max_knn = max(knn_list, key=lambda x: x.distance_to_target)
    min_not_knn = min(not_knn_list, key=lambda x: x.distance_to_target)
    return max_knn.distance_to_target <= min_not_knn.distance_to_target


if __name__ == '__main__':

    # Method 1: get all test file answers and validate
    # for file_name in os.listdir('./test_files'):

    #     if not file_name.endswith('0.txt'):
    #         continue

    #     start_time = datetime.today()
    #     target_node, coor_list = load_nodes(file_name)
    #     knn_list, not_knn_list = knn(target_node, coor_list)
    #     end_time = datetime.today()

    #     validate_ans = 'Validated' if validate_res_list(
    #         knn_list, not_knn_list) else 'Wrong'
    #     write_ans(file_name, target_node, knn_list)
    #     print(
    #         f'File: {file_name}, correctness: {validate_ans}, time elapsed: {(end_time - start_time).total_seconds()}')


    # Mthod 2: Get a single file result plot, data visualization
    # file_name = '100000.txt'

    # start_time = datetime.today()
    # target_node, coor_list = load_nodes(file_name)
    # knn_list, not_knn_list = knn(target_node, coor_list)
    # end_time = datetime.today()

    # validate_ans = 'Validated' if validate_res_list(
    #     knn_list, not_knn_list) else 'Wrong'
    # write_ans(file_name, target_node, knn_list)
    # print(
    #     f'File: {file_name}, correctness: {validate_ans}, time elapsed: {(end_time - start_time).total_seconds()}')

    # # draw plot
    # x = target_node.x
    # y = target_node.y
    # plt.scatter(x, y)

    # x = [node.x for node in knn_list]
    # y = [node.y for node in knn_list]
    # plt.scatter(x, y, c='orange')

    # x = [node.x for node in not_knn_list]
    # y = [node.y for node in not_knn_list]
    # plt.scatter(x, y, c='gray')

    # plt.show()


    # Method 3: compare single test file execution time
    # by using different knn
    # file_name = '1000000.txt'
    # target_node, coor_list = load_nodes(file_name)

    # print('done')

    # start_time = datetime.today()
    # knn_list, not_knn_list = knn(target_node, coor_list)
    # end_time = datetime.today()

    # validate_ans = 'Validated' if validate_res_list(
    #     knn_list, not_knn_list) else 'Wrong'
    # print(
    #     f'KNN_v1: file: {file_name}, correctness: {validate_ans}, time elapsed: {(end_time - start_time).total_seconds()}')

    # start_time = datetime.today()
    # knn_list, not_knn_list = knn_v2(target_node, coor_list)
    # end_time = datetime.today()

    # validate_ans = 'Validated' if validate_res_list(
    #     knn_list, not_knn_list) else 'Wrong'
    # print(
    #     f'KNN_v2: file: {file_name}, correctness: {validate_ans}, time elapsed: {(end_time - start_time).total_seconds()}')