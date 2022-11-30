import random
import sys

# cnt = 250000


if __name__ == '__main__':
    # Method 1: get test file size from command line
    # cnt = int(sys.argv[1])

    # Method 2: def test file size manully
    file_size_list = []
    for i in range(1000000, 10000000, 1000000): # 1,000,000 - 9,000,000, step = 1,000,000
        file_size_list.append(i)
    for i in range(10000000, 60000000, 10000000): # 10,000,000 - 50,000,000, step = 10,000,000
        file_size_list.append(i)


    for cnt in (file_size_list):
        limit = cnt / 10
        exist_list = []

        with open(f'./{cnt}.txt', 'w+', 1, 'utf8') as f:
            f.write(str(cnt))
            f.write('\n')

            # For knn test file
            x = round(random.uniform(0, limit), 8)
            y = round(random.uniform(0, limit), 8)
            # exist_list.append((x, y))
            f.write(str(x) + ' ' + str(y))
            f.write('\n')
            # End for

            for i in range(cnt):
                x = round(random.uniform(0, limit), 8)
                y = round(random.uniform(0, limit), 8)

                # while (x, y) in exist_list:
                #     x = round(random.uniform(0, limit), 4)
                #     y = round(random.uniform(0, limit), 4)
                
                # exist_list.append((x, y))
                f.write(str(x) + ' ' + str(y))
                f.write('\n')