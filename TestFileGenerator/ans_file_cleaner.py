import os

if __name__ == '__main__':
    for file in os.listdir('./'):
        if file.endswith('_ans.txt'):
            os.remove(file)