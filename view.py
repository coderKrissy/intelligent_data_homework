import bz2
import json

file_path = 'latest-lexemes.json.bz2'

with bz2.open(file_path, 'rb') as f:  # 使用二进制模式读取
    for i, line in enumerate(f):
        line_str = line.decode()
        if len(line_str) > 2:
            json_data = json.loads(line_str.rstrip(',\n'))
            print(json_data)
            print(type(json_data))
            print('*'*10)
        if i>10:
            exit(0)