import json
from pandas import DataFrame


raw_valid_examples = json.load(open("dev_reordered.json"))

with open('save.txt', 'w') as f:
    i = 0
    for exp in raw_valid_examples:
        f.write('----------------------------')
        f.write(exp['db_id'])
        f.write('/')
        f.write(str(i))
        f.write('----------------------------\n')
        f.write('Question:\n')
        f.write(exp['question'])
        f.write('\n')
        f.write('SQL:\n')
        f.write(exp['query'])
        f.write('\n')

        i += 1

# print("hi")
#
# data = {
#     'name': ['a', 'b', 'c'],
#     'age': [1, 2, 4],
#     'sex': ['male', 'female', 'hi']
# }
#
# df = DataFrame(data)
# df.to_excel("test.xls")