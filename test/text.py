import json
file = 'G:/06-002Python大数据尊享课程资料/必学部分课程资料/4-ETL/Day01/数据资料/JSON测试/x24'
data = []
for line in open(file, "r", encoding="utf-8"):
    data.append(json.loads(line))
print(data)
