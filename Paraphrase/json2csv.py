from pandas import DataFrame
import json

input_path = "data/dev_paraphrased.json"
input_file = open(input_path, "r")
json_obj = json.load(input_file)
input_file.close()

data = {}

dbid_list = []
subexpression_list = []
template_explanation_list = []
paraphrased_explanation_list = []


for cnt, ex in enumerate(json_obj):
    print(cnt)
    dbid_list.append(ex['db_id'])
    subexpression_list.append(ex['query'])
    template_explanation_list.append(ex['template explanation'])
    paraphrased_explanation_list.append(ex['question'])


data['DB id'] = dbid_list
data['SQL subexpression'] = subexpression_list
data['Template explanation'] = template_explanation_list
data['Pharaphrased explanation'] = paraphrased_explanation_list

df = DataFrame(data)
df.to_excel("paraphrased_result_csv.xls")