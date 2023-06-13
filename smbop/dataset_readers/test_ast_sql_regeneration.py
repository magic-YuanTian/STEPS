from smbop.utils import moz_sql_parser as msp
import smbop.utils.ra_preproc as ra_preproc
from smbop.dataset_readers.enc_preproc import *
from smbop.utils.ra_postproc import *

def main():
    file_path = '../../dataset/train_spider.json'
    output_path = 'sql_regeneration_test.txt'
    out_f = open(output_path, "w")

    with open(file_path, "r") as data_file:
        json_obj = json.load(data_file)

    for ex in json_obj:
        sql = ex['query']
        sql_with_values = ex['query']
        print("\noriginal sql:", flush=True)
        print(sql, flush=True)

        no_select_flag = False
        try:
            # do some preprocess
            sql = sql.replace('! =', '!=')

            # do some hack here
            if sql.split()[0].lower() != 'select':
                sql1 = 'select aaa from aaa ' + sql
                sql_with_values1 = 'select aaa from aaa ' + sql_with_values
                no_select_flag = True
            else:
                sql1 = sql
                sql_with_values1 = sql_with_values

            tree_dict = msp.parse(sql1)
            tree_dict_values = msp.parse(sql_with_values1)

            # remove the fake select .. from ...
            if no_select_flag:
                del tree_dict['query']['select']
                del tree_dict['query']['from']
                del tree_dict_values['query']['select']
                del tree_dict_values['query']['from']

        except msp.ParseException as e:
            print(f"could'nt create AST for:  {sql}")
            return None

        # sql dict ---> tree object
        tree_obj = ra_preproc.ast_to_ra(tree_dict["query"])
        tree_obj_values = ra_preproc.ast_to_ra(tree_dict_values["query"])

        # # TEST: tree ---regenerate---> sql
        irra = ra_to_irra(tree_obj)
        sql_temp = irra_to_sql(irra)
        sql_temp = fix_between(sql_temp)
        sql_temp = sql.replace("LIMIT value", "LIMIT 1")

        print('\nRegenerated sql:', flush=True)
        print(sql_temp, flush=True)

        if sql != sql_temp:
            out_f.write("\n\noriginal sql:\n")
            out_f.write(sql)
            out_f.write("\nRegenerated sql:\n")
            out_f.write(sql_temp)

    out_f.close()

main()