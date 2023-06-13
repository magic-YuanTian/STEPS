# this is an expert system for SQL edit/generation
# it should work because
# 1. the explanation only contains first order logic
# 2. the structure of logic is simple and cannot be abstracted to a large extent
# (the abstraction of sentence meanning is already be handled by the NL2SQL model)
import copy
import os
import sys
import sqlite3
from sql_metadata import Parser
import re
import sqlparse
import random
from pandas import DataFrame
import json
import editdistance




# generate noun based on the database schema from a raw Noun string
def generateNoun(rawNoun, dbid):
    noun = rawNoun
    noun = noun.strip()

    # get the schema of database
    table_id_list = os.listdir('../../DBjson/' + dbid)
    db_dict = {}
    for table in table_id_list:
        pure_tb = table.replace('.json', '')
        file_name = '../../DBjson/' + dbid + '/' + table
        with open(file_name, 'r') as f:
            # the content of table (which is a dictionary list)
            table_content = json.load(f)
        # get columns in the table
        column_list = []
        if len(table_content) > 0:
            column_list = list(table_content[0].keys())
        db_dict[pure_tb] = column_list[:]

    # --- finished getting the schema, which is stored in db_dict

    # if there is ' of ', separate the noun into column and table; otherwise, it's column name
    # calculate the edit distance to possible column names and table names
    # if there is table, the edit distance = dis(table) + dis(column)
    # find the column name and table name with the least edit distance

    final_columns = []
    final_tables = []

    ori_noun = noun  # original noun
    # identify aggregation function
    agg_max_list = ['max ', 'maximum ', 'highest ', 'largest ', 'most ', 'biggest ', 'greatest ', ]
    agg_min_list = ['min ', 'minimum ', 'lowest ', 'least ', 'smallest ', 'slightest ', 'littlest ']
    agg_avg_list = ['average ', 'mean ']
    agg_sum_list = ['sum ', 'summation ', 'accumulation of ', 'totality ']

    func = ''

    if func == '':
        keep_flag = True  # the flag for being in the loop
        for tok in agg_max_list:
            if tok in noun:
                # in case this tok also exists in the tables
                for key in db_dict.keys():
                    for tb in db_dict[key]:
                        if tok.strip() in tb:
                            keep_flag = False

                if not keep_flag:
                    func = ''
                    noun = ori_noun
                    break

                func = 'max'
                # remove this part
                noun = noun.replace('the maximum value of ', '')
                noun_toks = noun.split()
                for idx in range(len(noun_toks)):
                    if tok in noun_toks[idx]:
                        noun_toks[idx] = ''
                noun = ' '.join(noun_toks)

    if func == '':
        keep_flag = True  # the flag for being in the loop
        for tok in agg_min_list:
            if tok in noun:
                # in case this tok also exists in the tables
                for key in db_dict.keys():
                    for tb in db_dict[key]:
                        if tok.strip() in tb:
                            keep_flag = False

                if not keep_flag:
                    func = ''
                    noun = ori_noun
                    break

                func = 'min'
                # remove this part
                noun = noun.replace('the minimum value of ', '')
                noun_toks = noun.split()
                for idx in range(len(noun_toks)):
                    if tok in noun_toks[idx]:
                        noun_toks[idx] = ''
                noun = ' '.join(noun_toks)

    if func == '':
        if 'the average value of ' in noun:
            noun = noun.replace('the average value of ', '')
            func = 'avg'
        else:
            keep_flag = True  # the flag for being in the loop
            for tok in agg_avg_list:
                if tok in noun:
                    # in case this tok also exists in the tables
                    for key in db_dict.keys():
                        for tb in db_dict[key]:
                            if tok.strip() in tb:
                                keep_flag = False

                    if not keep_flag:
                        func = ''
                        noun = ori_noun
                        break

                    func = 'avg'
                    noun = noun.replace('the average value of ', '')
                    noun_toks = noun.split()
                    for idx in range(len(noun_toks)):
                        if tok in noun_toks[idx]:
                            noun_toks[idx] = ''
                    noun = ' '.join(noun_toks)

    if func == '':
        keep_flag = True  # the flag for being in the loop
        for tok in agg_sum_list:
            if tok in noun:
                # in case this tok also exists in the tables
                for key in db_dict.keys():
                    for tb in db_dict[key]:
                        if tok.strip() in tb:
                            keep_flag = False

                if not keep_flag:
                    func = ''
                    noun = ori_noun
                    break

                func = 'sum'
                noun = noun.replace('the sum of ', '')
                noun_toks = noun.split()
                for idx in range(len(noun_toks)):
                    if tok in noun_toks[idx]:
                        noun_toks[idx] = ''
                noun = ' '.join(noun_toks)

    best_matched = {"table": "", "column": ""}
    if func == '':
        if 'number of ' in noun:
            func = 'count'
            noun = noun.replace('the number of ', '')
            noun = noun.replace('number of ', '')

    # handle special situations
    if noun == 'all':
        if '*' not in final_columns:
            final_col = '*'
            best_matched["table"] = table_id_list[0]  # for this case, just name table to 0
    elif 'the number' in noun and 'of' not in noun:
        if 'count(*)' not in final_columns:
            final_col = 'count(*)'
            best_matched["table"] = table_id_list[0]  # for this case, just name table to 0

    # there is table
    elif ' of ' in noun:
        temp_column = noun.split(' of ')[0]
        temp_table = noun.split(' of ')[1]

        # calculate the edit distance
        min_distance = 10000000000  # infinite
        for table in db_dict.keys():
            table_dis = editdistance.eval(temp_table, table)
            for col in db_dict[table]:
                column_dis = editdistance.eval(temp_column, col)
                # update to the combination of table and column with a shorter distance
                if (table_dis + column_dis) < min_distance:
                    min_distance = table_dis + column_dis
                    best_matched["table"] = table
                    best_matched["column"] = col

        final_col = best_matched["table"] + '.' + best_matched["column"]


    # only column
    else:

        min_distance = 10000000000  # infinite

        for table in db_dict.keys():
            for col in db_dict[table]:
                column_dis = editdistance.eval(noun, col)
                # update to the combination of table and column with a shorter distance
                if column_dis < min_distance:
                    min_distance = column_dis
                    best_matched["table"] = table
                    best_matched["column"] = col

        final_col = best_matched["table"] + '.' + best_matched["column"]

    # add aggregation function
    # add aggregation function
    if func == 'max':
        final_col = 'max(' + final_col + ')'
    elif func == 'min':
        final_col = 'min(' + final_col + ')'
    elif func == 'avg':
        final_col = 'avg(' + final_col + ')'
    elif func == 'sum':
        final_col = 'sum(' + final_col + ')'
    elif func == 'count':
        final_col = 'count(' + final_col + ')'

    return final_col, best_matched["table"]


# raw condition could contain multiple small conditions devided by 'and', 'or'
def generateCondition(rawCondition, dbid):
    # get conditions
    # conditions = re.split(' and | or |,', exp)
    conditions = []

    # get the relationships between these conditions
    relationships = []

    where_toks = rawCondition.split()
    condt = []
    between_flag = True
    for tok in where_toks:
        if tok == 'and' or tok == 'or' and between_flag:
            relationships.append(tok)
            conditions.append(' '.join(condt))
            condt = []
        else:
            # for between, jump next and
            if tok == 'between':
                between_flag = False
            if tok == 'and':
                between_flag = True
            condt.append(tok)

    if len(condt):
        conditions.append(' '.join(condt))

    # the order is important
    gt_operators = [' is greater than ', ' is more than ', ' are greater than ', ' are more than ', ' is better than ',
                    ' are better than ']
    ls_operators = [' is less than ', ' is lower than ', ' is fewer than ', ' are less than ', ' are lower than ',
                    ' are fewer than ']
    gteq_operators = [' is not less than ', ' is not lower than ', ' is not fewer than ', ' are not less than ',
                      ' are not lower than ', ' are not fewer than ', ' is greater than or equal to ',
                      ' is more than or equal to ', ' are greater than or equal to ', ' are more than or equal to ',
                      ' greater than or equal to ', ' more than or equal to ', ' is better than or equal to ',
                      ' are better than or equal to ']
    lseq_operators = [' is not greater than ', ' is not more than ', ' are not greater than ', ' are not more than ',
                      ' not more than ', ' not greater than ', ' is less than or equal to ',
                      ' is lower than or equal to  ', ' is fewer than or equal to  ', ' are less than or equal to  ',
                      ' are lower than or equal to  ', ' are fewer than or equal to  ']

    gteq_operators2 = [' greater than ', ' more than ']
    ls_operators2 = [' lower than ', ' fewer than ']

    notIs_operator = [" is not equal to", " are not equal to ", " is not ", " isn't ", " are not ", " aren't ",
                      " not equal ", " not equals "]
    is_operators = [' is equal to', ' are equal to ', ' is ', ' are ', ' equal ', ' equals ']

    like_operators = [' like ', ' is in the form of ', ' are in the form of ', ' in the form of ']

    between_operators = [' is between ', ' are between ', ' between ']

    # parse each condition into <noun operator noun>
    for cond in conditions:
        op_status = ''
        noun1 = ''
        noun2 = ''

        if op_status == '':
            for op in gt_operators:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = '>'
                    break

        if op_status == '':
            for op in ls_operators:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = '<'
                    break

        if op_status == '':
            for op in gteq_operators:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = '>='
                    break

        if op_status == '':
            for op in lseq_operators:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = '<='
                    break

        if op_status == '':
            for op in gteq_operators2:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = '>'
                    break

        if op_status == '':
            for op in ls_operators2:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = '<'
                    break

        if op_status == '':
            for op in notIs_operator:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = '!='
                    break

        if op_status == '':
            for op in is_operators:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = '='
                    break

        if op_status == '':
            for op in like_operators:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = 'like'
                    break

        if op_status == '':
            for op in between_operators:
                if op in cond:
                    noun1 = cond.split(op)[0]
                    noun2 = cond.split(op)[1]
                    op_status = 'between'
                    break

        # ------ finished getting <raw noun1> <op_status> <raw noun2>

        # if it is a pure number, return it
        if noun1.isdigit():
            final_noun1 = noun1
        else:
            final_noun1 = generateNoun(noun1, dbid)[0]

        if noun2.isdigit():
            final_noun2 = noun2
        else:
            final_noun2 = generateNoun(noun2, dbid)[0]

        result = final_noun1 + ' ' + op_status + ' ' + final_noun2

        return result


# this one could be handled by nl2sql machine learning model
# explanation to subexpression function for "select ... from ... join ... "
def exp2sub_select(exp, dbid):
    original_exp = exp
    exp = exp.lower()

    # remove 'that has' in explanation, and keep the first part
    exp = exp.split(' that has ')[0]

    # get the schema of database
    table_id_list = os.listdir('../../DBjson/' + dbid)
    db_dict = {}
    for table in table_id_list:
        pure_tb = table.replace('.json', '')
        file_name = '../../DBjson/' + dbid + '/' + table
        with open(file_name, 'r') as f:
            # the content of table (which is a dictionary list)
            table_content = json.load(f)
        # get columns in the table
        column_list = []
        if len(table_content) > 0:
            column_list = list(table_content[0].keys())
        db_dict[pure_tb] = column_list[:]

    # --- finished getting the schema, which is stored in db_dict

    # start parsing the explanation
    # remove the first verb
    possible_verb = ['get', 'set', 'show', 'show me', 'find', 'demonstrate', 'manifest', 'display', 'reveal', 'tell']

    for verb in possible_verb:
        # remove it if the explanation starts with such a verb word
        if exp.startswith(verb):
            exp = exp.replace(verb, '')
            break

    # what remaining is nouns, devide them by 'and', 'or'

    nouns = re.split(' and | or |,', exp)

    # if there is ' of ', separate the noun into column and table; otherwise, it's column name
    # calculate the edit distance to possible column names and table names
    # if there is table, the edit distance = dis(table) + dis(column)
    # find the column name and table name with the least edit distance

    final_columns = []
    final_tables = []
    for rawNoun in nouns:

        final_col, final_tb = generateNoun(rawNoun, dbid)

        if final_col not in final_columns:
            final_columns.append(final_col)
        if final_tb not in final_tables:
            final_tables.append(final_tb)

    # add join if there are multiple tables

    # join operation is a graph
    # table = node
    # column = edge (interface)
    # edit distance of 2 column = cost of the edge
    # for each time, merge 2 connected component with a minimum edit distance until the graph is connected
    # join a new table into the current connect component
    # greedy is optimal
    # minimal spanning tree
    # like Prim algorithm

    # multiple tables, need to be joined
    if len(final_tables) > 0:

        # construct an undirected graph with the shortest edit distance between each node pair
        graph = [[None for i in range(len(final_tables))] for j in range(len(final_tables))]  # adjacent matrix
        for i in range(len(final_tables)):
            table = final_tables[i]
            # build a link for each remaining table
            for j in range(len(final_tables)):
                remain_tb = final_tables[j]
                # jump itself
                if table == remain_tb:
                    continue
                # find the minimal distance between these 2 nodes
                temp_col_list1 = db_dict[table]
                min_distance = 100000000
                for col1 in temp_col_list1:
                    temp_col_list2 = db_dict[remain_tb]
                    for col2 in temp_col_list2:
                        temp_min_distance = editdistance.eval(col1, col2)
                        # update shortest distance
                        if temp_min_distance <= min_distance:
                            graph[i][j] = (
                                col1, col2,
                                temp_min_distance)  # col2 is in current table, col2 is in the remaining table
                            # graph[j][i] = (col2, col1, temp_min_distance)
                            min_distance = temp_min_distance

        remaining_tables = final_tables[:]
        connect_mask = [0] * len(final_tables)  # 0 means not included, 1 means included in the current component
        cur_connected_component = [remaining_tables[0]]
        connect_mask[0] = 1

        # the final res7ult
        final_sql = 'select '
        for i in range(len(final_columns)):
            col = final_columns[i]
            if i == 0:
                final_sql += col
            else:
                final_sql += (', ' + col)

        final_sql += ' from '
        final_sql += final_tables[0]

        # find the shortest edit distance between current component and edges in remaining nodes

        # as long as there are remaining tables, find an edge through this cut
        while 0 in connect_mask:
            min_dis = 100000000000

            # find the shortest link between current connected component between remaining tables
            for cur in range(len(connect_mask)):
                if connect_mask[cur] == 1:
                    for rem in range(len(connect_mask)):
                        if connect_mask[rem] == 0:
                            if graph[cur][rem][2] <= min_dis:
                                min_dis = graph[cur][rem][2]
                                best_cols = (cur, rem, final_tables[cur] + '.' + graph[cur][rem][0],
                                             final_tables[rem] + '.' + graph[cur][rem][1])

            connect_mask[best_cols[1]] = 1
            final_sql += ' join '
            final_sql += final_tables[best_cols[1]]
            final_sql += (' on ' + best_cols[2] + ' = ' + best_cols[3])

            # print(final_sql)
            # print(connect_mask)

        return final_sql


def exp2sub_where(exp, dbid):
    original_exp = exp
    exp = exp.lower()

    exp = exp.replace('keep the records that', '')
    exp = exp.strip()

    if ' where ' in exp:
        exp = exp.split(' where ', 1)[1]
    if ' that ' in exp:
        exp = exp.split(' that ', 1)[1]

    return 'where ' + generateCondition(exp, dbid)


def exp2sub_group(exp, dbid):
    exp = exp.lower()

    raw_condition = ''
    noun = exp.replace('group the records based on ', '')
    if 'based on' in noun:
        noun = noun.split('based on')[1]

    if ' that ' in exp:
        raw_condition = noun.split(' that ')[1]
        noun = noun.split(' that ')[0]

    noun = generateNoun(noun, dbid)[0]

    if raw_condition != '':
        condition = generateCondition(raw_condition, dbid)
        condition = ' having ' + condition
    else:
        condition = ''

    result = 'group by ' + noun + condition

    return result


def exp2sub_order(exp, dbid):
    exp = exp.lower()

    # decide asd or desc

    if 'ascend' in exp:
        direction = 'asc'
    elif 'descend' in exp:
        direction = 'desc'
    else:
        direction = 'asc'  # asc is the default

    # limit detect: as long as there is number tok
    limit_value = None
    exp_tokens = exp.split()
    for tok in exp_tokens:
        if tok.isdigit():
            limit_value = tok

    noun = exp.replace('order these records based on ', '')
    if 'based on' in noun:
        noun = noun.split('based on')[1]

    if 'and sort' in noun:
        noun = noun.split('and sort')[0]

    noun = generateNoun(noun, dbid)[0]

    result = 'order by ' + noun + ' ' + direction
    if limit_value is not None:
        result += (' limit ' + limit_value)

    return result


# determine the subexpression category based on features in the explanation
def subexpression_category(exp):
    exp = exp.lower()

    # features for select clause
    possible_select_feature = ['get', 'set', 'show', 'show me', 'find', 'demonstrate', 'manifest',
                               'display', 'reveal', 'tell']
    for tok in possible_select_feature:
        if exp.startswith(tok):
            return 'select'

    # features for where clause category
    possible_where_feature = ['keep the record', 'keep the records', 'keep that', 'make the record',
                              'make the records', 'make it', 'keep the result']
    for tok in possible_where_feature:
        if tok in exp:
            return 'where'

    # features for where clause category
    possible_group_feature = ['group the records', 'group the record', 'group records', 'group them',
                              'group it', 'in group']
    for tok in possible_group_feature:
        if tok in exp:
            return 'group'

    # features for where clause category
    possible_order_feature = ['sort them', 'sort the record', 'sort the records', 'sort records', 'sort it'
                                                                                                  'order them',
                              'order the record', 'order the records', 'order records', 'order it'
                                                                                        'return the top',
                              ' descending ', ' ascending ']
    for tok in possible_order_feature:
        if tok in exp:
            return 'order'

    return 'select'  # select is the default category


def exp2subexpression(exp, dbid):
    category = subexpression_category(exp)
    subexpression = ''
    try:
        if category == 'select':
            subexpression = exp2sub_select(exp, dbid)
        elif category == 'where':
            subexpression = exp2sub_where(exp, dbid)
        elif category == 'group':
            subexpression = exp2sub_group(exp, dbid)
        elif category == 'order':
            subexpression = exp2sub_order(exp, dbid)

    except Exception as e:
        print(e)

    return subexpression

# a function that can remove table of a column (X.Y ---> Y; MAX(X.Y) ---> MAX(Y) )
def removeTableOfColumn(column_list):
    res = []
    for col in column_list:
        if '.' in col:
            if '(' in col:
                temp_col = re.split(r'\(|\)', col)
                final_col = temp_col[0] + '(' + temp_col[1].split('.')[1] + ')'
            else:
                final_col = col.split('.')[1]
        else:
            final_col = col

        res.append(final_col)

    return res

# get table of a column
def TableOF(column):
    table = ''
    if '.' in column:
        if '(' in column:
            temp_col = re.split(r'\(|\)', column)
            table = temp_col[1].split('.')[0]
        else:
            table = column.split('.')[0]

    return table

# get pure column of an entity
def pureColumnOF(column):
    res_col = ''
    if '.' in column:
        if '(' in column:
            temp_col = re.split(r'\(|\)', column)
            res = temp_col[1].split('.')[-1]
        else:
            res = column.split('.')[-1]
    else:
        res = column

    return res

def simpleConcatenate(sub_list):
    res = ''
    for sub in sub_list:
        res += ' ' + sub

    res = res.strip()
    return res

# When the modification is trivial, generate subexpression by replacing the word
def simpleModification(old_exp, new_exp, old_sub):
    # # if explanations are the same, no need to modify
    # if old_exp.lower() == new_exp.lower():
    #     return old_sub

    old_tok_list = old_exp.split()
    new_tok_list = new_exp.split()

    new_sub = old_sub

    if len(old_tok_list) == len(new_tok_list):
        for i in range(len(old_tok_list)):
            if new_tok_list[i] != old_tok_list[i]:
                if old_tok_list[i] in old_sub:
                    new_sub = new_sub.replace(old_tok_list[i], new_tok_list[i])
                else:
                    return False

        return new_sub

    else:
        if len(old_tok_list) > len(new_tok_list):

            # check if all tokens in the new_tok_list are in old_tok_list
            all_in_flag = True
            for tok in new_tok_list:
                if tok not in old_tok_list:
                    all_in_flag = False

            if all_in_flag:
                # get the removed tokens
                removed_tok_list = []
                for tok in old_tok_list:
                    if tok not in new_tok_list:
                        removed_tok_list.append(tok)

                for tok in removed_tok_list:
                    comma_tok = ', ' + tok
                    if comma_tok in new_sub:
                        new_sub = new_sub.replace(comma_tok, '')
                    elif tok in new_sub:
                        new_sub = new_sub.replace(tok, '')
                    else:
                        return False

                return new_sub
            else:
                return False

    return False

# only compose the first met corresponding clause
def simpleCompose(subs, dbid):
    res = ''

    categories = ['select', 'from', 'where', 'group', 'having', 'order']
    # 6 categories
    subexpression_dict = {
        # 'select': '',
        # 'from': '',
        # 'where': '',
        # 'group': '',
        # 'having': '',
        # 'order': '',
    }

    for sub in subs:
        if sub.lower().startswith('select ') and 'select' not in subexpression_dict.keys():
            subexpression_dict['select'] = sub
        elif sub.lower().startswith('from ') and 'from' not in subexpression_dict.keys():
            subexpression_dict['from'] = sub
        elif sub.lower().startswith('where ') and 'where' not in subexpression_dict.keys():
            subexpression_dict['where'] = sub
        elif sub.lower().startswith('group by ') and 'group' not in subexpression_dict.keys():
            subexpression_dict['group'] = sub
        elif sub.lower().startswith('having ') and 'having' not in subexpression_dict.keys():
            subexpression_dict['having'] = sub
        elif sub.lower().startswith('order by ') and 'order' not in subexpression_dict.keys():
            subexpression_dict['order'] = sub

    # handle missing select
    if 'select' not in subexpression_dict.keys():
        subexpression_dict['select'] = 'SELECT *'

    for key in categories:
        if key in subexpression_dict.keys():
            res += ' ' + subexpression_dict[key]

    res = res.strip()

    return res

# new version
# compose
def composeSQL(sub_list, dbid):
    print('\nsub list to compose')
    print(sub_list)

    # this is for IEU
    if len(sub_list) == 0:
        return ''

    # e.g. : first query intersect the second query
    if len(sub_list) == 1:
        return sub_list[0]

    final_SQL = ""
    ori_FROM = ''
    columns = [] # store all column names appeared
    ori_columns = []
    tables = [] # record necessary tables
    conditions = []
    # X.Y will be converted to Y at the beginning (for higher accuracy), and all tables will be calculated automatically

    # 6 clauses : select, from, where, group by, having, order by
    select_clause = ""
    from_clause = ""
    where_clause = ""
    group_clause = ""
    order_clause = ""

    # get each clause
    # by default, fill the 1st if there are multiple clauses for 'group by' and 'order by'

    # handle where_clause
    for sub in sub_list:
        if sub.lower().startswith('where'):
            # make aggregation functions as single units
            sub = re.sub(' *\( *', '(', sub)
            sub = re.sub(' *\)', ')', sub)

            sub = sub[6:] # remove 'where'
            # add to conditions
            conditions.append(sub)

            # get conditions (separated by 'and', 'or') ---> for getting new columns
            temp_conditions = re.split(' and | AND | or | OR ', sub)
            # get new columns (by default, the first token in each sub-condition)
            for cond in temp_conditions:
                col = cond.split()[0]
                if col not in columns:
                    columns.append(col)

    # handle group_clauses
    # by default, only use the first one
    for sub in sub_list:
        if sub.lower().startswith('group'):
            # make aggregation functions as single units
            sub = re.sub(' *\( *', '(', sub)
            sub = re.sub(' *\)', ')', sub)
            if group_clause == "":
                group_clause = ' ' + sub
                # add new column
                # remove 'group by ' order by name and return the first token
                col = sub[9:].split()[0]
                if col not in columns:
                    columns.append(col)

                # detect column after 'having'
                if ' having ' in sub.lower():
                    col = sub.lower().split(' having ')[1].split()[0]
                if col not in columns:
                    columns.append(col)

            else:
                break

    # handle order
    # by default, only use the first one
    for sub in sub_list:
        if sub.lower().startswith('order'):
            # make aggregation functions as single units
            sub = re.sub(' *\( *', '(', sub)
            sub = re.sub(' *\)', ')', sub)
            if order_clause == "":
                order_clause = ' ' + sub
                # add new column
                # remove 'order by ' order by name and return the first token
                col = sub[9:].split()[0]
                if col not in columns:
                    columns.append(col)
            else:
                break

    # handle select_clause
    # store columns & required tables
    select_counter = 0 # used to indicate the number of select clauses
    distinct_flag = False # used to indicate if there is the keyword 'DISTINCT' in select clause
    for sub in sub_list:
        if sub.lower().startswith('select'):
            select_counter += 1

            if 'distinct' in sub:
                sub = sub.replace('distinct', '')
                distinct_flag = True
            elif 'DISTINCT' in sub:
                sub = sub.replace('DISTINCT', '')
                distinct_flag = True

            # replace multiple spaces to 1
            sub = re.sub(' +', ' ', sub)

            # make aggregation functions as single units
            sub = re.sub(' *\( *', '(', sub)
            sub = re.sub(' *\)', ')', sub)

            # remove 'select'
            sub = sub[7:]

            if ' from ' in sub:
                parts = sub.split(' from ')
            elif ' FROM ' in sub:
                parts = sub.split(' FROM ')
            else:
                print("No from!!")
                # for wierd exception: no from
                parts = []
                parts[0] = sub
                parts[1] = ''

            # get new columns
            temp_columns = re.split(',| ', parts[0])
            temp_columns = [tok for tok in temp_columns if tok != '']  # remove ''
            for col in temp_columns:
                if col not in columns:
                    columns.append(col)

                ori_columns.append(col) # all column in select clauses will be added (repetitve columns doesn't matter)

            ori_FROM = parts[1]

    # generate from clause
    # judge if 'from ... join ...' need to be generated automatically

    # get the corresponding tables from table.json
    # open tables.json of spider

    print(os.getcwd())

    if os.getcwd() == '/home/yuan/Desktop/Projects/STEPS/backend/SQL2NL':
        table_path = '../../dataset/original/spider/tables.json'
    elif os.getcwd() == '/home/yuan/Desktop/Projects/STEPS':
        table_path = 'dataset/original/spider/tables.json'
    else:
        table_path = '../dataset/original/spider/tables.json'


    with open(table_path, 'r') as f:
        DB_list = json.load(f)

    # find the corresponding db
    db_dict = {}
    for db in DB_list:
        if dbid == db['db_id']:
            db_dict = db
    # if there is no such db, throw an exception
    if db_dict == {}:
        raise Exception('no such DB in tables.json')

    table_indexes = []
    # get table indexes for each column
    # if there is original table, use it
    existed_table_idxs = []  # first records all existed tables
    table_names_lower = [tb.lower() for tb in db_dict['table_names_original']]
    for col in columns:
        if '.' in col and TableOF(col) in table_names_lower:
            idx = table_names_lower.index(TableOF(col))
            existed_table_idxs.append(idx)

    for col in columns:
        idx = -2
        # if there is a table and the table in the list
        if '.' in col and TableOF(col) in db_dict['table_names_original']:
            idx = db_dict['table_names_original'].index(TableOF(col))
        # table not in list/ no table
        # need to infer the table based on the column
        else:
            pure_col = pureColumnOF(col)
            for col_name in db_dict['column_names_original']:
                if pure_col == col_name[1]:
                    idx = col_name[0]
                    # if the table is in the existed tables, make it as first priority
                    if idx in existed_table_idxs:
                        break
            # if still no match, soft match (no case consideration)
            if idx == -2:
                for col_name in db_dict['column_names_original']:
                    if pure_col.lower() == col_name[1].lower():
                        idx = col_name[0]
                        break
            # otherwise, no such column, let idx = -2
        table_indexes.append(idx)

    # new columns are introduced, but existed tables can cover all columns

    cover_flag = True
    for col in columns:
        cover_flag = False
        for tb_data in db_dict['column_names_original']:
            if tb_data[1].lower() == pureColumnOF(col).lower() and tb_data[0] in existed_table_idxs:
                cover_flag = True
                break
        if not cover_flag:
            break

    # if there is only 1 select clause; and no more column names are added; and there exists 'from ..',  use the original 'from .. join ...'
    if ori_FROM != '' and select_counter == 1 and set(ori_columns) == set(columns):
        from_clause = ' FROM ' + ori_FROM
        final_columns = copy.deepcopy(ori_columns)
    # new columns are introduced, but existed tables can cover all columns
    elif ori_FROM != '' and select_counter == 1 and cover_flag:
        from_clause = ' FROM ' + ori_FROM
        final_columns = copy.deepcopy(ori_columns)

    # otherwise, generate 'from .. join ...' automatically based on all columns
    else:
        # remove column that corresponding table index = -2
        columns = [columns[i] for i in range(len(columns)) if table_indexes[i] != -2]
        # remove tabel indexes that = -2
        table_indexes = [table_indexes[i] for i in range(len(table_indexes)) if table_indexes[i] != -2]

        dup_columns = copy.deepcopy(columns)  # save a duplicate, used to match table_indexes
        final_columns = [] # when table is joined, the corresponding column will be added

        # generate join condition based on table indexes and foreign keys
        # Minimum spanning tree algorithm (like Prim's algorithm)
        # join is bidirectional, so beginning at the first table
        # if cannot ends up with a connected component, create 2 SQL queries with same constraints and UNION them together

        from_clause = ' FROM '
        joined_tables = []
        remaining_tables = copy.deepcopy(table_indexes)

        # so beginning at the first table
        # pop it and add to remaining tables
        temp_tb_idx = remaining_tables.pop(0)
        final_columns.append(dup_columns.pop(0)) # add the corresponding column to final column
        joined_tables.append(temp_tb_idx)

        from_clause += db_dict['table_names_original'][temp_tb_idx]

        # the maximum loop numbers
        for i in range(len(db_dict['foreign_keys'])):
            # break if remaining list is empty
            if not remaining_tables:
                break

            # remove repetitive tables
            temp_cnt = 0
            while temp_cnt < len(remaining_tables):
                if remaining_tables[temp_cnt] in joined_tables:
                    final_columns.append(dup_columns.pop(temp_cnt))
                    remaining_tables.pop(temp_cnt)
                    temp_cnt = 0
                    continue
                temp_cnt += 1

            for relation in db_dict['foreign_keys']:
                tb1 = db_dict['column_names_original'][relation[0]][0]
                tb2 = db_dict['column_names_original'][relation[1]][0]
                # join if the relation covers 2 tables, 1 in join, the other in remaining (order doesn't matter)
                if (tb1 in joined_tables and tb2 in remaining_tables) or (tb2 in joined_tables and tb1 in remaining_tables):
                    # get columns
                    col1 = db_dict['column_names_original'][relation[0]][1]
                    col2 = db_dict['column_names_original'][relation[1]][1]

                    # judge which one is new table
                    if tb1 in joined_tables:
                        new_tb = tb2
                    else:
                        new_tb = tb1

                    # update from_clause
                    from_clause += ' JOIN ' + db_dict['table_names_original'][new_tb] + ' on ' + \
                                   db_dict['table_names_original'][tb1] + '.' + col1 + \
                                   ' = ' + db_dict['table_names_original'][tb2] + '.' + col2

                    # update joined_tables and remaining_tables

                    final_columns.append(dup_columns.pop(remaining_tables.index(new_tb))) # add the corresponding column to final column
                    joined_tables.append(remaining_tables.pop(remaining_tables.index(new_tb)))

                    break

        # if all tables cannot join together
        if remaining_tables:
            print("tables cannot join together!")

    # generate select clause
    final_columns = removeTableOfColumn(final_columns) # X.Y --> Y
    select_clause += 'SELECT '
    if distinct_flag:
        select_clause += 'DISTINCT '
    select_clause += ', '.join(final_columns)

    # generate where clause
    # based on conditions
    # use 'AND' to concatenate all conditions
    if conditions:
        final_cond = ' AND '.join(conditions)
        where_clause += " WHERE " + final_cond

    # simply combine 4 clauses together
    result = select_clause + from_clause + where_clause + group_clause + order_clause

    print('\nregenerated result', flush=True)
    print(result, flush=True)
    return result

# check if X in X.Y is the table of Y
def col_in_table_check(sql, dbid):
    # open table.json
    with open('../dataset/paraphrased/tables.json', 'r') as f:
        DB_list = json.load(f)

    # find the corresponding db
    db_dict = {}
    for db in DB_list:
        if dbid.lower() == db['db_id'].lower():
            db_dict = db

    TB_lower_list = [tb.lower() for tb in db_dict['table_names_original']]

    tok_list = sql.split()
    for i in range(len(tok_list)):
        if '.' in tok_list[i]:
            tb_remove_flag = True # a flag used to indicate if this table name should be removed
            tb = tok_list[i].split('.')[0].lower()
            idx = -1
            if tb in TB_lower_list:
                idx = TB_lower_list.index(tb)
                for col_data in db_dict['column_names_original']:
                    if col_data[0] == idx and col_data[1].lower() == tb:
                        tb_remove_flag = False
            if tb_remove_flag:
                tok_list[i] = tok_list[i].split('.')[1]

    res = ' '.join(tok_list)
    return res






# old version
# merge subexpressions into a single sql
# def mergeSQL(sub_list, dbid):
#     final_SQL = ""
#     temp_flag = 0  # used to indicate if this type of clause is already added
#
#     entities = []
#     tables = []
#     # find select
#     for sub in sub_list:
#         if sub.lower().startswith('select'):
#
#             # make aggregation functions as single units
#             sub = re.sub(' *\( *', '(', sub)
#             sub = re.sub(' *\) *', ')', sub)
#
#             if temp_flag == 0:
#                 temp_flag = 1
#                 final_SQL += sub
#
#                 # update table list
#                 sub = sub[7:] # remove 'select'
#
#                 if ' from ' in sub:
#                     new_parts = sub.split(' from ')
#                 elif ' FROM ' in sub:
#                     new_parts = sub.split(' FROM ')
#                 else:
#                     print("No from! exception!")
#                     # for wierd exception
#                     new_parts = []
#                     # new_parts[0] = sub
#                     # new_parts[1] = ''
#                     new_parts.append(sub)
#                     new_parts.append('')
#
#
#                 # add new entities
#                 temp_entity_list = re.split(',| ', new_parts[0])
#                 temp_entity_list = [tok for tok in temp_entity_list if tok != ''] # remove ''
#                 for ent in temp_entity_list:
#                     if ent not in entities:
#                         entities.append(ent)
#
#                 # get new table tokens
#                 cur_tb_phrases = re.split(' join | JOIN ', new_parts[1])
#
#                 # add new tables
#                 for tb_phrase in cur_tb_phrases:
#                     if tb_phrase.split()[0] not in tables:
#                         tables.append(tb_phrase.split()[0])
#
#             else:
#                 # remove 'select'
#                 sub = sub[7:]
#
#                 # separate sql from 'from'
#                 if ' from ' in final_SQL:
#                     ori_parts = final_SQL.split(' from ')
#                 elif ' FROM ' in final_SQL:
#                     ori_parts = final_SQL.split(' FROM ')
#                 else:
#                     print("No from! exception!")
#
#                 if ' from ' in sub:
#                     new_parts = sub.split(' from ')
#                 elif ' FROM ' in sub:
#                     new_parts = sub.split(' FROM ')
#                 else:
#                     print("No from! exception!")
#                     # for wierd exception
#                     new_parts = []
#                     new_parts[0] = sub
#                     new_parts[1] = ''
#
#                 # get new table tokens
#                 cur_tb_phrases = re.split(' join | JOIN ', new_parts[1])
#
#                 # add old entities
#                 final_SQL = ori_parts[0]
#
#                 # add new entities
#                 temp_entity_list = re.split(',| ', new_parts[0])
#                 temp_entity_list = [tok for tok in temp_entity_list if tok != ''] # remove ''
#                 for ent in temp_entity_list:
#                     if ent not in entities:
#                         final_SQL += ', ' + ent
#                         entities.append(ent)
#
#                 # add old tables
#                 final_SQL += ' FROM ' + ori_parts[1]
#
#                 # add new tables
#                 for tb_phrase in cur_tb_phrases:
#                     if tb_phrase.split()[0] not in tables:
#                         final_SQL += ' JOIN ' + tb_phrase
#                         tables.append(tb_phrase.split()[0])
#
#     temp_flag = 0
#     # find where
#     # multiple where clauses will be concatenated by 'and'
#     for sub in sub_list:
#         if sub.lower().startswith('where'):
#             if temp_flag == 0:
#                 temp_flag = 1
#                 final_SQL += ' ' + sub
#             else:
#                 # remove 'where' at the begining
#                 sub = sub[6:]
#                 final_SQL += ' AND ' + sub
#
#     temp_flag = 0
#     # find group by
#     for sub in sub_list:
#         if sub.lower().startswith('group by'):
#             final_SQL += ' '
#             final_SQL += sub
#             break
#
#     temp_flag = 0
#     # find order
#     for sub in sub_list:
#         if sub.lower().startswith('order'):
#             final_SQL += ' '
#             final_SQL += sub
#             break
#
#     return final_SQL

if __name__ == '__main__':
    instruction = 'test1'
    if instruction == 'expert':
        while True:
            db_id = 'concert_singer'

            print('\n' + db_id)
            exp = input("input the explanation > ")
            sub_sql = exp2subexpression(exp, db_id)

            print(sub_sql)
    elif instruction == 'compose':
        dbid = 'concert_singer'
        sub_list = ['SELECT average FROM stadium',
                    'SELECT year FROM concert']

        SQL = composeSQL(sub_list, dbid)
        print(SQL)
    elif instruction == 'test1':
        sql = 'SELECT COUNT( * ) FROM airlines JOIN flights ON uid = airline" WHERE airline = "United" Airlines AND sourceairport = "AHD"'
        dbid = 'flight_2'
        res = col_in_table_check(sql, dbid)
        print(res)