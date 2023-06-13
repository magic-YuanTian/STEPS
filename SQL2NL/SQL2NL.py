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
import nltk

try:
    import explanation2subexpression
except:
    from . import explanation2subexpression

try:
    from ramdomSQL import *
except:
    from .ramdomSQL import *

global g_p  # global Parser

global data_flag

#if true, the subexpression is for data generation;    (obsolete)
# otherwise, for explanation    '...not in ()'
data_flag = False


# direct manipulation

def needleman_wunsch(label, pred):
    m = len(label)
    n = len(pred)
    dp = [[1] * n for i in range(m)]
    for i in range(m):
        if pred[0] != label[i]:
            dp[i][0] = 0
        else:
            break
    for j in range(n):
        if pred[j] != label[0]:
            dp[0][j] = 0
        else:
            break

    for i in range(1, m):
        for j in range(1, n):
            if pred[j] == label[i]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j - 1], dp[i - 1][j], dp[i][j - 1])
    return dp

# find the most matched(aligned) part of string 1 in string 2
# using edit distance
def mostMatchedSubstring(s, sub):
    tok_list = s.split()
    min_distance = 99999999 # an infinite number
    res = ''

    cur_str = ''
    for tok in tok_list:
        cur_str += tok
        num = nltk.edit_distance(cur_str, sub)
        if num < min_distance:
            res = cur_str
            min_distance = num
        cur_str += ' '

    return res




# input: a sql
# return a no value token list
def removeValue(sql):
    # recognize the value: (1) number (2) quote

    tokens = sql.split('')

    for i in range(len(tokens)):
        # check if it is int
        if tokens[i].isdigit():
            tokens[i] = 'value'
            continue
        # check if it is float
        try:
            temp = float(tokens[i])
            tokens[i] = 'value'
            continue
        except ValueError:
            continue
        # check if it is quoted

    return result


def traceback(label, pred, dp):
    m = len(dp)
    n = len(dp[0])
    outLabel = []
    outPred = []
    i, j = m - 1, n - 1
    while i > 0 and j > 0:
        # need to add "label[i]==pred[j]", otherwise it makes mistakes such as "BBA" and "BAA"
        if dp[i - 1][j] == dp[i][j - 1] or label[i] == pred[j]:
            outLabel.append(label[i])
            outPred.append(pred[j])
            i -= 1
            j -= 1
        elif dp[i - 1][j] > dp[i][j - 1]:
            outLabel.append(label[i])
            outPred.append("_")
            i -= 1
        else:
            outLabel.append("_")
            outPred.append(pred[j])
            j -= 1

    while i > 0:
        if label[i] == pred[j]:
            outLabel.append(label[i])
            outPred.append(pred[j])
            i -= 1
            j -= 1
        else:
            outLabel.append(label[i])
            outPred.append("_")
            i -= 1

    while j > 0:
        if pred[j] == label[i]:
            outLabel.append(label[i])
            outPred.append(pred[j])
            i -= 1
            j -= 1
        else:
            outLabel.append("_")
            outPred.append(pred[j])
            j -= 1

    if i == 0 and j == 0:
        outLabel.append(label[i])
        outPred.append(pred[j])
    elif i == 0:
        outLabel.append(label[i])
        outPred.append("_")
    elif j == 0:
        outLabel.append("_")
        outPred.append(pred[j])

    return outLabel, outPred


# def directManipulationText(old_exp, new_exp, old_sql, dbid):
#
#     old_sql = old_sql.lower()
#
#     # ',' ---> ' , '
#     # make sure column names are separated
#     old_sql = old_sql.replace(',', ' , ')
#
#     # max ( a ) ---> max(a)
#     # make sure aggregation function as a whole
#     while ' (' in old_sql or '( ' in old_sql or ' )' in old_sql or ') ' in old_sql:
#         old_sql = old_sql.replace(' )', ')')
#         old_sql = old_sql.replace(') ', ')')
#         old_sql = old_sql.replace(' (', '(')
#         old_sql = old_sql.replace('( ', '(')
#
#     # old_exp = old_exp.lower()
#     # new_exp = new_exp.lower()
#
#     # # remove 'as XXX' and 'on ...'
#     # temp_toks = old_sql.split()
#     # for i in range(len(temp_toks)):
#     #     if temp_toks[i] == 'as' and i != len(temp_toks)-1:
#     #         temp_toks[i] = ''
#     #         temp_toks[i+1] = ''
#     #
#     # old_sql = ' '.join(temp_toks)
#
#     # get the schema of database
#     table_id_list = os.listdir('../../DBjson/' + dbid)
#     # dictionary of this database
#     # { table_id: [column1, colunm2, ...] }
#     db_dict = {}
#     for table in table_id_list:
#         pure_tb = table.replace('.json', '')
#         file_name = '../../DBjson/' + dbid + '/' + table
#         with open(file_name, 'r') as f:
#             # the content of table (which is a dictionary list)
#             table_content = json.load(f)
#         # get columns in the table
#         column_list = []
#         if len(table_content) > 0:
#             column_list = list(table_content[0].keys())
#         db_dict[pure_tb] = column_list[:]
#
#
#     # compare old_exp and new_exp, calculate their difference
#     new_sql = old_sql
#
#     # construct the mapping between old_str and old_sql
#
#     old_toks = old_exp.split()
#     new_toks = new_exps.split()
#
#     # # match tokens as perfect as possible
#     # # use new toks to match old toks
#     # diff_toks = []
#     # j = 0 # index of old tokens
#     # i = 0 # index of new tokens
#     # while i < len(new_toks):
#     #     if new_toks[i].lower() == old_toks[j].lower(): # matched
#     #         j += 1
#     #         i += 1
#     #         continue # 2 windows increase
#     #     else: # not matched
#     #         # add diff
#     #         diff_toks.append((i, new_toks[i]))
#     #         # decrease it whose remaining size is bigger
#     #         if len(new_toks)-i >= len(old_toks)-j:
#     #             i += 1
#     #         else:
#     #             j += 1
#
#     # match 2 token list using needleman-wunsch algorithm
#     dp = needleman_wunsch(old_toks, new_toks)
#     num_longest_common_substring = dp[-1][-1]
#     padding_old_toks, padding_new_toks = traceback(old_toks, new_toks, dp)
#     # need to reverse them
#     padding_old_toks = padding_old_toks[::-1]
#     padding_new_toks = padding_new_toks[::-1]
#
#
#     # finished calculating diffs
#
#     if old_sql.startswith('select'):
#         # get columns and tables
#         columns = []
#         tables = []
#
#         sql_toks = old_sql.split()
#         # +++++++ identify columns and tables in the original sql subexpression
#         # traverse and record tabls and colums with their index in the token list
#         column_flag = true # true if it's column area, and false if it is
#         for i in range(len(sql_toks)):
#             if sql_toks[i] == 'from':
#                 column_flag == false
#                 # add next token to tables
#                 if i != len(sql_toks)-1:
#                     tables.append((i+1, sql_toks[i+1]))
#                 else:
#                     print('error: from should not be the last token')
#                 continue
#             if i != 0 and column_flag and sql_toks[i] != ',':
#                 columns.append((i, sql_toks[i])) # with its index in sql_toks
#             # handle column names
#             if not column_flag and sql_toks[i] == 'join':
#                 if i != len(sql_toks)-1:
#                     tables.append((i+1, sql_toks[i+1]))
#                 else:
#                     print('error: join should not be the last token')
#
#         # ---- finished identifying columns and tables in the original sql subexpression
#
#         # modify original diffs based on context
#         # check them length of padding_old_toks and padding_new_toks, they should be the same
#         if len(padding_old_toks) != len(padding_new_toks):
#             print('error: the length of padding lists should be the same!')
#             return ""
#         else:
#             for i in range(len(padding_old_toks)):
#                 if padding_old_toks[i] != padding_new_toks[i]:
#                     pass
#                     # 3 situations: replace, add, miss
#                     # replace
#                     if padding_old_toks[i] != '_' and padding_new_toks[i] != '_':
#                         # find the replaced old toks in the
#                         if i > 0 and
#
#                         # table name is exactly after 'of'
#
#
#
#
#                     elif padding_old_toks[i] == '_':
#
#                     elif padding_new_toks[i] == '_':
#
#
#
#
#
#
#
#     elif old_sql.startswith('where'):
#
#     elif old_sql.startswith('group by'):
#
#     elif old_sql.startswith('order by'):
#
#     else:
#         print('error: the subexpression is strange')
#
#
#
#     return new_sql

# add collate nocase
def addCollateNocase(sql):
    if '\"' in sql or '\'' in sql:
        res = sql + ' COLLATE NOCASE'
    else:
        res = sql

    return res


def num2ordinalStr(num):
    ORDINAL_NUMBER = {1: "first", 2: "second", 3: "third", 4: "fourth", 5: "fifth", 6: "sixth", 7: "seventh",
                      8: "eighth", 9: "nineth", 10: "tenth", 11: "eleventh", 12: "twelfth", 13: "thirteenth",
                      14: "fourteenth", 15: "fifteenth", 16: "sixteenth", 17: "seventeenth", 18: "eighteenth",
                      19: "nineteenth"}
    if int(num) in ORDINAL_NUMBER.keys():
        res = ORDINAL_NUMBER[int(num)]
    else:
        res = str(num) + 'th'

    return res


# a function used to determine number
def isNumber(aString):
    try:
        float(aString)
        return True
    except:
        return False


def capitalizeKeyword(sql):
    p = Parser(sql)
    keywords = ['between', 'like', 'select', 'from', 'as', 'join', 'on', 'where', 'and', 'or', 'group by', 'having',
                'order by', 'asc', 'desc', 'avg', 'count', 'max', 'min']

    sql_tokens = sql.split()

    tokens = []  # result list
    for i, tok in enumerate(sql_tokens):
        # capitalize "order by", "group by" (keyword composed of multiple words)
        if tok.lower() == 'order' or tok.lower() == 'group':
            if i != len(sql_tokens) - 1 and sql_tokens[i + 1].lower() == 'by':
                tokens.append(tok.upper())
        elif tok.lower() == 'by' and i != 0:
            if sql_tokens[i - 1].lower() == 'order' or sql_tokens[i - 1].lower() == 'group':
                tokens.append(tok.upper())
        elif tok in keywords:
            tokens.append(tok.upper())  # capitalize keyword token
        else:
            tokens.append(tok)

    res = ' '.join(tokens)

    # replace

    # # there is a bug in this parser, we need to run it one more time to recognize function names
    # p = Parser(res)
    # tokens = []  # result list
    # for tok in p.tokens:
    #     if tok.is_keyword:
    #         tokens.append(tok.value.upper())  # capitalize keyword token
    #     elif tok.value.lower() == 'like': # 'LIKE' is operator (not keyword), also needed to be capitalized
    #         tokens.append(tok.value.upper())
    #     else:
    #         tokens.append(tok.value)
    #
    # res = ' '.join(tokens)

    return res


# currently only return the first encountered subexpression
def getSubExpressionBeforeNextKeyword(sql, keyword):
    sql = capitalizeKeyword(sql)
    keyword = keyword.upper()  # capitalizeKeyword the keyword

    p = Parser(sql)  # get the parser
    temp_flag = 0  # denote if the keyword is encountered
    sub_expression = ''
    temp_tokens = []

    possible_keywords = ['select', 'from', 'group by', 'having', 'order by', 'asc', 'desc', 'limit']

    # convert p.tokens for bug
    new_tok_list = []
    for t_tok in p.tokens:
        temp_flag2 = False # used to indicate if exception encountered
        for temp_keyword in possible_keywords:
            if temp_keyword.lower() != t_tok.value.lower() and t_tok.value.lower().endswith(temp_keyword.lower()):
                temp_list = []
                var = t_tok.value.lower().replace(temp_keyword.lower(), '')
                temp_list.append(var)
                temp_list.append(temp_keyword)
                new_tok_list += temp_list
                temp_flag2 = True
                break
        if not temp_flag2:
            new_tok_list.append(t_tok.value)

    if keyword in sql.upper():
        for tok in new_tok_list:
            if tok.lower() == keyword.lower():
                temp_flag = 1
                temp_tokens.append(tok)
                continue
            if temp_flag == 1 and tok.lower() in possible_keywords:
                break
            if temp_flag == 1:
                temp_tokens.append(tok)

        sub_expression = ' '.join(temp_tokens)

    return sub_expression


# get the description of nouns
# should input a sub sql expression
def getNouns(sub_sql, ori_sql):
    all_possible_keywords = [')', '(', 'between', '!=', '>=', '<=', '<', '>', '=', ',', 'select', 'from', 'as', 'join',
                             'on', 'where', 'and', 'or', 'group by', 'having', 'order by', 'asc', 'desc', 'avg',
                             'count', 'max', 'min']

    # judge if it is a number(for natural condition. e.g. age > 2.5)
    if isNumber(sub_sql):
        return sub_sql

    # if ' ' not in sub_sql:
    #     if 'X123X' in sub_sql.upper():
    #         return sub_sql
    #     else:
    #         return 'the ' + sub_sql

    if ' ' not in sub_sql and 'X123X' in sub_sql.upper():
        return sub_sql

    # the
    the_pos = ''
    if ' distinct ' not in sub_sql.lower() and ' between ' not in sub_sql.lower():
        the_pos = 'the '

    # quoted noun is adj. (no the)
    if 'a999specific999start999symbol' in sub_sql.lower():
        the_pos = ''

    p = Parser(ori_sql)
    g_p = Parser(ori_sql)
    temp_subexpression = 'SELECT ' + sub_sql  # adding SELECT is for ignoring the exception
    p1 = Parser(temp_subexpression)  # get the parser for subexpression

    # lower possible function names
    functionNames = ['count', 'max', 'min', 'avg', 'sum']

    temp_explanation = ''

    nouns = []
    noun = ''

    # find all functions and the corresponding paramenters
    funcs = []
    paras = []
    temp_paras = []

    # find the starting position of parameters (after '(' )
    positions = []
    for i in range(0, len(p1.tokens)):
        if p1.tokens[i].value == '(':
            positions.append(i)

    positions1 = [i + 1 for i in positions]  # move from '(' to the index of parameter

    # get function names (right in front of '(' )
    for pos in positions:
        funcs.append(p1.tokens[pos - 1].value.upper())

    # get parameter for this function
    for pos in positions1:
        temp_paras = []
        j = pos
        while p1.tokens[j].value != ')':
            if p1.tokens[j].value != ',':
                temp_paras.append(p1.tokens[j].value)
            j += 1
        paras.append(temp_paras)

    # generate nouns
    for i in range(0, len(funcs)):
        noun = ''
        # if i != 0:
        #     noun += ', '

        if funcs[i] == 'COUNT':
            if paras[i][0] == '*':
                noun += 'number of records'
            else:
                noun += 'number of '
        elif funcs[i] == 'MAX':
            noun += 'maximum value of '
        elif funcs[i] == 'MIN':
            noun += 'minimum value of '
        elif funcs[i] == 'AVG':
            noun += 'average value of '
        elif funcs[i] == 'SUM':
            noun += 'sum of '

        for j in range(0, len(paras[i])):
            if j != 0:
                noun += ' '
            if paras[i][j] != '*':
                noun += paras[i][j]

        nouns.append(noun)

    # add column not included in functions to nouns
    for i in range(1, len(p1.tokens)):
        if p1.tokens[i].value == '*' and p1.tokens[i].parenthesis_level == 0:
            nouns.append('all the records')
        elif p1.tokens[i].is_integer and p1.tokens[i].parenthesis_level == 0:
            nouns.append(p1.tokens[i].value)
        elif p1.tokens[i].value.lower() not in all_possible_keywords and p1.tokens[i].parenthesis_level == 0 and \
                p1.tokens[i].value != ')' and not (
                p1.tokens[i].value.lower() in functionNames and p1.tokens[i].next_token.value == '('):
            nouns.append(p1.tokens[i].value)


    # alias.Y ---> Y of X
    for i in range(0, len(nouns)):
        tok = nouns[i]
        if '.' in tok:
            temp_tok = tok.split('.')
            if len(temp_tok) != 2:
                print("exception: . should split it into 2 parts")
                break
            if temp_tok[0] in g_p.tables_aliases.keys():
                table_name = g_p.tables_aliases[temp_tok[0]]
            else:
                table_name = temp_tok[0]
            # hold the comma
            comma = ''
            if ',' in temp_tok[1]:
                comma = ','
                temp_tok[1] = temp_tok[1].replace(',', '')
            # Y of X
            res_str = temp_tok[1] + ' of ' + table_name + comma
            nouns[i] = res_str

    # add nouns to the explanation
    # X, Y, Z --> the X, the Y, and the Z
    for i in range(0, len(nouns)):
        if i != 0:
            if len(nouns) == 2:
                temp_explanation += ' and'
            else:
                if i == len(nouns)-1:
                    temp_explanation += ', and'
                else:
                    temp_explanation += ', '

        if 'all the records' not in nouns[i].lower():
            temp_explanation += ' ' + the_pos
        else:
            temp_explanation += ' '
        temp_explanation += nouns[i]


    return temp_explanation


def find_all(sub, s):
    index_list = []
    index = s.find(sub)
    while index != -1:
        index_list.append(index)
        index = s.find(sub, index + 1)

    if len(index_list) > 0:
        return index_list
    else:
        return -1


# def find_parser_tok_position(sub, sql):
#     res = []
#     tokens = sql.split()
#     for i in range(0, len(tokens)):
#         if tokens[i] == sub:
#             res.append(i)
#     return res

def NLforOperator(clause):
    if '>=' in clause:
        clause = clause.replace('>=', 'is greater than or equal to')
    if '<=' in clause:
        clause = clause.replace('<=', 'is less than or equal to')
    if '>' in clause:
        clause = clause.replace('>', 'is greater than')
    if '<' in clause:
        clause = clause.replace('<', 'is less than')
    if '!=' in clause:
        clause = clause.replace('!=', 'is not')
    if '=' in clause:
        clause = clause.replace('=', 'is')
    if ' NOT IN ' in clause:
        clause = clause.replace(' NOT IN ', ' is not in ')
    elif ' not in ' in clause:
        clause = clause.replace(' not in ', ' is not in ')
    elif ' IN ' in clause:
        clause = clause.replace(' IN ', ' is in ')
    elif ' in ' in clause:
        clause = clause.replace(' in ', ' is in ')
    if ' between ' in clause:
        clause = clause.replace(' between ', ' is between ')
    elif ' BETWEEN ' in clause:
        clause = clause.replace(' BETWEEN ', ' is between ')
    if ' NOT LIKE ' in clause:
        clause = clause.replace(' NOT LIKE ', ' is not in the form of ')
    elif ' not like ' in clause:
        clause = clause.replace(' not like ', ' is not in the form of ')
    elif ' not LIKE ' in clause:
        clause = clause.replace(' not LIKE ', ' is not in the form of ')
    elif ' LIKE ' in clause:
        clause = clause.replace(' LIKE ', ' is in the form of ')
    elif ' like ' in clause:
        clause = clause.replace(' like ', ' is in the form of ')

    return clause


# substitute nouns in the condition and make the operators natural languages
def naturalCondition(condition_clause, ori_sql):
    condition_sql = 'SELECT ' + condition_clause  # adding SELECT is for ignoring the exception when parsing

    core_keywords = ['select', 'from', 'as', 'join', 'on', 'where', 'group by', 'having', 'order by', 'asc', 'desc']
    operator_keywords = ['>', '<', '=', '!=', '>=', '<=', 'like', 'not like', 'in', 'not in', 'between']
    # p = Parser(condition_sql)
    # conflict: 'BETWEEN a AND b' v.s. 'AND'
    # solution: first replace 'BETWEEN a AND b' with 'BETWEEN axANDyb', and replace back later

    replace_back_list = []  # store the information to replace back when the function ends

    sql_toks0 = condition_clause.split()
    sql_toks = []
    # merge keywords (e.g. not, in ---> not in)
    idx = 0
    while idx < len(sql_toks0):
        temp_tok = sql_toks0[idx]
        if sql_toks0[idx].lower() == 'not' and idx != len(sql_toks0) - 1:
            if sql_toks0[idx + 1].lower() == 'like' or sql_toks0[idx + 1].lower() == 'in':
                temp_tok += ' ' + sql_toks0[idx + 1]
                idx += 1  # jump over next token
        elif sql_toks0[idx].lower() == 'group' and idx != len(sql_toks0) - 1:
            if sql_toks0[idx + 1].lower() == 'by':
                temp_tok += ' ' + sql_toks0[idx + 1]
                idx += 1  # jump over next token
        elif sql_toks0[idx].lower() == 'order' and idx != len(sql_toks0) - 1:
            if sql_toks0[idx + 1].lower() == 'by':
                temp_tok += ' ' + sql_toks0[idx + 1]
                idx += 1  # jump over next token

        sql_toks.append(temp_tok)
        idx += 1

    if 'between' in condition_clause or 'BETWEEN' in condition_clause:
        # find 'BETWEEN a AND b'
        for i in range(0, len(sql_toks)):
            if sql_toks[i] == 'BETWEEN' or sql_toks[i] == 'between':
                ori = ''
                new = ''
                # get the remaining part until (1) next key word OR (2) the end of the clause
                if i == len(sql_toks) - 1:  # between shouldn't be the last keyword
                    break
                j = i
                temp_tok = sql_toks[j + 1]
                j += 1
                and_flag = 0  # used to indicate whether 'AND' is met
                while True:
                    temp_tok_encode_value = temp_tok
                    # when AND is met
                    if temp_tok == 'AND':
                        temp_tok_encode_value = 'X123X'  # in case AND will conflict later
                        and_flag = 1

                    if len(ori) == 0:
                        ori += temp_tok
                    else:
                        ori += (' ' + temp_tok)

                    new += temp_tok_encode_value

                    # determine whether continue or end
                    if j == len(sql_toks) - 1:
                        break
                    elif sql_toks[j + 1] in core_keywords and and_flag == 1:
                        break
                    elif j <= len(sql_toks) - 3:
                        if sql_toks[j + 1].lower() == 'group' and sql_toks[j + 2].lower() == 'by':
                            break
                        elif sql_toks[j + 1].lower() == 'order' and sql_toks[j + 2].lower() == 'by':
                            break
                        else:
                            temp_tok = sql_toks[j + 1]
                            j += 1
                    else:
                        temp_tok = sql_toks[j + 1]
                        j += 1

                # replace the original condition_clause
                condition_sql = condition_sql.replace(ori, new)
                # also replace the original sql
                ori_sql = ori_sql.replace(ori, new)
                # ori: AND -> and (for lowering and in the explanantion)
                ori = ori.replace('AND', 'and')
                # add to the replace_back_list
                temp_replace_dict = {'ori': ori, 'new': new}
                replace_back_list.append(temp_replace_dict)

    # reparse
    # p = Parser(condition_sql)

    sql_toks0 = condition_sql.split()[1:]
    sql_toks = []
    # merge keywords (e.g. not, in ---> not in)
    idx = 0
    while idx < len(sql_toks0):
        temp_tok = sql_toks0[idx]
        if sql_toks0[idx].lower() == 'not' and idx != len(sql_toks0) - 1:
            if sql_toks0[idx + 1].lower() == 'like' or sql_toks0[idx + 1].lower() == 'in':
                temp_tok += ' ' + sql_toks0[idx + 1]
                idx += 1  # jump over next token
        elif sql_toks0[idx].lower() == 'group' and idx != len(sql_toks0) - 1:
            if sql_toks0[idx + 1].lower() == 'by':
                temp_tok += ' ' + sql_toks0[idx + 1]
                idx += 1  # jump over next token
        elif sql_toks0[idx].lower() == 'order' and idx != len(sql_toks0) - 1:
            if sql_toks0[idx + 1].lower() == 'by':
                temp_tok += ' ' + sql_toks0[idx + 1]
                idx += 1  # jump over next token

        sql_toks.append(temp_tok)
        idx += 1

    separaters = []  # 'OR', 'AND', 'NOT'
    nouns = []  # [[], [], ...]
    operators = []  # [ , ]
    operator_conditions = []  # [[], [], ...]

    flag = 0  # 0 for noun, 1 for condition
    for i in range(1, len(sql_toks)):

        if sql_toks[i].upper() == 'AND' or sql_toks[i].upper() == 'OR':
            separaters.append(sql_toks[i])
            flag = 0  # go to next condition
            continue
        if sql_toks[i].lower() in operator_keywords:
            operators.append(sql_toks[i])
            flag = 1
            continue
        if flag == 0:
            if len(nouns) <= len(separaters):
                atemp = []
                nouns.append(atemp)
            nouns[len(separaters)].append(sql_toks[i])

        else:
            if len(operator_conditions) <= len(separaters):
                atemp = []
                operator_conditions.append(atemp)
            operator_conditions[len(separaters)].append(sql_toks[i])

    # generate NL
    explanation = ''

    try:
        explanation = getNouns(' '.join(nouns[0]), ori_sql) + ' ' + operators[0] + ' ' + getNouns(
            ' '.join(operator_conditions[0]), ori_sql)  # explanation for the first part
    except Exception as e:
        pass

    for i in range(0, len(separaters)):
        explanation += ' '
        explanation += separaters[i]
        explanation += ' '
        explanation += getNouns(' '.join(nouns[i + 1]), ori_sql)
        explanation += ' '
        explanation += operators[i + 1]
        explanation += ' '
        if isNumber(' '.join(operator_conditions[i + 1])):
            explanation += ' '.join(operator_conditions[i + 1])
        else:
            explanation += getNouns(' '.join(operator_conditions[i + 1]), ori_sql)

    explanation = NLforOperator(explanation)

    # replace back
    for dict in replace_back_list:
        explanation = explanation.replace(dict['new'], dict['ori'])

    return explanation


# return the explanation for 'FROM ... [JOIN <nouns> [ON <Condition>]]
# def explainFrom()

# no recursion for intersect, union, except
def parseSQL(sql):
    global g_dict
    global replace_dict
    global db_dict
    global g_schema # the database schema where the current SQL come from

    all_possible_keywords = ['between', '!=', '>=', '<=', '<', '>', '=', ',', 'select', 'from', 'as', 'join', 'on',
                             'where', 'and', 'or', 'group by', 'having', 'order by', 'asc', 'desc', 'avg', 'count',
                             'max', 'min', 'distinct']
    core_keywords = ['select', 'from', 'as', 'join', 'on', 'where', 'group by', 'having', 'order by', 'asc', 'desc']

    # sql = sql.upper() # parser won't make the keyword upper in some cases. I need to make this for later checking
    p = Parser(sql)  # get the parser
    parsed = sqlparse.parse(sql)

    # list for subexpression
    sub_expression = []
    temp_subexpression = ''

    # list for natural language description
    explanations = []
    temp_explanation = ''

    '''
        get the subexpression and explanation for SELECT ... FROM ...(JOIN ... (AS) .... (ON) .... )
    '''

    # check if the first token is "SELECT"
    if p.tokens[0].value.lower() != 'select':
        print("exception: the first word is not SELECT, it is " + p.tokens[0].value)
        return sub_expression, explanations

    # error check: there must exists FROM in sql
    if ' from ' not in sql.lower():
        print("exception: no FROM")
        return sub_expression, explanations

    temp_tokens = []
    temp_flag = 0  # denote if "FROM" or "JOIN" is encountered
    range_idx = 0
    # match the substring before the next keyword of "FROM"
    if ' JOIN ' not in sql and ' join ' not in sql:
        for tok in p.tokens:
            if tok.value == "FROM" or tok.value == "from":
                temp_flag = 1
            if temp_flag == 1 and tok.value != 'FROM' and tok.value != 'from' and tok.value.lower() in core_keywords:
                break
            temp_tokens.append(tok.value)
    # match the substring before the next keyword of "FROM"
    # 'JOIN' in sql
    else:
        # first, find the last index of 'AS' or 'ON'
        for i in range(0, len(p.tokens)):
            if p.tokens[i].value.lower() == 'join' or p.tokens[i].value.lower() == 'as' \
                    or p.tokens[i].value.lower() == 'on':
                range_idx = i

        # second, find the index of next key word
        temp_flag = False # used to indicate if the next keyword is met
        for i in range(range_idx + 1, len(p.tokens)):
            if p.tokens[i].value.lower() in core_keywords:
                temp_flag = True
                range_idx = i  # the range is from 0 to the position before next keyword
                break
        # if next keyword hasn't been met, it means it mets EOF,
        if not temp_flag:
            range_idx = len(p.tokens)

        # get the subexpression
        for _ in range(0, range_idx):
            temp_tokens.append(p.tokens[_].value)

    temp_subexpression = ' '.join(temp_tokens)
    sub_expression.append(temp_subexpression)

    # generate NL explanation for select subexpression

    p1 = Parser(temp_subexpression)  # get the parser for subexpression

    # generate explanation for 'FROM ... [JOIN <nouns> [ON <Condition>]]'
    from_tb = ''  # used to store the table name of FROM
    join_tbs = []  # used to store the table name of JOIN
    for i in range(0, len(p1.tokens)):
        if p1.tokens[i].value == 'FROM' or p1.tokens[i].value == 'from':
            from_tb = p1.tokens[i].next_token.value
        elif p1.tokens[i].value == 'JOIN' or p1.tokens[i].value == 'join':
            join_tbs.append(p1.tokens[i].next_token.value)
        i += 1
    # remove repetitive elements (while keep the order unchanged)
    join_tbs_set = []
    for j in range(len(join_tbs)):
        if join_tbs[j] not in join_tbs_set:
            join_tbs_set.append(join_tbs[j])
    # generate the explanation of "from ... join ... "
    # from_exp = ' of ' + from_tb
    from_exp = ' from TABLE ' + from_tb
    for j in range(0, len(join_tbs_set)):
        if len(join_tbs_set) == 1:
            from_exp += ' and TABLE ' + join_tbs_set[j]
        else:
            if j == len(join_tbs_set)-1:
                from_exp += ', and TABLE ' + join_tbs_set[j]
            else:
                from_exp += ', TABLE ' + join_tbs_set[j]

    # randomly choose a verb
    # verb = ['Find out', 'Find', 'Get', 'Show me', 'Show', 'List', 'Give me', 'Tell me', 'search']
    verb = ['Return']

    # distinct
    # str_distinct = ' distinct ' if 'DISTINCT' in sql else ''

    nouns = []
    noun = ''

    # find all functions and the corresponding paramenters
    funcs = []
    paras = []
    temp_paras = []

    # find the starting position of parameters (after '(' )
    positions = []
    for i in range(0, len(p1.tokens)):
        if p1.tokens[i].value == '(':
            positions.append(i)

    positions1 = [i + 1 for i in positions]  # move from '(' to the index of parameter

    # get function names (right in front of '(' )
    for pos in positions:
        funcs.append(p1.tokens[pos - 1].value)

    # get parameter for this function
    for pos in positions1:
        temp_paras = []
        j = pos
        while p1.tokens[j].value != ')':
            if p1.tokens[j].value != ',':
                temp_paras.append(p1.tokens[j].value)
            j += 1
        paras.append(temp_paras)

    # generate nouns
    for i in range(0, len(funcs)):
        noun = ''
        # if i != 0:
        #     noun += ', '

        if funcs[i] == 'COUNT' or funcs[i] == 'count':
            if paras[i][0] == '*':
                noun += 'number of records'
            else:
                noun += 'number of '
        elif funcs[i] == 'MAX' or funcs[i] == 'max':
            noun += 'maximum value of '
        elif funcs[i] == 'MIN' or funcs[i] == 'min':
            noun += 'minimum value of '
        elif funcs[i] == 'AVG' or funcs[i] == 'avg':
            noun += 'average value of '
        elif funcs[i] == 'SUM' or funcs[i] == 'sum':
            noun += 'sum of '

        for j in range(0, len(paras[i])):
            if j != 0:
                noun += ' '
            # if len(paras) != 1 and j == len(paras)-1:
            #     noun += 'and'
            if paras[i][j] != '*':
                noun += paras[i][j]
            else:
                if funcs[i] != 'COUNT' and funcs[i] != 'count':
                    noun += 'all the records'

        nouns.append(noun)

    # add column not included in functions to nouns
    for i in range(0, len(p1.tokens)):
        if p1.tokens[i].value.lower() == 'from':
            break
        elif p1.tokens[i].value == '*' and p1.tokens[i].parenthesis_level == 0:
            nouns.append('all the records')
        elif p1.tokens[i].value.lower() not in all_possible_keywords and p1.tokens[i].parenthesis_level == 0 and \
                p1.tokens[i].value != ')':
            if p1.tokens[i].previous_token.value.lower() == 'distinct':
                nouns.append('distinct ' + p1.tokens[i].value)
            else:
                nouns.append(p1.tokens[i].value)

    temp_explanation = random.choice(verb)

    # add nouns to the explanation
    the_pos = ''
    if ' distinct ' not in sql.lower():
        the_pos = 'the '

    # X, Y, Z --> the X, the Y, and the Z
    for i in range(0, len(nouns)):

        if i != 0:
            if len(nouns) == 2:
                temp_explanation += ' and'
            else:
                if i == len(nouns)-1:
                    temp_explanation += ', and'
                else:
                    temp_explanation += ', '

        if 'all the records' not in nouns[i].lower():
            temp_explanation += ' ' + the_pos
        else:
            temp_explanation += ' '
        temp_explanation += nouns[i]



    # instead just delete [alias]
    temp_explanation_00 = temp_explanation.replace(',', ' , ')
    exp_token = temp_explanation_00.split()
    for i in range(0, len(exp_token)):
        tok = exp_token[i]
        if '.' in tok:
            temp_tok = tok.split('.')
            if len(temp_tok) != 2:
                print("exception: . should split it into 2 parts")
                break
            exp_token[i] = temp_tok[1] + ' of ' + temp_tok[0]

    temp_explanation = ' '.join(exp_token)

    temp_explanation += from_exp  # add explanation for ''

    # # add explanation for 'ON' only the 2 keys are not the same
    # on_flag = 0 # used to express if other explanation for 'on' has been added
    # for tok in p1.tokens:
    #     if tok.value == 'ON':
    #         key1 = tok.next_token.value
    #         if tok.next_token.next_token.value != '=':
    #             print("exception: should be = which connects on clause")
    #             break
    #         key2 = tok.next_token.next_token.next_token.value
    #         # remove '.' in the key
    #         if '.' in key1:
    #             temp_tok = key1.split('.')
    #             if len(temp_tok) != 2:
    #                 print("exception: . should split it into 2 parts for on")
    #                 break
    #             key1 = temp_tok[1]
    #         if '.' in key2:
    #             temp_tok = key2.split('.')
    #             if len(temp_tok) != 2:
    #                 print("exception: . should split it into 2 parts for on")
    #                 break
    #             key2 = temp_tok[1]
    #         # compare key1 and key2
    #         if key1 != key2:
    #             if on_flag == 0:
    #                 temp_explanation += ' when ' + key1 + ' is ' + key2
    #             else:
    #                 temp_explanation += ' and ' + key1 + ' is ' + key2
    #         on_flag = 1 # set the flag to 1 if different keys for on are encountered

    explanations.append(temp_explanation)

    '''
        get the subexpression and explanation for "WHERE ..."
    '''

    if ' WHERE ' in sql or ' where ' in sql:
        # get the subexpression for where clause
        where_clause = [token for token in parsed[0] if isinstance(token, sqlparse.sql.Where)][0]

        # get the explanation for where clause
        sub_expression.append(str(where_clause))

        # replace operators with natural language
        str_where_clause = str(where_clause)

        temp_explanation = ("Keep the records where " + naturalCondition(str_where_clause, sql))
        temp_explanation = temp_explanation.strip(' ')

        explanations.append(temp_explanation)

    '''
        get the subexpression and explanation for "GROUP BY ... (HAVING ...)"
    '''

    if 'GROUP BY' in sql or 'group by' in sql:
        temp_explanation = ''
        if 'GROUP BY' in sql:
            group_clause = getSubExpressionBeforeNextKeyword(sql, 'GROUP BY')
        else:
            group_clause = getSubExpressionBeforeNextKeyword(sql, 'group by')

        noun = getNouns(group_clause, sql)

        temp_explanation = 'Group the records based on ' + noun
        # replace '*' with "it"
        if '*' in noun:
            noun = noun.replace('*', 'it')

        temp_subexpression = group_clause

        # having
        # there may be 'OR' or 'AND' so

        if ' HAVING ' in sql or ' having ' in sql:
            p = Parser(sql)  # get the parser
            temp_flag = 0  # denote if the keyword is encountered

            have_clause = getSubExpressionBeforeNextKeyword(sql, 'HAVING')
            temp_subexpression += ' ' + have_clause
            temp_explanation += (' where ' + naturalCondition(have_clause, sql))

        explanations.append(temp_explanation)

        sub_expression.append(temp_subexpression)

    '''
        get the subexpression and explanation for "ORDER BY ... (ASC/DESC) (LIMIT (value))"
    '''
    if 'ORDER BY' in sql or 'order by' in sql:
        temp_explanation = ''
        if 'ORDER BY' in sql:
            order_clause = getSubExpressionBeforeNextKeyword(sql, 'ORDER BY')
        else:
            order_clause = getSubExpressionBeforeNextKeyword(sql, 'order by')

        noun = getNouns(order_clause, sql)
        # temp_explanation = 'Order these records based on ' + noun
        temp_explanation = 'Sort the records in '

        sorting = ' '  # used to express ASC or DESC
        limit_clause = ''
        if ' asc' in sql.lower() or ' desc' in sql.lower() or ' limit' in sql.lower():
            if ' asc' in sql.lower():
                # temp_explanation += ' and sort them in ascending order'
                temp_explanation += 'ascending order'
                sorting = ' ASC'
            elif ' desc' in sql.lower():
                # temp_explanation += ' and sort them in descending order'
                temp_explanation += 'descending order'
                sorting = ' DESC'
            # default: asc
            else:
                # temp_explanation += ' and sort them in ascending order'
                temp_explanation += 'ascending order'
                sorting = ' '
        # default: asc
        else:
            # temp_explanation += ' and sort them in ascending order'
            temp_explanation += 'ascending order'
            sorting = ' '

        temp_explanation += ' based on ' + noun

        if ' limit' in sql.lower():
            if ' LIMIT' in sql:
                limit_clause = ' ' + getSubExpressionBeforeNextKeyword(sql, 'LIMIT')
            else:
                limit_clause = ' ' + getSubExpressionBeforeNextKeyword(sql, 'limit')
            for tok in p.tokens:
                if tok.value == 'LIMIT' or tok.value == 'limit':
                    if tok.next_token.position != -1:
                        if not tok.next_token.is_integer:
                            print("exception: the limit value should be an integer")
                        limit_value = tok.next_token.value
                        if int(limit_value) == 1:
                            temp_explanation += ', and return the first record'
                        elif int(limit_value) > 1:
                            temp_explanation += ', and return the top ' + limit_value + ' records'
                    else:
                        temp_explanation += ', and return the first record'

        order_clause = order_clause + sorting + limit_clause
        sub_expression.append(order_clause)

        explanations.append(temp_explanation)

    # add \" to all quoted values


    # replace '_' with ' ' in explanations

    # postprocess explanations
    for i in range(0, len(explanations)):
        # add '\"' to explanations
        temp_tok_list = explanations[i].split()
        for k in range(len(temp_tok_list)):
            if 'a999specific999start999symbol' in temp_tok_list[k]:
                temp_tok_list[k] = '\"' + temp_tok_list[k] + '\"' # add '\"' if it contains the hypen
        # update explanation
        explanations[i] = ' '.join(temp_tok_list)

        explanations[i] = postprocess(explanations[i], sql)
        explanations[i] = explanations[i].replace('9999this0is0a0specific0hypen123654777', ' ')
        explanations[i] = explanations[i].replace('a999specific999start999symbol', '')

        # add '\"' to subexpressions
        temp_tok_list = sub_expression[i].split()
        for k in range(len(temp_tok_list)):
            if 'a999specific999start999symbol' in temp_tok_list[k]:
                temp_tok_list[k] = '\"' + temp_tok_list[k] + '\"' # add '\"' if it contains the hypen
        # update explanation
        sub_expression[i] = ' '.join(temp_tok_list)

        # replace back for decimal point: numberfloatreplacingprocessthisisjustapaddingstringhaha ---> .
        sub_expression[i] = sub_expression[i].replace('numberfloatreplacingprocessthisisjustapaddingstringhaha', '.')
        sub_expression[i] = sub_expression[i].replace('9999this0is0a0specific0hypen123654777', ' ')
        sub_expression[i] = sub_expression[i].replace('a999specific999start999symbol', '')

        for key in replace_dict.keys():
            explanations[i] = explanations[i].replace(replace_dict[key], key)
            sub_expression[i] = sub_expression[i].replace(replace_dict[key], key)
        for key in g_dict.keys():
            temp_res = '''"''' + key + '''"'''
            sub_expression[i] = sub_expression[i].replace(g_dict[key], temp_res)

        # if there exists a natural name dict (spider), use it
        # replace both column and table name
        explanations[i] = re.sub(r' *, *', ' , ', explanations[i]) # separate comma

        temp_exp_tok_list = explanations[i].split() # get explanation token list

        if natural_name_flag and g_schema:
            for tok_idx in range(len(temp_exp_tok_list)):
                # replace column names
                for idx in range(1, len(g_schema['column_names_original'])):
                    a = temp_exp_tok_list[tok_idx].lower()
                    b = g_schema['column_names_original'][idx][1].lower()

                    if temp_exp_tok_list[tok_idx].lower() == g_schema['column_names_original'][idx][1].lower():
                        temp_exp_tok_list[tok_idx] = g_schema['column_names'][idx][1]

                # replace table names
                for idx in range(len(g_schema['table_names_original'])):
                    c = temp_exp_tok_list[tok_idx].lower()
                    d = g_schema['table_names_original'][idx].lower()

                    if temp_exp_tok_list[tok_idx].lower() == g_schema['table_names_original'][idx].lower():
                        temp_exp_tok_list[tok_idx] = g_schema['table_names'][idx]


            # token list to string
            explanations[i] = ' '.join(temp_exp_tok_list)

            # lower keywords
            explanations[i] = explanations[i].replace(' FROM ', ' from ')
            explanations[i] = explanations[i].replace(' ORDER ', ' order ')
            explanations[i] = explanations[i].replace(' OR ', ' or ')
            explanations[i] = explanations[i].replace(' AND ', ' and ')
            explanations[i] = explanations[i].replace(' WHERE ', ' where ')
            explanations[i] = explanations[i].replace(' HAVING ', ' having ')


            # otherwise, just remove '_' of nouns
        else:
            explanations[i] = explanations[i].replace('_', ' ')

        explanations[i] = re.sub(r' *, *', ', ', explanations[i])  # naturalize comma

    return [sub_expression, explanations]


def SQLUnifiedProcess(sql):
    sql = sql.lower()
    sql = re.sub(r' *\( *', '(', sql)
    sql = re.sub(r' *\) *', ') ', sql)
    sql = re.sub('! +=', '!=', sql)
    sql = re.sub('> +=', '>=', sql)
    sql = re.sub('< +=', '<=', sql)
    sql = re.sub(' +', ' ', sql)  # replace multiple spaces to 1
    sql = sql.replace('\'', '\"')
    sql = sql.strip('; ')
    sql = sql.strip()
    return sql

def replaceBack_Postprocess(sql_str):
    # add '\"' to subexpressions
    temp_tok_list = sql_str.split()
    for k in range(len(temp_tok_list)):
        if 'a999specific999start999symbol' in temp_tok_list[k]:
            temp_tok_list[k] = '\"' + temp_tok_list[k] + '\"'  # add '\"' if it contains the hypen
    # update explanation
    sql_str = ' '.join(temp_tok_list)

    # replace back for decimal point: numberfloatreplacingprocessthisisjustapaddingstringhaha ---> .
    sql_str = sql_str.replace('numberfloatreplacingprocessthisisjustapaddingstringhaha', '.')
    sql_str = sql_str.replace('9999this0is0a0specific0hypen123654777', ' ')
    sql_str = sql_str.replace('a999specific999start999symbol', '')

    for key in replace_dict.keys():
        sql_str = sql_str.replace(replace_dict[key], key)
    for key in g_dict.keys():
        temp_res = '''"''' + key + '''"'''
        sql_str = sql_str.replace(g_dict[key], temp_res)

    return sql_str


def printExplanation(sub_expression, explanations):
    if len(sub_expression) != len(explanations):
        print("lengths of 2 lists should be the same!")
        return

    print("\033[33m\nStructured explanation: \033[0m")

    for i in range(0, len(sub_expression)):
        print('\033[1;30;46m' + sub_expression[i] + '\033[0m' + ': ', end='')
        # print(sub_expression[i], end=': ')
        print(explanations[i])


# g_subSQL = {}
high_level_explanation = []

# post process for re-organizing explanation (after discussion)
# using regular expression to
def reorganize_explanations():
    # directly modify the high_level_explanation
    global high_level_explanation

    new_high_level_explanation = copy.deepcopy(high_level_explanation)

    for query_idx, exp_unit in enumerate(new_high_level_explanation):
        sub_list = exp_unit['explanation'][0]
        exp_list = exp_unit['explanation'][1]

        Explanation_unit = {
            'from': {
                'sub': '',
                'exp': ''
            },
            'where': {
                'sub': '',
                'exp': ''
            },
            'group': {
                'sub': '',
                'exp': ''
            },
            'having': {
                'sub': '',
                'exp': ''
            },
            'order': {
                'sub': '',
                'exp': ''
            },
            'select': {
                'sub': '',
                'exp': ''
            },
        }

        for idx, (sub, exp) in enumerate(zip(sub_list, exp_list)):
            # temp process
            exp = exp.replace(' SELECT ', ' select ')
            exp = exp.replace(' FROM ', ' from ')
            exp = exp.replace(' TABLE ', ' table ')
            exp = exp.replace(' WHERE ', ' where ')
            exp = exp.replace(' ORDER ', ' order ')
            exp = exp.replace(' GROUP ', ' group ')
            exp = exp.replace(' BY ', ' by ')

            # separate ``select ...`` and ``from ...``
            if sub.lower().startswith('select '):
                # original pattern `` Show ... from TABLE ... ``
                temp_exp = exp.replace(' from table ', ' from TABLE ')
                temp_exp = temp_exp.replace(' FROM TABLE ', ' from TABLE ')
                temp_exp = temp_exp.replace(' FROM table ', ' from TABLE ')
                temp_sub = sub.replace(' from ', ' FROM ')

                if ' from TABLE ' in temp_exp:
                    # explanation
                    select_exp = temp_exp.split(' from TABLE ', 1)[0]
                    from_exp = 'In table ' + temp_exp.split(' from TABLE ', 1)[1]

                    # subexpression
                    select_sub = temp_sub.split(' FROM ')[0]
                    from_sub = 'FROM ' + temp_sub.split(' FROM ')[1]

                    # set
                    Explanation_unit['select']['sub'] = select_sub
                    Explanation_unit['select']['exp'] = select_exp

                    Explanation_unit['from']['sub'] = from_sub
                    Explanation_unit['from']['exp'] = from_exp

                else:
                    raise Exception('Cannot separate to from clause and select clause')

            elif sub.lower().startswith('group by '):
                if ' having ' not in sub.lower():
                    Explanation_unit['group']['exp'] = exp
                    Explanation_unit['group']['sub'] = sub
                else:
                    temp_exp = exp
                    temp_sub = sub.replace(' having ', ' HAVING ')

                    # explanation
                    group_exp = temp_exp.split(' where ', 1)[0]
                    having_exp = 'Keep the groups where ' + temp_exp.split(' where ', 1)[1]

                    # subexpression
                    group_sub = temp_sub.split(' HAVING ')[0]
                    having_sub = 'HAVING ' + temp_sub.split(' HAVING ')[1]

                    # set
                    Explanation_unit['group']['sub'] = group_sub
                    Explanation_unit['group']['exp'] = group_exp

                    Explanation_unit['having']['sub'] = having_sub
                    Explanation_unit['having']['exp'] = having_exp

            elif sub.lower().startswith('order by '):
                # if `count (*)`: sort the records ---> sort the groups
                temp_sub = sub.lower()
                temp_sub = re.sub(r' *\( *', '(', temp_sub)
                temp_sub = re.sub(r' *\) *', ') ', temp_sub)
                if 'count(*)' in temp_sub:
                    temp_exp = exp.replace('Sort the records', 'Sort the groups')
                else:
                    temp_exp = exp

                # set
                Explanation_unit['order']['sub'] = sub
                Explanation_unit['order']['exp'] = temp_exp

            elif sub.lower().startswith('where '):
                # set
                Explanation_unit['where']['sub'] = sub
                Explanation_unit['where']['exp'] = exp

        # update the explanation and subexpressions
        new_sub_list = []
        new_exp_list = []
        type_order = ['from', 'where', 'group', 'having', 'order', 'select']
        for type in type_order:
            if not Explanation_unit[type]['sub']:
                continue

            new_sub_list.append(Explanation_unit[type]['sub'])
            new_exp_list.append(Explanation_unit[type]['exp'])

        if not isinstance(high_level_explanation[query_idx]['explanation'], str):
            high_level_explanation[query_idx]['explanation'][0] = copy.deepcopy(new_sub_list)
            high_level_explanation[query_idx]['explanation'][1] = copy.deepcopy(new_exp_list)

# each 'SELECT' corresponds to 1 subquery
# decompose compound sql into atom sqls
# there 2 situations: INT or nested
def decompose(sql):
    global g_subSQL
    global high_level_explanation
    global g_dict
    global replace_dict

    original_sql = sql[:] # copy the original SQL

    sql = preprocessSQL(sql)

    p = Parser(sql)  # get the parser
    # num = len(g_subSQL) # number for this sub SQL

    # construct token list of Parser(sql)
    token_list = []
    for tok in p.tokens:
        token_list.append(tok.value)

    # construct the temp dict for high level explanation
    temp_explanation_dict = {'number': '', 'subquery': '', 'explanation': '', 'supplement': ''}

    # 1. divide by INT of parenthesis level 0
    # check the first encountered INT with parenthesis level 0
    # don't need to parse sql
    for i in range(0, len(p.tokens)):
        if p.tokens[i].parenthesis_level == 0 and (
                p.tokens[i].value.upper() == 'INTERSECT' or p.tokens[i].value.upper() == 'UNION' or p.tokens[i].value.upper() == 'EXCEPT'):
            pos = i  # divide 2 sub-queries from this token

            # left sub-sql
            sql1 = ' '.join(token_list[0:pos])
            while sql1[0] == '(' and sql1[-1] == ')':
                sql1 = sql1.strip('()')  # delete extra parenthesis
            # right sub-sql
            sql2 = ' '.join(token_list[pos + 1:])
            while sql2[0] == '(' and sql2[-1] == ')':
                sql2 = sql2.strip('()')  # delete extra parenthesis
            # recursively decompose
            res1 = decompose(sql1)  # res: the xxth query result
            res2 = decompose(sql2)

            # construct result (which query this is)
            result = num2ordinalStr(len(high_level_explanation) + 1) + ' query result'

            # construct replaced/modified subquery
            modified_subquery = res1 + ' ' + p.tokens[i].value.lower() + ' ' + res2

            # construct the explanation
            if p.tokens[i].value.upper() == 'INTERSECT':
                content = 'Keep the intersection of ' + res1 + ' and ' + res2 + '.'
            elif p.tokens[i].value.upper() == 'UNION':
                content = 'Keep the union of ' + res1 + ' and ' + res2 + '.'
            elif p.tokens[i].value.upper() == 'EXCEPT':
                content = 'Keep the records in ' + res1 + ' but not in ' + res2 + '.'

            temp_explanation_dict['number'] = result
            temp_explanation_dict['subquery'] = modified_subquery
            temp_explanation_dict['explanation'] = content

            # add it to high level explanation list
            high_level_explanation.append(temp_explanation_dict)

            return result

    # if situation 1 is not satisfied
    # 2. find ... (select ...)
    # need to parse sql
    for i in range(0, len(p.tokens)):
        if p.tokens[i].value.upper() == 'SELECT' and p.tokens[i].parenthesis_level == 1 and p.tokens[
            i].previous_token.value == '(':
            pos = i
            # get the end pos
            j = i
            temp_tok = p.tokens[i]
            while True:
                temp_tok = temp_tok.next_token
                j += 1
                if temp_tok.value == ')' and temp_tok.parenthesis_level == 0:
                    end_pos = j
                    break
                if temp_tok.next_token.position == -1:
                    print("Exception: No matched )")
                    break

            sql1 = ' '.join(token_list[pos:end_pos])
            while sql1[0] == '(' and sql1[-1] == ')':
                sql1 = sql1.strip('()')  # delete extra parenthesis
            res1 = decompose(sql1)

            # construct result (which query this is)
            result = num2ordinalStr(len(high_level_explanation) + 1) + ' query result'

            # construct replaced/modified subquery
            temp_concatenate_res_str = res1.replace(' ',
                                                    '123')  # to prevent this string is broken when parse the sql, and replace back later

            # handle with - and :
            # sql1 = re.sub(' *- *', '-', sql1)
            # sql1 = re.sub(' *: *', ':', sql1)
            # sql1 = re.sub(' *@ *', '@', sql1)

            # modified_subquery = ' '.join(token_list[0:pos - 1]) + ' ' + temp_concatenate_res_str

            # ' ( ' --> '('
            modified_subquery = sql
            modified_subquery = re.sub(r' *\( *', '(', modified_subquery)
            modified_subquery = re.sub(r' *\) *', ') ', modified_subquery)

            sql1 = re.sub(r' *\( *', '(', sql1)
            sql1 = re.sub(r' *\) *', ') ', sql1)

            temp_sql = sql
            temp_sql = re.sub(r' *\( *', '(', temp_sql)
            temp_sql = re.sub(r' *\) *', ') ', temp_sql)


            if sql1 in temp_sql:
                modified_subquery = modified_subquery.replace('('+sql1+')', '('+temp_concatenate_res_str+')')
            else:
                raise ValueError('temp_concatenate_res_str not in the original sql')

            modified_subquery = modified_subquery.replace('(', ' ( ')
            modified_subquery = modified_subquery.replace(')', ' ) ')

            # delete ’(' and ')' of modified_subquery and double check whether if the parenthesis include the subquery
            temp_str_tok_l = modified_subquery.split()
            # print(temp_str_tok_l)
            try:
                temp_index = temp_str_tok_l.index(temp_concatenate_res_str)
            except Exception as e:
                pass

            # check if the left of subquery is '(' and the right of subquery is ')'
            if temp_str_tok_l[temp_index - 1] == '(' and temp_str_tok_l[temp_index + 1] == ')':
                # delete the corresponding '(' and ')'
                del temp_str_tok_l[temp_index + 1]
                del temp_str_tok_l[temp_index - 1]
            else:
                raise Exception("Exception: the left of subquery should be ( and the right of subquery should be )")

            # get the modified_subquery without '(', ')'
            modified_subquery_new = ' '.join(temp_str_tok_l)

            # parse the sql and get the structured explanantion
            content = parseSQL(modified_subquery_new)

            # add '()' to the xxth query result
            # res11 = '( ' + res1 + ' ) '

            # do some hack
            another_query_str = 'anotherQueryResult'

            # find the corresponding string in structured explanation, and replace back
            # the123first123query123result ---> the first query
            temp_operators = ['>', '<', '=', '!=']

            # if data_flag:
            #     for i in range(0, len(content[0])):
            #         content[0][i] = re.sub('! +=', '!=', content[0][i])
            #         content[0][i] = re.sub('> +=', '>=', content[0][i])
            #         content[0][i] = re.sub('< +=', '<=', content[0][i])
            #         temp_sql_tok_list = content[0][i].split()
            #         for idx in range(len(temp_sql_tok_list)):
            #             if temp_sql_tok_list[idx].lower() == temp_concatenate_res_str.lower():
            #                 if temp_sql_tok_list[idx - 1].lower() == 'in' and temp_sql_tok_list[
            #                     idx - 2].lower() != 'not':
            #                     temp_sql_tok_list[idx] = '"anotherQueryResultIN"'
            #                     temp_sql_tok_list[idx - 1] = '='
            #                 elif temp_sql_tok_list[idx - 1].lower() == 'in' and temp_sql_tok_list[
            #                     idx - 2].lower() == 'not':
            #                     temp_sql_tok_list[idx] = '"anotherQueryResultNotIN"'
            #                     temp_sql_tok_list[idx - 1] = '='
            #                     temp_sql_tok_list[idx - 2] = ''
            #                 elif temp_sql_tok_list[idx - 1].lower() == '=':
            #                     temp_sql_tok_list[idx] = '"anotherQueryResultEqual"'
            #                 elif temp_sql_tok_list[idx - 1].lower() == '!=':
            #                     temp_sql_tok_list[idx] = '"anotherQueryResultNotEqual"'
            #                     temp_sql_tok_list[idx - 1] = '='
            #                 elif temp_sql_tok_list[idx - 1].lower() == '>':
            #                     temp_sql_tok_list[idx] = '"anotherQueryResultGT"'
            #                     temp_sql_tok_list[idx - 1] = '='
            #                 elif temp_sql_tok_list[idx - 1].lower() == '>=':
            #                     temp_sql_tok_list[idx] = '"anotherQueryResultGTEqual"'
            #                     temp_sql_tok_list[idx - 1] = '='
            #                 elif temp_sql_tok_list[idx - 1].lower() == '<':
            #                     temp_sql_tok_list[idx] = '"anotherQueryResultLT"'
            #                     temp_sql_tok_list[idx - 1] = '='
            #                 elif temp_sql_tok_list[idx - 1].lower() == '<=':
            #                     temp_sql_tok_list[idx] = '"anotherQueryResultLTEqual"'
            #                     temp_sql_tok_list[idx - 1] = '='
            #
            #         content[0][i] = ' '.join(temp_sql_tok_list)

            for i in range(0, len(content[0])):
                # content[0][i] = content[0][i].replace(temp_concatenate_res_str, '( ' + res1 + ' )')
                if data_flag:
                    content[0][i] = content[0][i].replace(temp_concatenate_res_str, '(1)') # for generating data
                else:
                    sql1_1 = sql1.replace('a999specific999start999symbol', '')
                    sql1_1 = sql1_1.replace('9999this0is0a0specific0hypen123654777', '')

                    content[0][i] = content[0][i].replace(temp_concatenate_res_str, '( ' + sql1_1 + ')')  # for explanation

            for i in range(0, len(content[1])):
                content[1][i] = content[1][i].replace(temp_concatenate_res_str, res1)

            temp_explanation_dict['number'] = result
            temp_explanation_dict['subquery'] = modified_subquery
            temp_explanation_dict['explanation'] = content
            temp_explanation_dict['supplement'] = result + ' uses ' + res1  # add supplement explanation

            # add it to high level explanation list
            high_level_explanation.append(temp_explanation_dict)

            return result

    # otherwise, there should no further high level
    # directly regard it as atomic sql
    # double check:
    temp_cnt = sql.count('SELECT') + sql.count('select')
    if temp_cnt != 1:
        print("Exception: there should only 1 SELECT in the atomic query sentence!")
        return

    # construct result (which query this is)
    result = num2ordinalStr(len(high_level_explanation) + 1) + ' query result'
    # parse the sql and get the structured explanantion
    content = parseSQL(sql)

    # recover SQL
    temp_tok_list = sql.split()
    for k in range(len(temp_tok_list)):
        if 'a999specific999start999symbol' in temp_tok_list[k]:
            temp_tok_list[k] = '\"' + temp_tok_list[k] + '\"'
            temp_tok_list[k] = temp_tok_list[k].replace('a999specific999start999symbol', '')
            temp_tok_list[k] = temp_tok_list[k].replace('9999this0is0a0specific0hypen123654777', ' ')
            for key in replace_dict.keys():
                temp_tok_list[k] = temp_tok_list[k].replace(replace_dict[key], key)

    sql = ' '.join(temp_tok_list)

    temp_explanation_dict['number'] = result
    temp_explanation_dict['subquery'] = sql  # doesn't change
    temp_explanation_dict['explanation'] = content

    # add it to high level explanation list
    high_level_explanation.append(temp_explanation_dict)

    return result



# decompose once and return the result
def oneTimeDecompose(sql):
    # initialize high level structure
    global high_level_explanation
    global g_dict
    high_level_explanation = []
    preprocessSQL(sql)
    decompose(sql)
    reorganize_explanations()
    print(high_level_explanation)
    result = high_level_explanation.copy()
    g_dict = {}
    return result


# global dictionary for content inside '' or ""
g_dict = {}

# specific codes for replacement
replace_dict = {
    '-': 'this000is333a999specific888minus777keyword',
    ':': 'this0909is111a222specific999column666symbol',
    '@': 'this444is555an333at888symbol333',
    '%': 'this999is999a999specific444percentage111symbol',
    'and': 'X123X',
    'and': 'x123x',
}

# table.json of spider
natural_name_flag = True # used to indicate if use natural name from table.json
db_dict = {}
if natural_name_flag:
    if os.getcwd() == '/home/yuan/Desktop/Projects/Structured_Explanation/spider_evaluation':
        with open('../backend/SQL2NL/dataset/spider/tables.json', 'r') as f:
            db_dict = json.load(f)
    elif os.getcwd() == '/home/yuan/Desktop/Projects/STEPS/spider_evaluation':
        with open('../dataset/original/spider/tables.json', 'r') as f:
            db_dict = json.load(f)
    elif os.getcwd() == '/home/yuan/Desktop/Projects/STEPS/backend/SQL2NL':
        with open('../../dataset/original/spider/tables.json', 'r') as f:
            db_dict = json.load(f)
    else:
        with open('dataset/original/spider/tables.json', 'r') as f:
            db_dict = json.load(f)


g_schema = {} # the schema of current SQL

def preprocessSQL(sql):
    # save all values in '' or "" into a global list
    # assume it can't be nested
    global g_dict
    global replace_dict
    # separate ' and "
    # sql = sql.replace('\'', ' \' ')
    # sql = sql.replace('\"', ' \" ')

    # ' ---> "
    sql = sql.replace('\'', '\"')

    sql = re.sub(' +', ' ', sql)  # replace multiple spaces to 1

    # "  france  " ---> "france"
    quotes = re.findall(r'" *.*? *"', sql)

    for qt in quotes:
        new_qt = qt.strip('"')
        new_qt = new_qt.strip()
        new_qt = '"' + new_qt + '"'
        sql = sql.replace(qt, new_qt)

    # temporarily replace space in quotes with a specific hyphen, and start symbol
    # start symbol: a999specific999start999symbol
    # hypen: 9999this0is0a0specific0hypen123654777
    # "hello world" --> "a999specific999start999symbolhello9999this0is0a0specific0hypen123654777world"
    quotes = re.findall(r'" *.*? *"', sql)

    for qt in quotes:
        new_qt = qt.strip('"')
        replaced_new_qt = 'a999specific999start999symbol' + new_qt
        replaced_new_qt = replaced_new_qt.replace(' ', '9999this0is0a0specific0hypen123654777')
        # replace symbols in the replace_dict
        for key in replace_dict.keys():
            replaced_new_qt = replaced_new_qt.replace(key, replace_dict[key])
        replaced_new_qt = '\"' + replaced_new_qt + '\"'
        sql = sql.replace(qt, replaced_new_qt)


    # replace "" ' to temporary token, and replace back at the end (for parsing SQL)
    # sql = sql.replace('\'', ' quote123456quote ')
    # sql = sql.replace('\"', ' quote123456quote ')

    # get the dictionary
    temp_token_list = sql.split()  # to tokens
    # '
    i = 0
    while i < len(temp_token_list):
        if temp_token_list[i] == '\'':
            j = i + 1
            if j >= len(temp_token_list):
                print('exception: no matched here')
                break
            temp_str = ''
            while temp_token_list[j] != '\'':
                if i == j + 1:
                    temp_str += temp_token_list[j]
                else:
                    temp_str += ' ' + temp_token_list[j]
                j += 1
                if j >= len(temp_token_list):
                    print('exception: no matched here')
                    break
            temp_str = temp_str.strip(' ')
            # generate a random substituted value
            if temp_str not in g_dict.keys():
                temp_key = generateRandomLetterString()

                while True:
                    flag = 1
                    for temp_value in g_dict.values():
                        if temp_value.count(temp_key) > 0 or temp_key.count(temp_value):
                            flag = 0
                            break
                    if flag == 1:
                        break
                    else:
                        temp_key = generateRandomLetterString()

                g_dict[temp_str] = temp_key
            # delete the tokens from i to j ([i: j+1]) including ''
            del temp_token_list[i: j + 1]
            # replace this part with the new token
            temp_token_list.insert(i, g_dict[temp_str])

        # update i
        i += 1
    # "
    i = 0
    while i < len(temp_token_list):
        if temp_token_list[i] == '\"':
            j = i + 1
            if j >= len(temp_token_list):
                print('exception: no matched here')
                break
            temp_str = ''
            while temp_token_list[j] != '\"':
                if i == j + 1:
                    temp_str += temp_token_list[j]
                else:
                    temp_str += ' ' + temp_token_list[j]
                j += 1
                if j >= len(temp_token_list):
                    print('exception: no matched here')
                    break
            temp_str = temp_str.strip(' ')
            # generate a random substituted value
            if temp_str not in g_dict.keys():
                temp_key = generateRandomLetterString()

                while True:
                    flag = 1
                    for temp_value in g_dict.values():
                        if temp_value.count(temp_key) > 0 or temp_key.count(temp_value):
                            flag = 0
                            break
                    if flag == 1:
                        break
                    else:
                        temp_key = generateRandomLetterString()

                g_dict[temp_str] = temp_key
            # delete the tokens from i to j ([i: j+1]) including ''
            del temp_token_list[i: j + 1]
            # replace this part with the new token
            temp_token_list.insert(i, g_dict[temp_str])

        # update i
        i += 1

    # replace float (e.g.  2.5 ---> 2numberfloatreplacingprocessthisisjustapaddingstringhaha5)
    for i in range(len(temp_token_list)):
        # detect float number (with decimal point)
        if isNumber(temp_token_list[i]) and temp_token_list[i].count('.') > 0:
            temp_token_list[i] = temp_token_list[i].replace('.',
                                                            'numberfloatreplacingprocessthisisjustapaddingstringhaha')

    sql = ' '.join(temp_token_list)

    # # replace with the strings in the dictionary
    # for key in g_dict.keys():
    #     sql = sql.replace(key, g_dict[key])

    # # special case
    # # 1) WHERE name = " xxx and yyy "
    # # replace and in "" with hehehehehe
    # # capitalize 'and' to 'AND'
    # sql = sql.replace(' and ', ' AND ')
    #
    # start_idx = 0 # begin for find()
    # start_idx = sql.find('AND', start_idx, len(sql))
    # while start_idx != -1:
    #     str1 = sql[0:start_idx]
    #     str2 = sql[start_idx+3:]
    #     # find the and in "" or '' (odd number = inside)
    #     if str1.count('\"') % 2 == 1 and str2.count('\"') % 2 == 1 or str1.count('\'') % 2 == 1 and str2.count('\'') % 2 == 1:
    #         sql = str1 + 'hehehehehe' + str2
    #     start_idx += 3
    #     start_idx = sql.find('AND', start_idx, len(sql))

    # in case ...
    sql = sql.strip('; ')
    while sql[0] == '(' and sql[-1] == ')':
        sql = sql.strip('()')
    sql = sql.strip('; ')

    sql = re.sub(' +', ' ', sql)  # replace multiple spaces to 1

    res = sql
    res = capitalizeKeyword(res)  # capitalize keywords

    # remove '', ""
    # there must be, because parser(sql) will automatically remove \" and \', which will lead to inconsistence
    res = res.replace('\'', '')
    res = res.replace('\"', '')

    res = re.sub(' +', ' ', res)  # replace multiple spaces to 1

    # separate '(' and ')'
    res = res.replace('(', ' ( ')
    res = res.replace(')', ' ) ')
    res = res.replace('<', ' < ')
    res = res.replace('>', ' > ')
    res = res.replace('=', ' = ')

    temp_tokens = res.split()
    res = ' '.join(temp_tokens)

    # # exceptions
    # # WHERE dept_name  =  'Comp. Sci.'
    # res = res.replace('. ', ' ')
    # res = res.strip('. ')

    # ! = ---> !=
    # < = ---> <=
    # > = ---> >=
    res = re.sub('! +=', '!=', res)
    res = re.sub('< +=', '<=', res)
    res = re.sub('> +=', '>=', res)

    res = re.sub(' +', ' ', res)  # replace multiple spaces to 1
    res = res.strip(' ')

    return res


def postprocess(explanation, ori_sql):
    global g_dict

    # substitute value with original value (especially for Name)

    # first, remove '', ""
    ori_sql = ori_sql.replace('\'', '')
    ori_sql = ori_sql.replace('\"', '')

    # tokenize explanation and sql
    tok_exp = explanation.split()
    tok_sql = ori_sql.split()

    for i in range(0, len(tok_exp)):
        for tk_sql in tok_sql:
            if tok_exp[i] == tk_sql.lower():
                tok_exp[i] = tk_sql

    # lower some special words (e.g. ' distinct ')

    result = ' '.join(tok_exp)
    result = result.replace(' DISTINCT ', ' distinct ')
    result = result.replace(' ON ', ' on ')
    result = result.replace(' BETWEEN ', ' between ')
    result = result.replace(' IN ', ' in ')
    result = result.replace(' NOT ', ' not ')
    result = result.replace(' OF ', ' of ')
    result = result.replace(' THAT ', ' that ')
    result = result.replace(' HAS ', ' has ')

    # replace back for contents in '' or ""
    # result = result.replace(' hehehehehe ', ' and ')
    for key in g_dict.keys():
        temp_res = '''"''' + key + '''"'''
        result = result.replace(g_dict[key], temp_res)

    # replace back numberfloatreplacingprocessthisisjustapaddingstringhaha ---> .
    result = result.replace('numberfloatreplacingprocessthisisjustapaddingstringhaha', '.')

    result = re.sub(r' *, *', ', ', result)  # naturalize comma

    return result


# this function will replace all aliases in SQL with the original table names
# depracated, please use removeAlias()
# def replaceAlias(sql):
#     # format assumption: table AS alias
#     # find all 'AS' in the SQL
#     res = sql
#
#     # construct the parser
#     parser = Parser(sql)
#
#     dict = parser.tables_aliases
#
#     # replace all aliases in the original SQL
#     for key in dict:
#         temp_alias = key + '.'
#         temp_table = dict[key] + '.'
#         res = res.replace(temp_alias, temp_table)
#
#     # remove as
#
#     return res


def sql2nl(sql):
    global high_level_explanation
    global g_p

    high_level_explanation = []

    sql = preprocessSQL(sql)
    parsed = sqlparse.parse(sql)
    # global g_p
    g_p = Parser(sql)  # get the parser
    # construct high level explanation
    decompose(sql)
    reorganize_explanations()

    # update subqueries in the datastructure (make sure there is no the123first123query ....)
    for idx, unit in enumerate(high_level_explanation):
        temp_subquery_str = explanation2subexpression.simpleCompose(unit['explanation'][0], '')
        high_level_explanation[idx]['subquery'] = temp_subquery_str


    for i in range(0, len(high_level_explanation)):
        print('\nStart the ' + high_level_explanation[i]['number'].replace('query result', 'query') + ',')

        # supplementary explanation
        if len(high_level_explanation[i]['supplement']) > 0:
            print(high_level_explanation[i]['supplement'])

        # judge if it has structured explanation or not
        if isinstance(high_level_explanation[i]['explanation'], str):
            print('\n' + high_level_explanation[i]['explanation'])
        else:
            # output the structured explanation
            sub = high_level_explanation[i]['explanation'][0]
            exp = high_level_explanation[i]['explanation'][1]
            printExplanation(sub, exp)

    result = high_level_explanation[:]

    # the first query result -> start the first query,
    for i in range(len(result)):
        result[i]['number'] = 'Start ' + result[i]['number'] + ','
        result[i]['number'] = result[i]['number'].replace('query result', 'query')

        # modify pure explanation for 'xxx INT yyy'
        if isinstance(result[i]['explanation'], str):
            temp1 = [[result[i]['subquery']], [result[i]['explanation']]]
            result[i]['explanation'] = temp1

        # explanation[[subexpression], [explanation]] --->explanation[{subexpression, explanation}, ...]
        newList = []

        for j in range(len(result[i]['explanation'][0])):
            temp = {
                'subexpression': result[i]['explanation'][0][j],
                'explanation': result[i]['explanation'][1][j]
            }

            newList.append(temp)

        result[i]['explanation'] = newList

    return result


# replace all aliases with the original table names
def removeAlias(sql):
    # sql0 = sql
    sql = preprocessSQL(sql)
    sql = sql.lower()
    # remove space around '.'
    sql = re.sub(' *\. *', '.', sql)

    g_p = Parser(sql)  # get the parser

    # delete AS XXX
    temp_dict = g_p.tables_aliases

    # replace all aliases in the original SQL
    for key in temp_dict:
        temp_alias = key + '.'
        temp_table = temp_dict[key] + '.'
        sql = sql.replace(temp_alias, temp_table)

    # delete all 'AS xxx'
    temp_tokens = sql.split()
    idx = 0
    while True:
        if temp_tokens[idx].lower() == 'as' and idx != len(temp_tokens) - 1:
            del temp_tokens[idx + 1]
            del temp_tokens[idx]
            idx = -1
        idx += 1
        if idx == len(temp_tokens):
            break

    res = ' '.join(temp_tokens)

    res = replaceBack_Postprocess(res)

    return res


# remove (select ... from ... join ... as)
def removeSelectClause(sql, keyword):
    sql = capitalizeKeyword(sql)
    keyword = keyword.upper()  # capitalizeKeyword the keyword

    p = Parser(sql)  # get the parser
    temp_flag = 0  # denote if the keyword is encountered
    temp_flag2 = 0  # denote this (select ... from ... join ... as )subexpression has ended
    sub_expression = ''
    temp_tokens = []

    if keyword in sql:
        for tok in p.tokens:
            if temp_flag2 == 1:
                if tok.value == 'select' or tok.value == 'SELECT':
                    temp_flag2 = 0
                    continue
                else:
                    temp_tokens.append(tok.value)
                    continue
            if tok.value == keyword:
                temp_flag = 1
            if temp_flag == 1 and tok.value != keyword and tok.is_keyword and tok.value.upper() != 'HAVING' and tok.value.upper() != 'AND' and tok.value.upper() != 'OR' and tok.value.upper() != 'COUNT' and tok.value.upper() != 'AVG' and tok.value.upper() != 'SUM' and tok.value.upper() != 'MIN' and tok.value.upper() != 'MAX' and tok.value.upper() != 'BETWEEN' and tok.value.upper() != 'between' and tok.value.upper() != 'DISTINCT' and tok.value.upper() != 'FROM' and tok.value != 'from' and tok.value != 'AS' and tok.value != 'as' and tok.value != 'JOIN' and tok.value != 'join':
                temp_tokens.append(tok.value)
                temp_flag2 = 1

        sub_expression = ' '.join(temp_tokens)

    return sub_expression


# since SmBop will for sure add select .. from .. (join ... (as)), remove them in this function
def remove_select_from_for_structured_explanation(sql):
    temp_sql = sql
    if not temp_sql.startswith('select'):
        return sql

    global g_subSQL
    global high_level_explanation

    sql = sql.lower()  # lower it
    # judge if there is only select
    if 'where ' not in sql and 'order by ' not in sql and 'group by ' not in sql:
        return sql

    exception_num = 0
    # decompose this sql
    high_level_explanation = []

    sql = capitalizeKeyword(sql)
    decompose(sql)

    result = sql

    # remove all select ... from ...
    result = removeSelectClause(result, 'SELECT')

    # replace IEU
    result = result.lower()
    result = result.replace('intersect', 'and')
    result = result.replace('union', 'or')

    # delete extra keyword
    token_list = result.split()
    keyword = token_list[0] + ' '
    result = result.replace(keyword, '')
    result = keyword + result

    # exception checking
    for idx in range(len(high_level_explanation)):
        if not isinstance(high_level_explanation[idx]['explanation'], str):
            sub_list = high_level_explanation[idx]['explanation'][0]
            if not (len(sub_list) == 2 and 'select' in sub_list[0].lower()):
                exception_num += 1
                raise Exception("Unexpected predicted situation", high_level_explanation[idx]['explanation'][0])

    return result


# return [{dbid, explanation, subexpression}]
def getStructuredData(path):
    global g_subSQL
    global high_level_explanation
    result = []

    # get test data from json
    examples = json.load(open(path))

    for i in range(0, len(examples)):
        # test position
        print("\n##### " + str(i) + " ######")
        # output input SQL with NL question
        # print("\nQuestion:")
        # print(examples[i]['question'])
        # print("SQL:")
        # print(examples[i]['query'])

        # parse SQL part
        high_level_explanation = []
        sql = examples[i]['query']
        sql = capitalizeKeyword(sql)
        sql = removeAlias(sql)

        # g_p = Parser(sql)  # get the parser
        # construct high level explanation
        decompose(sql)
        for j in range(0, len(high_level_explanation)):
            # print('\nStart the ' + high_level_explanation[i]['number'].replace('query result', 'query') + ',')
            # # supplementary explanation
            # if len(high_level_explanation[i]['supplement']) > 0:
            #     print(high_level_explanation[i]['supplement'])

            # judge if it has structured explanation or not
            if isinstance(high_level_explanation[j]['explanation'], str):
                pass
                # print('\n' + high_level_explanation[j]['explanation'])
            else:
                # output the structured explanation
                sub = high_level_explanation[j]['explanation'][0]
                exp = high_level_explanation[j]['explanation'][1]

                for k in range(len(sub)):
                    temp_dict = {
                        "dbid": examples[i]['db_id'],
                        "explanation": exp[k],
                        "subexpression": sub[k],
                    }

                    result.append(temp_dict)

    return result


# remove table names in the sql:  xxx.Y ---> Y
def removeTables(sql):
    # separate ( )
    sql = sql.replace('(', ' ( ')
    sql = sql.replace(')', ' ) ')

    # remove space around '.'
    sql = re.sub(r' *\. *', '.', sql)

    tokens = sql.split()

    # remove tables
    for i in range(len(tokens)):
        if '.' in tokens[i]:
            tokens[i] = tokens[i].split('.')[1]

    result = ' '.join(tokens)

    result = re.sub(r' *\( *', '(', result)
    result = re.sub(r' *\) *', ') ', result)


    return result


# add quotes to names
def addQuotes(sql):

    # list keywords that can be after a quoted name
    keywords = ['select', 'where', 'group', 'order', 'and', 'or']

    # '! =' ---> '!=' ..
    sql = re.sub('! +=', '!=', sql)
    sql = re.sub('> +=', '>=', sql)
    sql = re.sub('< +=', '<=', sql)

    on_flag = False # determine on (do not add quotes to on xxx = xxx)

    # the quotes can only apear after '='
    tok_list = sql.split()
    for i in range(len(tok_list)):
        # the quotes can only apear after '='
        if i != len(tok_list)-1:
            if tok_list[i].lower() == 'on':
                on_flag = True
            if tok_list[i] == '=' or tok_list[i] == '!=':
                # determine if the next token is number
                if not isNumber(tok_list[i+1]) and not on_flag:
                    # determine if there was quote
                    if not tok_list[i+1].startswith('\"') and not tok_list[i+1].startswith('\''):
                        tok_list[i + 1] = '\"' + tok_list[i+1] # add the first quote
                        # looking for the end symbol and add the second quote
                        j = i + 1
                        while True:
                            j += 1
                            # the last token
                            if j >= len(tok_list):
                                j = j - 1
                                break
                            # next one is a new subexpression
                            if tok_list[j].lower() in keywords:
                                j = j - 1
                                break

                        # current j refers to the last token needs a quote
                        if not tok_list[j].endswith('\"') and not tok_list[j].endswith('\''):
                            tok_list[j] = tok_list[j] + '\"'  # add the first quote

                if on_flag:
                    on_flag = False


    res = ' '.join(tok_list)
    return res



# compare the predicted subexpression and gold subexpression are in the same category
# if the first word are the same and there is only 1 keyword, return 1; otherwise, return 0
# sql2 is the predicted subexpression
# correct: return 0; normally incorrect: 1; except situation: 2
def categoryMatch(gold_sql, pred_sql):
    tokL1 = gold_sql.split()
    tokL2 = pred_sql.split()

    temp_str = pred_sql.lower()

    if temp_str.count('select ') > 1 and temp_str.count('except ') > 0:
        return -1

    # the first word are the same
    if tokL1[0].lower() != tokL2[0].lower():
        return 0

    # there is only 1 keyword

    keyword_cnt = 0
    keyword_cnt += temp_str.count('where')
    keyword_cnt += temp_str.count('group')
    keyword_cnt += temp_str.count('order')
    keyword_cnt += temp_str.count('select')

    if keyword_cnt != 1:
        print('there should be only 1 keyword!!!  ', keyword_cnt)
        print('\ngold sql: ', gold_sql)
        print('predicted sql: ', pred_sql)
        return 0

    return 1


# return true only if 2 sql are absolutely the same
def exactMatch(gold_sql, pred_sql):
    tokL1 = gold_sql.split()
    tokL2 = pred_sql.split()

    # the first word are not same, the score is 0
    if tokL1[0].lower() != tokL2[0].lower():
        return 0

    # X.Y ---> Y
    for i in range(len(tokL1)):
        tokL1[i] = tokL1[i].lower()
        tok = tokL1[i]
        if tok.count('.') > 0:
            temp_toks = tok.split('.')
            tokL1[i] = temp_toks[1]

    for i in range(len(tokL2)):
        tokL2[i] = tokL2[i].lower()
        tok = tokL2[i]
        if tok.count('.') > 0:
            temp_toks = tok.split('.')
            tokL2[i] = temp_toks[1]

    str1 = ' '.join(tokL1)
    str2 = ' '.join(tokL2)

    # return false if the lengths are not equal
    if len(str1) != len(str2):
        return false

    # considering the order may be different, for each predicted token, check if it exists in the other token list
    for tok in tokL2:
        if not tok in tokL1:
            return False

    return True


# sequential matching
def sequentialMatch(gold_sql, pred_sql):
    gold_sql = gold_sql.lower()
    pred_sql = pred_sql.lower()

    tokL1 = gold_sql.split()
    tokL2 = pred_sql.split()

    # the first word are not same, the score is 0
    if tokL1[0].lower() != tokL2[0].lower():
        return 0

    # X.Y ---> Y
    for i in range(len(tokL1)):
        tokL1[i] = tokL1[i].lower()
        tok = tokL1[i]
        if tok.count('.') > 0:
            temp_toks = tok.split('.')
            tokL1[i] = temp_toks[1]

    for i in range(len(tokL2)):
        tokL2[i] = tokL2[i].lower()
        tok = tokL2[i]
        if tok.count('.') > 0:
            temp_toks = tok.split('.')
            tokL2[i] = temp_toks[1]

    gold_sql = ' '.join(tokL1)
    pred_sql = ' '.join(tokL2)

    gold_sql = gold_sql.replace(' (', '(')
    gold_sql = gold_sql.replace('( ', '(')
    gold_sql = gold_sql.replace(' )', ')')

    pred_sql = pred_sql.replace(' (', '(')
    pred_sql = pred_sql.replace('( ', '(')
    pred_sql = pred_sql.replace(' )', ')')

    tokL1 = gold_sql.split()
    tokL2 = pred_sql.split()

    # for each sequential relationship, if it exists in gold relationship, +1
    match_num = 0
    for i in range(len(tokL2) - 1):
        relation = [tokL2[i], tokL2[i + 1]]
        # check if this relationship exists in the original sequence
        flag = 0
        for j in range(len(tokL1) - 1):
            if flag == 1:
                break
            if tokL1[j] == relation[0]:
                # check the remaining tokens
                for k in range(j + 1, len(tokL1)):
                    if tokL1[k] == relation[1]:
                        match_num += 1
                        flag = 1
                        break

    return match_num / (len(tokL2) - 1)

# a function split sql by 'IEU'
# return [{subquery, concatenate}], concatenate of the 1st subquery should be ''
def splitByIEU(sql):

    sql = re.sub(r' *\( *', ' ( ', sql)
    sql = re.sub(r' *\) *', ' ) ', sql)

    token_list = sql.split()
    split_idxs = [] # a list used to store

    # prevent split in a bracket
    backet_cnt = 0

    # generate separate idx
    for idx, tok in enumerate(token_list):
        if '(' in tok:
            backet_cnt += 1
        elif ')' in tok:
            backet_cnt -= 1

        if backet_cnt == 0:
            if tok.lower() == 'intersect' or tok.lower() == 'union' or tok.lower() == 'except':
                split_idxs.append(idx)

    result = []
    # add the 1st subquery
    if len(split_idxs) == 0:  # for no IEU
        temp = {'subquery': sql, 'concatenate': ''}
    else:
        temp_sql = ' '.join(token_list[:split_idxs[0]])
        temp = {'subquery': temp_sql, 'concatenate': ''}

    result.append(temp)


    # add the remaining subqueries (if there is)
    for i in range(len(split_idxs)):
        if i == len(split_idxs) - 1: # the last separator
            temp_sql = ' '.join(token_list[split_idxs[i]+1:])
        else:
            temp_sql = ' '.join(token_list[split_idxs[i]+1:split_idxs[i+1]])

        temp = {'subquery': temp_sql, 'concatenate': token_list[split_idxs[i]]}
        result.append(temp)

    return result



# return a number from 0-1 indicating the matching accuracy
# if the category is incorrect or the model predicts multiple, the score is 0
# old version
def matchScore_old(gold_sql, pred_sql):
    if pred_sql.lower() == 'select avg ( singer.age ) , min ( singer.age ) , max ( singer.age ) from singer':
        print('here')

    if gold_sql == '' and pred_sql == '':
        return 1

    if pred_sql == '':
        return 0

    if gold_sql == '':
        return 0

    # preprocess
    gold_sql = gold_sql.lower()
    pred_sql = pred_sql.lower()

    # ' ( ' --> '('
    gold_sql = re.sub(r' *\( *', '(', gold_sql)
    gold_sql = re.sub(r' *\) *', ') ', gold_sql)

    pred_sql = re.sub(r' *\( *', '(', pred_sql)
    pred_sql = re.sub(r' *\) *', ') ', pred_sql)

    # merge a single operators
    gold_sql = re.sub('> +=', '>=', gold_sql)
    gold_sql = re.sub('< +=', '<=', gold_sql)
    gold_sql = re.sub('! +=', '!=', gold_sql)

    pred_sql = re.sub('> +=', '>=', pred_sql)
    pred_sql = re.sub('< +=', '<=', pred_sql)
    pred_sql = re.sub('! +=', '!=', pred_sql)

    # separate these operators
    operators = ['>', '<', '>=', '<=', '=', '!=']
    for op in operators:
        temp_op = ' ' + op + ' '
        gold_sql = gold_sql.replace(op, temp_op)
        pred_sql = pred_sql.replace(op, temp_op)

    # X.Y ---> Y
    gold_sql = removeTables(gold_sql)
    pred_sql = removeTables(pred_sql)

    tokL1 = gold_sql.split()
    tokL2 = pred_sql.split()

    # the first word are not same, the score is 0
    if tokL1[0].lower() != tokL2[0].lower():
        # return [0,1]
        return 0

    # # X.Y ---> Y
    # for i in range(len(tokL1)):
    #     tokL1[i] = tokL1[i].lower()
    #     tok = tokL1[i]
    #     if tok.count('.') > 0:
    #         temp_toks = tok.split('.')
    #         tokL1[i] = temp_toks[1]
    #
    # for i in range(len(tokL2)):
    #     tokL2[i] = tokL2[i].lower()
    #     tok = tokL2[i]
    #     if tok.count('.') > 0:
    #         temp_toks = tok.split('.')
    #         tokL2[i] = temp_toks[1]

    gold_sql = ' '.join(tokL1)
    pred_sql = ' '.join(tokL2)

    gold_sql = gold_sql.replace(' (', '(')
    gold_sql = gold_sql.replace('( ', '(')
    gold_sql = gold_sql.replace(' )', ')')

    pred_sql = pred_sql.replace(' (', '(')
    pred_sql = pred_sql.replace('( ', '(')
    pred_sql = pred_sql.replace(' )', ')')

    pred_sql = pred_sql.replace('\'', '')
    pred_sql = pred_sql.replace('\"', '')
    gold_sql = gold_sql.replace('\'', '')
    gold_sql = gold_sql.replace('\"', '')

    # discuss for each situation
    if tokL2[0].lower() == 'select':
        matched_num = 1
        total_num = 1  # including select
        # remove aliases of sql
        pred_sql = removeAlias(pred_sql).lower()
        gold_sql = removeAlias(gold_sql).lower()

        # handle 'DISTINCT'
        if ' distinct ' in pred_sql:
            total_num += 1
            if ' distinct ' in gold_sql:
                matched_num += 1

            # remove distinct in both sql
            pred_sql = pred_sql.replace('distinct ', '')
            gold_sql = gold_sql.replace('distinct ', '')
            # replace multiple spaces to 1
            pred_sql = re.sub(' +', ' ', pred_sql)
            gold_sql = re.sub(' +', ' ', gold_sql)

        # parse select nouns
        # get nouns before from of predicted sql
        # separated by from
        temp_tok_list = re.split('from', pred_sql)
        temp_str = temp_tok_list[0].replace('select', '').strip(' ')
        temp_var_pred = temp_tok_list[1].strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_pred = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_pred.append(ele)

        # get nouns before 'from' of gold sql
        temp_tok_list = re.split('from', gold_sql)
        temp_str = temp_tok_list[0].replace('select', '').strip(' ')
        temp_var_gold = temp_tok_list[1].strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_gold = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_gold.append(ele)

        # calculate the portion of select_nouns
        for noun in select_nouns_pred:
            total_num += 1
            if noun in select_nouns_gold:
                matched_num += 1

        # compare join (neglecting on ...)
        temp_tok_list1 = re.split(' join ', temp_var_pred)
        temp_tok_list2 = re.split(' join ', temp_var_gold)

        # the first token in the string in above lists are the table names
        table_list1 = [s.strip(' ').split()[0] for s in temp_tok_list1]
        table_list2 = [s.strip(' ').split()[0] for s in temp_tok_list2]

        # calculate the portion of join nouns
        for tok in table_list1:
            total_num += 1
            if tok in table_list2:
                matched_num += 1

    elif tokL2[0].lower() == 'where':
        matched_num = 1
        total_num = 1  # including where

        # conditions are separate by 'and', 'or'
        temp_str_pred = pred_sql.replace('where ', '').strip(' ')
        conditions_pred = re.split(' and | or ', temp_str_pred)

        # conditions are separate by 'and', 'or'
        temp_str_gold = gold_sql.replace('where ', '').strip(' ')
        conditions_gold = re.split(' and | or ', temp_str_gold)

        total_num += len(conditions_gold)

        # calculate the portion of correct conditions
        for tok in conditions_pred:
            if tok in conditions_gold:
                matched_num += 1

        # calculate conjunction
        total_num += gold_sql.count(' and ')
        total_num += gold_sql.count(' or ')

        matched_num += min(pred_sql.count(' and '), gold_sql.count(' and '))
        matched_num += min(pred_sql.count(' or '), gold_sql.count(' or '))

    elif tokL2[0].lower() == 'group':
        # group by ... having ...
        matched_num = 1
        total_num = 1  # including select
        # check having
        if 'having ' in pred_sql and 'having ' in gold_sql:

            # parse select nouns
            # get nouns before from of predicted sql
            # separated by from
            temp_tok_list = re.split('having', pred_sql)
            temp_str = temp_tok_list[0].replace('group by', '').strip(' ')
            temp_var_pred = temp_tok_list[1]
            temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
            select_nouns_pred = []
            # remove ''
            for ele in temp_tok_list:
                if ele != '':
                    select_nouns_pred.append(ele)

            # get nouns before 'from' of gold sql
            temp_tok_list = re.split('having', gold_sql)
            temp_str = temp_tok_list[0].replace('group by', '').strip(' ')
            temp_var_gold = temp_tok_list[1]
            temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
            select_nouns_gold = []
            # remove ''
            for ele in temp_tok_list:
                if ele != '':
                    select_nouns_gold.append(ele)

            # calculate the portion of select_nouns
            for noun in select_nouns_pred:
                total_num += 1
                if noun in select_nouns_gold:
                    matched_num += 1

            # compare having part
            # conditions are separate by 'and', 'or'
            temp_str_pred = temp_var_pred.replace('having ', '').strip(' ')
            conditions_pred = re.split(' and | or ', temp_str_pred)

            # conditions are separate by 'and', 'or'
            temp_str_gold = temp_var_gold.replace('having ', '').strip(' ')
            conditions_gold = re.split(' and | or ', temp_str_gold)

            total_num += len(conditions_gold)

            # calculate the portion of select_nouns
            for tok in conditions_pred:
                if tok in conditions_gold:
                    matched_num += 1

            # calculate conjunction
            total_num += gold_sql.count(' and ')
            total_num += gold_sql.count(' or ')

            matched_num += min(pred_sql.count(' and '), gold_sql.count(' and '))
            matched_num += min(pred_sql.count(' or '), gold_sql.count(' or '))

        else:
            # remove having ....
            # parse select nouns
            # get nouns before from of predicted sql
            # separated by from
            temp_tok_list = re.split('having', pred_sql)
            temp_str = temp_tok_list[0].replace('group by', '').strip(' ')
            temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
            select_nouns_pred = []
            # remove ''
            for ele in temp_tok_list:
                if ele != '':
                    select_nouns_pred.append(ele)

            # get nouns before 'from' of gold sql
            temp_tok_list = re.split('having', gold_sql)
            temp_str = temp_tok_list[0].replace('group by', '').strip(' ')
            temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
            select_nouns_gold = []
            # remove ''
            for ele in temp_tok_list:
                if ele != '':
                    select_nouns_gold.append(ele)

            # calculate the portion of select_nouns
            for noun in select_nouns_pred:
                total_num += 1
                if noun in select_nouns_gold:
                    matched_num += 1



    elif tokL2[0].lower() == 'order':
        # order by ... asc/desc limit value
        # regarding it as nouns

        matched_num = 1
        total_num = 1  # including select

        # match sorting direction
        gold_sql, pred_sql

        total_num += 1
        if ' asc' not in gold_sql and ' desc' not in gold_sql:  # the default direction is asc
            if ' desc' not in pred_sql:
                matched_num += 1
        elif ' asc' in gold_sql:
            if ' asc' in pred_sql:
                matched_num += 1
        elif ' desc' in gold_sql:
            if ' desc' in pred_sql:
                matched_num += 1

        # remove asc and desc
        pred_sql = pred_sql.replace(' asc', '').strip(' ')
        pred_sql = pred_sql.replace(' desc', '').strip(' ')
        gold_sql = gold_sql.replace(' asc', '').strip(' ')
        gold_sql = gold_sql.replace(' desc', '').strip(' ')

        # get nouns before from of predicted sql
        # separated by from

        temp_str = pred_sql.replace('order by', '').strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_pred = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_pred.append(ele)

        # get nouns before 'from' of gold sql
        temp_str = gold_sql.replace('order by', '').strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_gold = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_gold.append(ele)

        # calculate the portion of select_nouns
        for noun in select_nouns_pred:
            total_num += 1
            if noun in select_nouns_gold:
                matched_num += 1
    else:
        print("exception! no other situation")
        # return [0, 1]
        return 0

    # return [matched_num, total_num]
    return matched_num / total_num

# new version
def matchScore(gold_sql, pred_sql):
    if pred_sql.lower() == 'select avg ( singer.age ) , min ( singer.age ) , max ( singer.age ) from singer':
        print('here')

    if gold_sql == '' and pred_sql == '':
        return 1

    if pred_sql == '':
        return 0

    if gold_sql == '':
        return 0

    # preprocess
    gold_sql = gold_sql.lower()
    pred_sql = pred_sql.lower()

    # ' ( ' --> '('
    gold_sql = re.sub(r' *\( *', '(', gold_sql)
    gold_sql = re.sub(r' *\) *', ') ', gold_sql)

    pred_sql = re.sub(r' *\( *', '(', pred_sql)
    pred_sql = re.sub(r' *\) *', ') ', pred_sql)

    # merge a single operators
    gold_sql = re.sub('> +=', '>=', gold_sql)
    gold_sql = re.sub('< +=', '<=', gold_sql)
    gold_sql = re.sub('! +=', '!=', gold_sql)

    pred_sql = re.sub('> +=', '>=', pred_sql)
    pred_sql = re.sub('< +=', '<=', pred_sql)
    pred_sql = re.sub('! +=', '!=', pred_sql)

    # separate these operators
    operators = ['>', '<', '>=', '<=', '=', '!=']
    for op in operators:
        temp_op = ' ' + op + ' '
        gold_sql = gold_sql.replace(op, temp_op)
        pred_sql = pred_sql.replace(op, temp_op)

    # X.Y ---> Y
    gold_sql = removeTables(gold_sql)
    pred_sql = removeTables(pred_sql)

    tokL1 = gold_sql.split()
    tokL2 = pred_sql.split()

    # the first word are not same, the score is 0
    if tokL1[0].lower() != tokL2[0].lower():
        # return [0,1]
        return 0

    gold_sql = ' '.join(tokL1)
    pred_sql = ' '.join(tokL2)

    gold_sql = gold_sql.replace(' (', '(')
    gold_sql = gold_sql.replace('( ', '(')
    gold_sql = gold_sql.replace(' )', ')')

    pred_sql = pred_sql.replace(' (', '(')
    pred_sql = pred_sql.replace('( ', '(')
    pred_sql = pred_sql.replace(' )', ')')

    pred_sql = pred_sql.replace('\'', '')
    pred_sql = pred_sql.replace('\"', '')
    gold_sql = gold_sql.replace('\'', '')
    gold_sql = gold_sql.replace('\"', '')

    # discuss for each situation
    if tokL2[0].lower() == 'select':
        matched_num = 1
        total_num = 1  # including select
        # remove aliases of sql
        pred_sql = removeAlias(pred_sql).lower()
        gold_sql = removeAlias(gold_sql).lower()

        # handle 'DISTINCT'
        if ' distinct ' in pred_sql:
            total_num += 1
            if ' distinct ' in gold_sql:
                matched_num += 1

            # remove distinct in both sql
            pred_sql = pred_sql.replace('distinct ', '')
            gold_sql = gold_sql.replace('distinct ', '')
            # replace multiple spaces to 1
            pred_sql = re.sub(' +', ' ', pred_sql)
            gold_sql = re.sub(' +', ' ', gold_sql)

        # parse select nouns
        # get nouns before from of predicted sql
        # separated by from

        temp_str = pred_sql.replace('select ', '').strip(' ')

        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_pred = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_pred.append(ele)

        # get nouns before 'from' of gold sql
        temp_str = gold_sql.replace('select ', '').strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_gold = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_gold.append(ele)

        # calculate the portion of select_nouns
        for noun in select_nouns_pred:
            total_num += 1
            if noun in select_nouns_gold:
                matched_num += 1

    elif tokL2[0].lower() == 'from':
        matched_num = 1
        total_num = 1  # including from

        temp_var_pred = pred_sql.lower().replace('from ', '')
        temp_var_gold = gold_sql.lower().replace('from ', '')

        # compare join (neglecting on ...)
        temp_tok_list1 = re.split(' join ', temp_var_pred)
        temp_tok_list2 = re.split(' join ', temp_var_gold)

        # the first token in the string in above lists are the table names
        table_list1 = [s.strip(' ').split()[0] for s in temp_tok_list1]
        table_list2 = [s.strip(' ').split()[0] for s in temp_tok_list2]

        # calculate the portion of join nouns
        for tok in table_list1:
            total_num += 1
            if tok in table_list2:
                matched_num += 1

    elif tokL2[0].lower() == 'where':
        matched_num = 1
        total_num = 1  # including where

        # conditions are separate by 'and', 'or'
        temp_str_pred = pred_sql.replace('where ', '').strip(' ')
        conditions_pred = re.split(' and | or ', temp_str_pred)

        # conditions are separate by 'and', 'or'
        temp_str_gold = gold_sql.replace('where ', '').strip(' ')
        conditions_gold = re.split(' and | or ', temp_str_gold)

        total_num += len(conditions_gold)

        # calculate the portion of correct conditions
        for tok in conditions_pred:
            if tok in conditions_gold:
                matched_num += 1

        # calculate conjunction
        total_num += gold_sql.count(' and ')
        total_num += gold_sql.count(' or ')

        matched_num += min(pred_sql.count(' and '), gold_sql.count(' and '))
        matched_num += min(pred_sql.count(' or '), gold_sql.count(' or '))

    elif tokL2[0].lower() == 'group':
        # group by ... having ...
        matched_num = 1
        total_num = 1  # including select

        temp_str = pred_sql.replace('group by', '').strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_pred = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_pred.append(ele)

        # get nouns before 'having' of gold sql
        temp_str = gold_sql.replace('group by', '').strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before having
        select_nouns_gold = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_gold.append(ele)

        # calculate the portion of select_nouns
        for noun in select_nouns_pred:
            total_num += 1
            if noun in select_nouns_gold:
                matched_num += 1


    elif tokL2[0].lower() == 'having':
        pred_temp = pred_sql.lower().replace('having ', '')
        gold_temp = gold_sql.lower().replace('having ', '')

        matched_num = 1
        total_num = 2  # including having

        if pred_temp.lower() == gold_temp.lower():
            matched_num += 1

    elif tokL2[0].lower() == 'order':
        # order by ... asc/desc limit value
        # regarding it as nouns

        matched_num = 1
        total_num = 1  # including order

        # match sorting direction
        gold_sql, pred_sql

        total_num += 1
        if ' asc' not in gold_sql and ' desc' not in gold_sql:  # the default direction is asc
            if ' desc' not in pred_sql:
                matched_num += 1
        elif ' asc' in gold_sql:
            if ' asc' in pred_sql:
                matched_num += 1
        elif ' desc' in gold_sql:
            if ' desc' in pred_sql:
                matched_num += 1

        # remove asc and desc
        pred_sql = pred_sql.replace(' asc', '').strip(' ')
        pred_sql = pred_sql.replace(' desc', '').strip(' ')
        gold_sql = gold_sql.replace(' asc', '').strip(' ')
        gold_sql = gold_sql.replace(' desc', '').strip(' ')

        # get nouns before from of predicted sql
        # separated by from

        temp_str = pred_sql.replace('order by', '').strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_pred = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_pred.append(ele)

        # get nouns before 'from' of gold sql
        temp_str = gold_sql.replace('order by', '').strip(' ')
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_gold = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_gold.append(ele)

        # calculate the portion of select_nouns
        for noun in select_nouns_pred:
            total_num += 1
            if noun in select_nouns_gold:
                matched_num += 1
    else:
        print("exception! no other situation")
        # return [0, 1]
        return 0

    # return [matched_num, total_num]
    return matched_num / total_num

# execute sql on the target database and store the results in G_result (as a list)
def executeDB(dbname, sql):
    table_id = dbname
    path = "../SmBop/dataset/database/" + '{}/{}.sqlite'.format(table_id, table_id)
    print("\n" + path)
    conn = sqlite3.connect(path)
    conn.text_factory = str
    cursor = conn.cursor()
    # global G_result

    result = []

    try:
        cursor.execute(sql)
        results = cursor.fetchall()

        # extract information from results to result_dict
        G_result = []  # initialize it
        # get the title name (description)
        description = []
        for tup in cursor.description:
            description.append(tup[0])

        list = []  # which will be converted to json later

        for res in results:
            temp_dict = {}
            for j in range(0, len(description)):
                temp_dict[description[j]] = res[j]
            # print(temp_dict)
            list.append(temp_dict)

        # G_result = json.dumps(G_result, ensure_ascii=False, indent=2)
        # G_result = list
        result = list

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('\n' + message)
        pass

    # close the database
    conn.close()

    return result

if __name__ == "__main__":
    instruction = 'sql2nl'

    if instruction == 'temp_test':

        sql = "SELECT venue ,  name FROM event ORDER BY Event_Attendance DESC LIMIT 2"
        drop_num = 0
        # drop the data which contains IEU

        # drop nested SQL
        if sql.lower().count('select ') > 1:
            drop_num += 1

        sql = sql.strip('; ')

        # keep " and ', replace back later
        sql = sql.replace('\"', 'quote12345symbol')
        sql = sql.replace('\'', 'quote12345symbol')

        parser = Parser(sql)
        ori_list = []
        # parser to token list
        for tok in parser.tokens:
            temp_str = tok.value
            temp_str = temp_str.lower()
            temp_str = temp_str.strip("\'")

            if isNumber(temp_str):  # solve the confliction between float and .
                ori_list.append(temp_str)
            elif '.' in temp_str:
                firstPart = temp_str.split('.')[0].lower()
                table_list = parser.tables
                alias_list = parser.tables_aliases.keys()
                # lower them
                table_list = [tb.lower() for tb in table_list]
                alias_list = [als.lower() for als in alias_list]

                if firstPart in table_list or firstPart in alias_list:
                    temp_str = temp_str.replace('.', ' . ')  # [X.Y] -> [X, ., Y]

                temp_str = temp_str.replace('(', ' ( ')  # (*) -> ( * )
                temp_str = temp_str.replace(')', ' ) ')  # (*) -> ( * )
                temp_str = temp_str.replace('=', ' = ')  # != -> ! =
                temp_str_l = temp_str.split()
                for tempTok in temp_str_l:
                    ori_list.append(tempTok)
            elif '(' in temp_str or ')' in temp_str or 'order by' in temp_str or 'group by' in temp_str or '=' in temp_str or 'not in' in temp_str or 'not like' in temp_str:
                temp_str = temp_str.replace('(', ' ( ')  # (*) -> ( * )
                temp_str = temp_str.replace(')', ' ) ')  # (*) -> ( * )
                temp_str = temp_str.replace('=', ' = ')  # != -> ! =
                temp_str_l = temp_str.split()
                for tempTok in temp_str_l:
                    ori_list.append(tempTok)
            else:
                ori_list.append(temp_str)

        # build the set of value in the original sql
        set_of_value = set()

        sql = ' '.join(ori_list)
        # replace back for "
        sql = sql.replace('quote12345symbol', '\"')

        sql0 = sql
        sql = removeAlias(sql)

        if 't1' in sql.lower():
            raise Exception("there should be no other case, check here!")

        print(sql)
        sql1 = sql
        print(sql1)
        try:
            # g_dict = {}
            highLevelResult = []
            highLevelResult = oneTimeDecompose(sql1)
        except:
            drop_num += 1

        # print(highLevelResult)
        print('end')

    elif instruction == 'sql2nl':
        while True:
            sql = input("\ninput the sql > ")
            result_explanation = sql2nl(sql)
            print(result_explanation)


    elif instruction == 'shell':
        natural_name_flag = False
        while True:
            high_level_explanation = []
            g_dict = {}

            sql = input("\ninput the sql > ")
            ori_sql = sql

            sql = preprocessSQL(sql)

            parsed = sqlparse.parse(sql)

            # global g_p
            g_p = Parser(sql)  # get the parser

            # g_subSQL[0] = sql

            # construct high level explanation
            decompose(sql)
            # reorganize_explanations()

            for i in range(0, len(high_level_explanation)):
                print('\nStart the ' + high_level_explanation[i]['number'].replace('query result', 'query') + ',')

                # supplementary explanation
                if len(high_level_explanation[i]['supplement']) > 0:
                    print(high_level_explanation[i]['supplement'])

                # judge if it has structured explanation or not
                if isinstance(high_level_explanation[i]['explanation'], str):
                    print('\n' + high_level_explanation[i]['explanation'])
                else:
                    # output the structured explanation
                    sub = high_level_explanation[i]['explanation'][0]
                    exp = high_level_explanation[i]['explanation'][1]
                    printExplanation(sub, exp)

    elif instruction == 'test':
        print("Test mode ... ")

        # get raw data from json
        examples = json.load(open("dataset/spider/dev.json"))
        examples = json.load(open("dataset/spider/train_spider.json"))

        start_num = 0  # last end position
        for i in range(start_num, len(examples)):
            # test position
            print("\n##### " + str(i) + " ######")

            # get the global g_schema from dbid
            for db in db_dict:
                if examples[i]['db_id'].lower() == db['db_id'].lower():
                    g_schema = db

            print("\nDatabase ID:")
            print(examples[i]['db_id'])
            # output input SQL with NL question
            print("\nQuestion:")
            print(examples[i]['question'])
            print("SQL:")
            print(examples[i]['query'])

            g_dict = {}

            # parse SQL part
            high_level_explanation = []
            sql = examples[i]['query']
            ori_sql = sql
            sql = preprocessSQL(sql)
            sql = removeAlias(sql)
            parsed = sqlparse.parse(sql)
            g_p = Parser(sql)  # get the parser
            # construct high level explanation
            decompose(sql)
            reorganize_explanations()

            for i in range(0, len(high_level_explanation)):
                print('\nStart the ' + high_level_explanation[i]['number'].replace('query result', 'query') + ',')

                # supplementary explanation
                if len(high_level_explanation[i]['supplement']) > 0:
                    print(high_level_explanation[i]['supplement'])

                # judge if it has structured explanation or not
                if isinstance(high_level_explanation[i]['explanation'], str):
                    print('\n' + high_level_explanation[i]['explanation'])
                else:
                    # output the structured explanation
                    sub = high_level_explanation[i]['explanation'][0]
                    exp = high_level_explanation[i]['explanation'][1]
                    printExplanation(sub, exp)

        print("\nCongratulations!")

    elif instruction == 'save':

        # calcualte the distribution of data
        # 0. select from
        # 1. select from join
        # 2. where
        # 3. group by
        # 4. group by having
        # 5. order by
        # 6. order by ASC/DESC
        # 7. order by limit
        # 8. order by ASC/DESC limit

        distribution = [0, 0, 0, 0, 0, 0, 0, 0, 0]

        keword_distribution = {'SELECT': 0, 'FROM': 0, 'JOIN': 0, 'AS': 0, 'ON': 0, 'WHERE': 0, 'GROUP BY': 0,
                               'HAVING': 0, 'ORDER BY': 0, 'LIMIT': 0}

        unnamed = []  # exception: subclause not belongs to these category

        data = {}

        GT_sql = []  # ground truth sql
        questions = []  # NL questions
        db_ids = []  # database id
        example_num = []  # the number of example

        high_level_num = []
        sub_sqls = []
        structured_explanations = []
        sub_expression = []

        pure_exps = []

        # open a text file and write the training data into it (sub_sql \t explanation)
        pair_f = open("nl_sql_pair.txt", "w")

        # get raw data from json
        # examples = json.load(open("dev_reordered.json"))
        examples = json.load(open("dataset/spider/merge.json"))

        for i in range(0, len(examples)):

            print(i)
            g_dict = {}

            example_num.append(i)
            db_ids.append(examples[i]['db_id'])
            GT_sql.append(examples[i]['query'])
            questions.append(examples[i]['question'])

            high_level_num.append('')
            sub_sqls.append('')
            sub_expression.append('')
            structured_explanations.append('')
            pure_exps.append('')

            # parse SQL part
            pure_explanation = ''  # pure explanation in 1 sentence
            high_level_explanation = []
            sql = examples[i]['query']
            ori_sql = sql
            sql = preprocessSQL(sql)
            parsed = sqlparse.parse(sql)
            g_p = Parser(sql)  # get the parser
            # construct high level explanation
            decompose(sql)
            for j in range(0, len(high_level_explanation)):
                if len(high_level_explanation) > 1:
                    pure_explanation += '\nStart the ' + high_level_explanation[j]['number'].replace('query result',
                                                                                                 'query') + ',\n'  # edit pure explanation
                high_level_num.append(str(j + 1))  # number of high level sub query (for each select)
                sub_sqls.append(high_level_explanation[j]['subquery'])
                sub_expression.append('')
                structured_explanations.append('')
                example_num.append('')
                db_ids.append('')
                GT_sql.append('')
                questions.append('')
                pure_exps.append('')

                # supplementary explanation
                if len(high_level_explanation[j]['supplement']) > 0:
                    pure_explanation += high_level_explanation[j]['supplement'] + '\n'

                # judge if it has structured explanation or not
                if isinstance(high_level_explanation[j]['explanation'], str):
                    pure_explanation += high_level_explanation[j]['explanation'] + '\n'

                    high_level_num.append('')
                    sub_sqls.append('')
                    sub_expression.append('')
                    structured_explanations.append(high_level_explanation[j]['explanation'])
                    example_num.append('')
                    db_ids.append('')
                    GT_sql.append('')
                    questions.append('')
                    pure_exps.append('')

                else:
                    # output the structured explanation
                    sub = high_level_explanation[j]['explanation'][0]
                    exp = high_level_explanation[j]['explanation'][1]

                    for k in range(0, len(sub)):
                        high_level_num.append('')
                        sub_sqls.append('')
                        sub_expression.append(sub[k])
                        structured_explanations.append(exp[k])
                        example_num.append('')
                        db_ids.append('')
                        GT_sql.append('')
                        questions.append('')
                        pure_exps.append('')
                        pure_explanation += exp[k] + '. '

                        # also save sub_sql & NL pair to a text file
                        pair_f.write(exp[k])
                        pair_f.write('\t')
                        pair_f.write(sub[k])
                        pair_f.write('\n')

                        # calcualte the distribution of data
                        # 0. select from
                        # 1. select from join
                        # 2. where
                        # 3. group by
                        # 4. group by having
                        # 5. order by
                        # 6. order by ASC/DESC
                        # 7. order by limit
                        # 8. order by ASC/DESC limit

                        if 'SELECT' in sub[k] or 'select' in sub[k]:
                            if 'JOIN' in sub[k] or 'join' in sub[k]:
                                distribution[1] += 1
                            else:
                                distribution[0] += 1
                        elif 'WHERE' in sub[k] or 'where' in sub[k]:
                            distribution[2] += 1
                        elif 'GROUP' in sub[k] or 'group' in sub[k]:
                            if 'HAVING' in sub[k] or 'having' in sub[k]:
                                distribution[4] += 1
                            else:
                                distribution[3] += 1
                        elif 'ORDER' in sub[k] or 'order' in sub[k]:
                            if 'ASC' in sub[k] or 'asc' in sub[k] or 'DESC' in sub[k] or 'desc' in sub[k]:
                                if 'LIMIT' in sub[k] or 'limit' in sub[k]:
                                    distribution[8] += 1
                                else:
                                    distribution[6] += 1
                            else:
                                if 'LIMIT' in sub[k] or 'limit' in sub[k]:
                                    distribution[7] += 1
                                else:
                                    distribution[5] += 1
                        else:
                            unnamed.append(sub[k])

                        # keyword distribution
                        # overlapped_distribution = {'SELECT': 0, 'JOIN': 0, 'AS': 0, 'ON': 0, 'WHERE': 0, 'GROUP BY': 0,
                        #                            'HAVING': 0, 'ORDER BY': 0, 'LIMIT': 0}

                        keword_distribution['SELECT'] += sub[k].count('SELECT')
                        keword_distribution['FROM'] += sub[k].count('FROM')
                        keword_distribution['JOIN'] += sub[k].count('JOIN')
                        keword_distribution['AS'] += sub[k].count('AS')
                        keword_distribution['ON'] += sub[k].count('ON')
                        keword_distribution['WHERE'] += sub[k].count('WHERE')
                        keword_distribution['GROUP BY'] += sub[k].count('GROUP BY')
                        keword_distribution['HAVING'] += sub[k].count('HAVING')
                        keword_distribution['ORDER BY'] += sub[k].count('ORDER BY')
                        keword_distribution['LIMIT'] += sub[k].count('LIMIT')

            # add pure explanation
            pure_exps.append(pure_explanation)
            high_level_num.append('')
            sub_sqls.append('')
            sub_expression.append('')
            structured_explanations.append('')
            example_num.append('')
            db_ids.append('')
            GT_sql.append('')
            questions.append('')

        data['Example number'] = example_num
        data['Database id'] = db_ids
        data['Question'] = questions
        data['Ground truth SQL'] = GT_sql
        data['Sub SQL number'] = high_level_num
        data['Sub SQL'] = sub_sqls
        data['Subexpression'] = sub_expression
        data['Explanation for subexpression'] = structured_explanations
        data['Pure explanation'] = pure_exps

        df = DataFrame(data)
        df.to_excel("data.xls")

        pair_f.close()

        # output the distribution
        print('########## Distribution #############')
        print(distribution)
        # unnamed
        print('unnamed: ')
        print(unnamed)

        print(keword_distribution)


    elif instruction == 'GTdata':

        data = {}

        GT_sql = []  # ground truth sql
        questions = []  # NL questions
        db_ids = []  # database id
        example_num = []  # the number of example

        high_level_num = []
        sub_sqls = []
        structured_explanations = []
        sub_expression = []

        pure_exps = []

        # open a text file and write the training data into it (sub_sql \t explanation)
        pair_f = open("nl_sql_pair.txt", "w")

        # get raw data from json
        # examples = json.load(open("dev_reordered.json"))
        examples = json.load(open("dataset/spider/merge.json"))

        for i in range(0, len(examples)):

            print(i)
            g_dict = {}
            example_num.append(i)
            db_ids.append(examples[i]['db_id'])
            GT_sql.append(examples[i]['query'])
            questions.append(examples[i]['question'])

            high_level_num.append('')
            sub_sqls.append('')
            sub_expression.append('')
            structured_explanations.append('')
            pure_exps.append('')

            # parse SQL part
            pure_explanation = ''  # pure explanation in 1 sentence
            high_level_explanation = []
            sql = examples[i]['query']
            ori_sql = sql
            sql = preprocessSQL(sql)
            parsed = sqlparse.parse(sql)
            g_p = Parser(sql)  # get the parser

            # replace all aliases with the original table names
            # delete AS XXX
            temp_dict = g_p.tables_aliases
            # replace all aliases in the original SQL
            for key in temp_dict:
                temp_alias = key + '.'
                temp_table = temp_dict[key] + '.'
                sql = sql.replace(temp_alias, temp_table)
            # delete all 'AS xxx'
            temp_tokens = sql.split()
            idx = 0
            while True:
                if temp_tokens[idx].lower() == 'as' and idx != len(temp_tokens) - 1:
                    del temp_tokens[idx + 1]
                    del temp_tokens[idx]
                    idx = -1
                idx += 1
                if idx == len(temp_tokens):
                    break

            sql = ' '.join(temp_tokens)

            # construct high level explanation
            decompose(sql)

            for j in range(0, len(high_level_explanation)):
                if len(high_level_explanation) > 1:
                    pure_explanation += '\nStart the ' + high_level_explanation[j]['number'].replace('query result',
                                                                                                 'query') + ',\n'  # edit pure explanation
                high_level_num.append(str(j + 1))  # number of high level sub query (for each select)
                sub_sqls.append(high_level_explanation[j]['subquery'])
                sub_expression.append('')
                structured_explanations.append('')
                example_num.append('')
                db_ids.append('')
                GT_sql.append('')
                questions.append('')
                pure_exps.append('')

                # supplementary explanation
                if len(high_level_explanation[j]['supplement']) > 0:
                    pure_explanation += high_level_explanation[j]['supplement'] + '\n'

                # judge if it has structured explanation or not
                if isinstance(high_level_explanation[j]['explanation'], str):
                    pure_explanation += high_level_explanation[j]['explanation'] + '\n'

                    high_level_num.append('')
                    sub_sqls.append('')
                    sub_expression.append('')
                    structured_explanations.append(high_level_explanation[j]['explanation'])
                    example_num.append('')
                    db_ids.append('')
                    GT_sql.append('')
                    questions.append('')
                    pure_exps.append('')

                else:
                    # output the structured explanation
                    sub = high_level_explanation[j]['explanation'][0]
                    exp = high_level_explanation[j]['explanation'][1]

                    for k in range(0, len(sub)):
                        high_level_num.append('')
                        sub_sqls.append('')
                        sub_expression.append(sub[k])
                        structured_explanations.append(exp[k])
                        example_num.append('')
                        db_ids.append('')
                        GT_sql.append('')
                        questions.append('')
                        pure_exps.append('')
                        pure_explanation += exp[k] + '. '

                        # also save sub_sql & NL pair to a text file
                        pair_f.write(exp[k])
                        pair_f.write('\t')
                        pair_f.write(sub[k])
                        pair_f.write('\n')

        pair_f.close()



    # in this mode, the program will randomly generate SQLs, thereby generating infinite data
    elif instruction == 'data':

        data_num = 500  # the number of data needed to be generated
        # open the data file
        F = open("randomData.txt", "w")

        for i in range(data_num):
            print('############## ', i, ' ###############')
            sqlOB = randomAtomicSQL()
            ran_sql = sqlOB.sql_str  # get the randomly generated SQL

            print(ran_sql)

            # global g_p
            g_p = Parser(ran_sql)  # get the parser

            # get the subexpressions and explanations
            sub, exp = parseSQL(ran_sql)
            for j in range(len(sub)):
                F.write(exp[j])
                F.write('\n')
                F.write(sub[j])
                F.write('\n\n')

        F.close()

    elif instruction == 'trainingData':


        input_path = '../../dataset/original/spider/dev.json'
        output_path = '../../dataset/structured/spider/dev.json'

        input_path = '../../dataset/original/spider/train_spider.json'
        output_path = '../../dataset/structured/spider/train_spider.json'


        with open(input_path) as load_f:
            load_dict = json.load(load_f)

        # which will be stored as a json file
        json_output = []

        counter = 0
        drop_num = 0

        # for each data example, decompose it using SQL2NL
        for cnt in range(0, int(len(load_dict))):
            # for cnt in range(0, 1):

            data = load_dict[cnt]
            print(cnt)
            sql = copy.deepcopy(data['query'])

            if 'sci.' in sql.lower():
                continue

            print(data['question'])
            sql = preprocessSQL(sql)
            sql = removeAlias(sql)
            # get the global g_schema from dbid
            for db in db_dict:
                if data['db_id'].lower() == db['db_id'].lower():
                    g_schema = db


            sql = sql.strip('; ')

            # parser = Parser(sql)
            # ori_list = []
            # # parser to token list
            # for tok in parser.tokens:
            #     temp_str = tok.value
            #     temp_str = temp_str.lower()
            #     temp_str = temp_str.strip("\'")
            #
            #     if isNumber(temp_str):  # solve the confliction between float and .
            #         ori_list.append(temp_str)
            #
            #     elif '.' in temp_str:
            #         firstPart = temp_str.split('.')[0].lower()
            #         table_list = parser.tables
            #         alias_list = parser.tables_aliases.keys()
            #         # lower them
            #         table_list = [tb.lower() for tb in table_list]
            #         alias_list = [als.lower() for als in alias_list]
            #
            #         if firstPart in table_list or firstPart in alias_list:
            #             temp_str = temp_str.replace('.', ' . ')  # [X.Y] -> [X, ., Y]
            #
            #         temp_str = temp_str.replace('(', ' ( ')  # (*) -> ( * )
            #         temp_str = temp_str.replace(')', ' ) ')  # (*) -> ( * )
            #         temp_str = temp_str.replace('=', ' = ')  # != -> ! =
            #         temp_str_l = temp_str.split()
            #         for tempTok in temp_str_l:
            #             ori_list.append(tempTok)
            #     elif '(' in temp_str or ')' in temp_str or 'order by' in temp_str or 'group by' in temp_str or '=' in temp_str or 'not in' in temp_str or 'not like' in temp_str:
            #         temp_str = temp_str.replace('(', ' ( ')  # (*) -> ( * )
            #         temp_str = temp_str.replace(')', ' ) ')  # (*) -> ( * )
            #         temp_str = temp_str.replace('=', ' = ')  # != -> ! =
            #         temp_str_l = temp_str.split()
            #         for tempTok in temp_str_l:
            #             ori_list.append(tempTok)
            #     else:
            #         ori_list.append(temp_str)

            # build the set of value in the original sql
            set_of_value = set()

            # no_value_list = data['query_toks_no_value']
            # # compare query_toks and query_toks_no_value
            # if len(ori_list) != len(no_value_list):
            #     print(ori_list)
            #     print(no_value_list)
            #     # raise Exception("length are not equal, cannot compare!")
            # else:
            #     for k in range(len(ori_list)):
            #         if ori_list[k] != no_value_list[k]:
            #             if no_value_list[k] == 'value':
            #                 set_of_value.add(ori_list[k])
            #             else:
            #                 print(ori_list)
            #                 print(no_value_list)
            #                 # raise Exception("there should be no other case, check here!")
            #
            # sql = ' '.join(ori_list)


            # replace back for "
            # sql = sql.replace('quote12345symbol', '\"')

            sql0 = sql
            sql = removeAlias(sql)

            if 't1' in sql.lower():
                raise Exception("there should be no other case, check here!")

            sql1 = sql
            print(sql1)
            try:
                # g_dict = {}
                highLevelResult = []
                highLevelResult = oneTimeDecompose(sql1)
                # highLevelResult = oneTimeDecompose(data['query'])
            except:
                drop_num += 1
                continue
            for subquery in highLevelResult:
                # parse the subquery and get the parsed sql structure
                # db_id = data["db_id"]
                # schemas, db_names, tables = get_schemas_from_json('tables.json')
                # schema = schemas[db_id]
                # table = tables[db_id]
                # schema = Schema(schema, table)
                # sql_label = get_sql(schema, sql1)

                sql_label = data["sql"]
                ori_query = data["query"]
                ori_question = data["question"]
                if not isinstance(subquery['explanation'], str):
                    subexpressions = subquery['explanation'][0]
                    explanations = subquery['explanation'][1]
                    # lower subexpression again
                    subexpressions = [tok.lower() for tok in subexpressions]

                    # output the structured explanation
                    temp_sub = subquery['explanation'][0]
                    temp_exp = subquery['explanation'][1]
                    printExplanation(temp_sub, temp_exp)

                    for i in range(len(subexpressions)):
                        # construct empty parsed sql
                        parsed_sql = {
                            "from": {
                                "table_units": [],
                                "conds": []
                            },
                            "select": [],
                            "where": [],
                            "groupBy": [],
                            "having": [],
                            "orderBy": [],
                            "limit": None,
                            "intersect": None,
                            "union": None,
                            "except": None
                        }

                        # construct query_toks and query_toks_no_value
                        queryToks = subexpressions[i].split()
                        queryToksNoValue = []
                        for j in range(len(queryToks)):
                            if queryToks[j] in set_of_value:
                                queryToksNoValue.append('value')
                            else:
                                queryToksNoValue.append(queryToks[j])

                        # construct parsed sql
                        temp_subexpression = subexpressions[i].lower()
                        if 'where ' in temp_subexpression:
                            parsed_sql['where'] = sql_label['where']
                        elif 'group by ' in temp_subexpression:
                            parsed_sql['groupBy'] = sql_label['groupBy']
                            parsed_sql['having'] = sql_label['having']
                        elif 'order by ' in temp_subexpression:
                            parsed_sql['orderBy'] = sql_label['orderBy']
                            parsed_sql['limit'] = sql_label['limit']
                        elif 'select ' in temp_subexpression and 'from ' in temp_subexpression:
                            parsed_sql['select'] = sql_label['select']
                            parsed_sql['from'] = sql_label['from']

                        # construct the data dictionary
                        data_piece = {
                            "db_id": data['db_id'],
                            "query": subexpressions[i],
                            "query_toks": queryToks,
                            "query_toks_no_value": queryToksNoValue,
                            "question": explanations[i],
                            "question_toks": explanations[i].split(),
                            "sql": parsed_sql,
                            "original_sql": ori_query,
                            "original_question": ori_question,
                            "instance_id": counter,
                        }

                        counter += 1
                        json_output.append(data_piece)
            # if 'Comp. Sci.' in data['query']:
            #     break

        with open(output_path, 'w') as out_f:
            outputFile = json.dumps(json_output)
            out_f.write(outputFile)

        print('data number: ', counter)
        print('dropped data: ', drop_num)

    # test match function
    elif instruction == 'match':
        pred_sql = 'select count ( * ) , car_makers.fullname from model_list join car_makers on model_list.maker = car_makers.id'
        gold_sql = 'select count ( * ) , car_makers.fullname from car_makers join model_list on car_makers.id = model_list.maker'

        score = matchScore(gold_sql, pred_sql)

        print(score)

    elif instruction == 'execute':
        dbname = 'tvshow'
        while True:
            sql = input('\ninput the SQL > ')
            sql = addCollateNocase(sql)
            print(sql)
            res = executeDB(dbname, sql)
            print(res)

# select airline , abbreviation from airlines where country = "usa" COLLATE NOCASE
# select airline , abbreviation from airlines COLLATE NOCASE
'SELECT first_name , last_name FROM players ORDER BY birth_date'