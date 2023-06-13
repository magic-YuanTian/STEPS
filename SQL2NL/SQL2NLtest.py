from sql_metadata import Parser
import re
import sqlparse
import random
from pandas import DataFrame
import json
# from .ramdomSQL import *
from ramdomSQL import *


global g_p # global Parser

def num2ordinalStr(num):
    ORDINAL_NUMBER = {1:"first", 2:"second", 3:"third", 4:"fourth", 5:"fifth", 6:"sixth", 7:"seventh", 8:"eighth", 9:"nineth", 10:"tenth", 11:"eleventh", 12:"twelfth", 13:"thirteenth", 14:"fourteenth", 15:"fifteenth", 16:"sixteenth", 17:"seventeenth", 18:"eighteenth", 19:"nineteenth" }
    if int(num) in ORDINAL_NUMBER.keys():
        res = 'the ' + ORDINAL_NUMBER[int(num)]
    else:
        res = 'the ' + str(num) + 'th'

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
    tokens = []  # result list
    for tok in p.tokens:
        if tok.is_keyword:
            tokens.append(tok.value.upper())  # capitalize keyword token
        elif tok.value.lower() == 'like': # 'LIKE' is operator (not keyword), also needed to be capitalized
            tokens.append(tok.value.upper())
        else:
            tokens.append(tok.value)

    res = ' '.join(tokens)

    # there is a bug in this parser, we need to run it one more time to recognize function names
    p = Parser(res)
    tokens = []  # result list
    for tok in p.tokens:
        if tok.is_keyword:
            tokens.append(tok.value.upper())  # capitalize keyword token
        elif tok.value.lower() == 'like': # 'LIKE' is operator (not keyword), also needed to be capitalized
            tokens.append(tok.value.upper())
        else:
            tokens.append(tok.value)

    res = ' '.join(tokens)

    return res

# currently only return the first encountered subexpression
def getSubExpressionBeforeNextKeyword(sql, keyword):
    sql = capitalizeKeyword(sql)
    keyword = keyword.upper()  # capitalizeKeyword the keyword

    p = Parser(sql)  # get the parser
    temp_flag = 0  # denote if the keyword is encountered
    sub_expression = ''
    temp_tokens = []


    if keyword in sql:
        for tok in p.tokens:
            if tok.value == keyword:
                temp_flag = 1
            # if temp_flag == 1 and tok.value != keyword and tok.is_keyword:
            #     if tok.value != 'COUNT' and tok.value != 'AVG' and tok.value != 'MAX' and tok.value != 'MIN' and tok.value != 'SUM':
            #         break
            if temp_flag == 1 and tok.value != keyword and tok.is_keyword and tok.value != 'HAVING' and tok.value != 'AND' and tok.value != 'OR' and tok.value != 'COUNT' and tok.value != 'AVG' and tok.value != 'SUM' and tok.value != 'MIN' and tok.value != 'MAX' and tok.value != 'BETWEEN' and tok.value != 'between' and tok.value != 'DISTINCT':
                break
            if temp_flag == 1:
                temp_tokens.append(tok.value)

        sub_expression = ' '.join(temp_tokens)

    return sub_expression

# since SmBop will for sure add select .. from .. (join ... (as)), remove them in this function
def remove_select_from_for_structured_explanation(sql):
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

    # sql = preprocessSQL(sql)
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

# replace all aliases with the original table names
def removeAlias(sql):
    # sql = preprocessSQL(sql)
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
    return res

# remove (select ... from ... join ... as)
def removeSelectClause(sql, keyword):
    sql = capitalizeKeyword(sql)
    keyword = keyword.upper()  # capitalizeKeyword the keyword

    p = Parser(sql)  # get the parser
    temp_flag = 0  # denote if the keyword is encountered
    temp_flag2 = 0 # denote this (select ... from ... join ... as )subexpression has ended
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
            if temp_flag == 1 and tok.value != keyword and tok.is_keyword and tok.value != 'HAVING' and tok.value != 'AND' and tok.value != 'OR' and tok.value != 'COUNT' and tok.value != 'AVG' and tok.value != 'SUM' and tok.value != 'MIN' and tok.value != 'MAX' and tok.value != 'BETWEEN' and tok.value != 'between' and tok.value != 'DISTINCT' and tok.value != 'FROM' and tok.value != 'from' and tok.value != 'AS' and tok.value != 'as' and tok.value != 'JOIN' and tok.value != 'join':
                temp_tokens.append(tok.value)
                temp_flag2 = 1

        sub_expression = ' '.join(temp_tokens)

    return sub_expression

# get the description of nouns
# should input a sub sql expression
def getNouns(sub_sql, ori_sql):
    p = Parser(ori_sql)
    g_p = Parser(ori_sql)
    temp_subexpression = 'SELECT ' + sub_sql  # adding SELECT is for ignoring the exception
    p1 = Parser(temp_subexpression)  # get the parser for subexpression

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

        if funcs[i] == 'COUNT':
            if paras[i][0] == '*':
                noun += 'the number'
            else:
                noun += 'the number of '
        elif funcs[i] == 'MAX':
            noun += 'the maximum value of '
        elif funcs[i] == 'MIN':
            noun += 'the minimum value of '
        elif funcs[i] == 'AVG':
            noun += 'the average value of '
        elif funcs[i] == 'SUM':
            noun += 'the sum of '


        for j in range(0, len(paras[i])):
            if j != 0:
                noun += ' '
            if paras[i][j] != '*':
                noun += paras[i][j]

            # if len(paras) != 1 and j == len(paras)-1:
            #     noun += 'and'

            # if paras[i][j] != '*':
            #     noun += paras[i][j]
            # else:
            #     if funcs[i] != 'COUNT':
            #         noun += 'all the information'

        nouns.append(noun)

    # add column not included in functions to nouns
    for i in range(1, len(p1.tokens)):
        if p1.tokens[i].is_name and p1.tokens[i].parenthesis_level == 0:
            nouns.append(p1.tokens[i].value)
            # nouns.insert(0, p1.tokens[i].value)
        if p1.tokens[i].is_integer and p1.tokens[i].parenthesis_level == 0:
            nouns.append(p1.tokens[i].value)
            # nouns.insert(0, p1.tokens[i].value)
        if p1.tokens[i].value == '*' and p1.tokens[i].parenthesis_level == 0:
            nouns.append('all')
            # nouns.insert(0, 'all')
        # if p1.tokens[i].next_token.is_keyword:
        #     print("exception: the subexpression should only contain 1 keyword!")
        #     break

    # add nouns to the explanation
    for i in range(0, len(nouns)):
        if i != 0 and len(nouns) != 2:
            # temp_explanation += ','
            temp_explanation += ' '
        if len(nouns) != 1 and i == len(nouns) - 1:
            # temp_explanation += ' and'
            temp_explanation += ' '
        temp_explanation += ' '
        temp_explanation += nouns[i]

    # alias.Y ---> Y of X
    exp_token = temp_explanation.split()
    for i in range(0, len(exp_token)):
        tok = exp_token[i]
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
            exp_token[i] = res_str

    temp_explanation = ' '.join(exp_token)

    # try:
    #
    # except Exception:
    #     # print(Exception)
    #     pass



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
    p = Parser(condition_sql)

    # conflict: 'BETWEEN a AND b' v.s. 'AND'
    # solution: first replace 'BETWEEN a AND b' with 'BETWEEN axANDyb', and replace back later

    replace_back_list = [] # store the information to replace back when the function ends

    if 'between' in condition_clause or 'BETWEEN' in condition_clause:
        # find 'BETWEEN a AND b'
        for i in range(0, len(p.tokens)):
            if p.tokens[i].value == 'BETWEEN' or p.tokens[i].value == 'between':
                ori = ''
                new = ''
                # get the remaining part until (1) next key word OR (2) the end of the clause
                temp_tok = p.tokens[i].next_token
                and_flag = 0 # used to indicate whether 'AND' is met
                while True:
                    temp_tok_encode_value = temp_tok.value
                    # when AND is met
                    if temp_tok.value == 'AND':
                        temp_tok_encode_value = 'X123X' # in case AND will conflict later
                        and_flag = 1

                    if len(ori) == 0:
                        ori += temp_tok.value
                    else:
                        ori += (' ' + temp_tok.value)

                    new += temp_tok_encode_value

                    # determine whether continue or end
                    if (temp_tok.next_token.is_keyword and and_flag == 1) or (temp_tok.next_token.position == -1):
                        break
                    else:
                        temp_tok = temp_tok.next_token

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
    p = Parser(condition_sql)

    separaters = [] # 'OR', 'AND', 'NOT'
    nouns = [] # [[], [], ...]
    operators = [] # [ , ]
    operator_conditions = [] # [[], [], ...]


    flag = 0 # 0 for noun, 1 for condition
    for i in range(2, len(p.tokens)):
        # if p.tokens[i].value == 'AND' or p.tokens[i].value == 'OR' or p.tokens[i].value == 'NOT':
        if p.tokens[i].value == 'AND' or p.tokens[i].value == 'OR':
            separaters.append(p.tokens[i].value)
            flag = 0 # go to next condition
            continue
        if p.tokens[i].value == '>' or p.tokens[i].value == '<' or p.tokens[i].value == '=' or p.tokens[i].value == '!=' or p.tokens[i].value == '>=' or p.tokens[i].value == '<=' or p.tokens[i].value == 'LIKE' or p.tokens[i].value == 'like' or p.tokens[i].value == 'NOT LIKE' or p.tokens[i].value == 'not like' or p.tokens[i].value == 'not LIKE' or p.tokens[i].value == 'IN' or p.tokens[i].value == 'in' or p.tokens[i].value == 'NOT IN' or p.tokens[i].value == 'not in' or p.tokens[i].value == 'BETWEEN' or p.tokens[i].value == 'between':
            operators.append(p.tokens[i].value)
            flag = 1
            continue
        if flag == 0:
            if len(nouns) <= len(separaters):
                atemp = []
                nouns.append(atemp)
            nouns[len(separaters)].append(p.tokens[i].value)

        else:
            if len(operator_conditions) <= len(separaters):
                atemp = []
                operator_conditions.append(atemp)
            operator_conditions[len(separaters)].append(p.tokens[i].value)

    # generate NL
    explanation = ''

    explanation = getNouns(' '.join(nouns[0]), ori_sql) + ' ' + operators[0] + ' ' + getNouns(' '.join(operator_conditions[0]), ori_sql) # explanation for the first part

    for i in range(0, len(separaters)):
        explanation += ' '
        explanation += separaters[i]
        explanation += ' '
        explanation += getNouns(' '.join(nouns[i+1]), ori_sql)
        explanation += ' '
        explanation += operators[i+1]
        explanation += ' '
        explanation += getNouns(' '.join(operator_conditions[i+1]), ori_sql)

    explanation = NLforOperator(explanation)

    # replace back
    for dict in replace_back_list:
        explanation = explanation.replace(dict['new'], dict['ori'])

    return explanation

# return the explanation for 'FROM ... [JOIN <nouns> [ON <Condition>]]
# def explainFrom()

# no recursion for intersect, union, except
def parseSQL(sql):
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
    if p.tokens[0].value != 'SELECT':
        print("exception: the first word is not SELECT, it is " + p.tokens[0].value)
        return sub_expression, explanations

    # error check: there must exists FROM in sql
    if 'FROM' not in sql:
        print("exception: no FROM")
        return sub_expression, explanations

    temp_tokens = []
    temp_flag = 0  # denote if "FROM" or "JOIN" is encountered
    range_idx = 0
    # match the substring before the next keyword of "FROM"
    if 'JOIN' not in sql:
        for tok in p.tokens:
            if tok.value == "FROM":
                temp_flag = 1
            if temp_flag == 1 and tok.value != 'FROM' and tok.is_keyword:
                break
            temp_tokens.append(tok.value)
    # match the substring before the next keyword of "FROM"
    # 'JOIN' in sql
    else:
        # first, find the last index of 'AS' or 'ON'
        for i in range(0, len(p.tokens)):
            if p.tokens[i].value == 'JOIN' or p.tokens[i].value == 'AS' or p.tokens[i].value == 'ON':
                range_idx = i

        # second, find the index of next key word
        for i in range(range_idx + 1, len(p.tokens)):
            if p.tokens[i].is_keyword:
                range_idx = i  # the range is from 0 to the position before next keyword
                break

        # get the subexpression
        for _ in range(0, range_idx):
            temp_tokens.append(p.tokens[_].value)

    temp_subexpression = ' '.join(temp_tokens)
    sub_expression.append(temp_subexpression)

    # generate NL explanation for select subexpression

    p1 = Parser(temp_subexpression)  # get the parser for subexpression

    # generate explanation for 'FROM ... [JOIN <nouns> [ON <Condition>]]'
    from_tb = '' # used to store the table name of FROM
    join_tbs = [] # used to store the table name of JOIN
    for i in range(0, len(p1.tokens)):
        if p1.tokens[i].value == 'FROM':
            from_tb = p1.tokens[i].next_token.value
        elif p1.tokens[i].value == 'JOIN':
            join_tbs.append(p1.tokens[i].next_token.value)
        i += 1
    # remove repetitive elements (while keep the order unchanged)
    join_tbs_set = []
    for j in range(len(join_tbs)):
        if join_tbs[j] not in join_tbs_set:
            join_tbs_set.append(join_tbs[j])
    # generate the explanation of "from ... join ... "
    from_exp = ' of ' + from_tb
    for j in range(0, len(join_tbs_set)):
        if j == 0:
            from_exp += ' that has ' + join_tbs_set[j]
        else:
            from_exp += ' and ' + join_tbs_set[j]


    # randomly choose a verb
    # verb = ['Find out', 'Find', 'Get', 'Show me', 'Show', 'List', 'Give me', 'Tell me', 'search']
    verb = ['Get']

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

    positions1 = [i + 1 for i in positions] # move from '(' to the index of parameter

    # get function names (right in front of '(' )
    for pos in positions:
        funcs.append(p1.tokens[pos-1].value)

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
                noun += 'the number'
            else:
                noun += 'the number of '
        elif funcs[i] == 'MAX':
            noun += 'the maximum value of '
        elif funcs[i] == 'MIN':
            noun += 'the minimum value of '
        elif funcs[i] == 'AVG':
            noun += 'the average value of '
        elif funcs[i] == 'SUM':
            noun += 'the sum of '

        for j in range(0, len(paras[i])):
            if j != 0:
                noun += ' '
            # if len(paras) != 1 and j == len(paras)-1:
            #     noun += 'and'
            if paras[i][j] != '*':
                noun += paras[i][j]
            else:
                if funcs[i] != 'COUNT':
                    noun += 'all'


        nouns.append(noun)

    # add column not included in functions to nouns
    for i in range(0, len(p1.tokens)):
        if p1.tokens[i].is_name and p1.tokens[i].parenthesis_level == 0:
            nouns.append(p1.tokens[i].value)
            # nouns.insert(0, p1.tokens[i].value)
        if p1.tokens[i].value == '*' and p1.tokens[i].parenthesis_level == 0:
            nouns.append('all')
            # nouns.insert(0, 'all')
        if p1.tokens[i].next_token.value == 'FROM':
            break


    temp_explanation = random.choice(verb)

    # add nouns to the explanation
    for i in range(0, len(nouns)):
        # if i != 0 and len(nouns) != 2:
        #     temp_explanation += ','
        if len(nouns) != 1 and i == len(nouns)-1:
            temp_explanation += ' '
        if i != 0:
            temp_explanation += ' and'
        temp_explanation += ' '
        temp_explanation += nouns[i]

    # # alias.Y ---> Y of X
    # exp_token = temp_explanation.split()
    # for i in range(0, len(exp_token)):
    #     tok = exp_token[i]
    #     if '.' in tok:
    #         temp_tok = tok.split('.')
    #         if len(temp_tok) != 2:
    #             print("exception: . should split it into 2 parts")
    #             break
    #         if temp_tok[0] in g_p.tables_aliases.keys():
    #             table_name = g_p.tables_aliases[temp_tok[0]]
    #         else:
    #             table_name = temp_tok[0]
    #
    #         # hold the comma
    #         comma = ''
    #         if ',' in temp_tok[1]:
    #             comma = ','
    #             temp_tok[1] = temp_tok[1].replace(',', '')
    #         # Y of X
    #         res_str = temp_tok[1] + ' of ' + table_name + comma
    #         exp_token[i] = res_str

    # no longer need to do: alias.Y ---> Y of X
    # instead just delete [alias]
    exp_token = temp_explanation.split()
    for i in range(0, len(exp_token)):
        tok = exp_token[i]
        if '.' in tok:
            temp_tok = tok.split('.')
            if len(temp_tok) != 2:
                print("exception: . should split it into 2 parts")
                break
            exp_token[i] = temp_tok[1]


    temp_explanation = ' '.join(exp_token)

    temp_explanation += from_exp # add explanation for ''

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

    if 'WHERE' in sql:
        # get the subexpression for where clause
        where_clause = [token for token in parsed[0] if isinstance(token, sqlparse.sql.Where)][0]

        # get the explanation for where clause
        sub_expression.append(str(where_clause))

        # replace operators with natural language
        str_where_clause = str(where_clause)

        temp_explanation = ("Keep the records that " + naturalCondition(str_where_clause, sql))

        explanations.append(temp_explanation)

        # if '>=' in str_where_clause:
        #     str_where_clause = str_where_clause.replace('>=', 'is greater than or equal to')
        # if '<=' in str_where_clause:
        #     str_where_clause = str_where_clause.replace('<=', 'is less than or equal to')
        # if '>' in str_where_clause:
        #     str_where_clause = str_where_clause.replace('>', 'is greater than')
        # if '<' in str_where_clause:
        #     str_where_clause = str_where_clause.replace('<', 'is less than')
        # if '!=' in str_where_clause:
        #     str_where_clause = str_where_clause.replace('!=', 'is not')
        # if '=' in str_where_clause:
        #     str_where_clause = str_where_clause.replace('=', 'is')
        # if ' LIKE ' in str_where_clause:
        #     str_where_clause = str_where_clause.replace(' LIKE ', ' is in the form of ')
        #
        # # modify the where clause
        # # xianxingci = ['where', 'whose', 'in which']
        # xianxingci = ['that']
        # str_where_clause = str_where_clause.replace('WHERE', random.choice(xianxingci))
        #
        # # alias.Y ---> Y of X
        # exp_token = str_where_clause.split()
        # for i in range(0, len(exp_token)):
        #     tok = exp_token[i]
        #     if '.' in tok:
        #         temp_tok = tok.split('.')
        #         if len(temp_tok) != 2:
        #             print("exception: . should split it into 2 parts")
        #             break
        #         if temp_tok[0] in g_p.tables_aliases.keys():
        #             table_name = g_p.tables_aliases[temp_tok[0]]
        #         else:
        #             table_name = temp_tok[0]
        #         # hold the comma
        #         comma = ''
        #         if ',' in temp_tok[1]:
        #             comma = ','
        #             temp_tok[1] = temp_tok[1].replace(',', '')
        #         # Y of X
        #         res_str = temp_tok[1] + ' of ' + table_name + comma
        #         exp_token[i] = res_str
        # str_where_clause = ' '.join(exp_token)


    '''
        get the subexpression and explanation for "GROUP BY ... (HAVING ...)"
    '''

    if 'GROUP BY' in sql:
        temp_explanation = ''
        group_clause = getSubExpressionBeforeNextKeyword(sql, 'GROUP BY')
        noun = getNouns(group_clause, sql)

        temp_explanation = 'Group the records based on ' + noun
        # replace '*' with "it"
        if '*' in noun:
            noun = noun.replace('*', 'it')

        temp_subexpression = group_clause

        # having
        # there may be 'OR' or 'AND' so

        if 'HAVING' in sql:
            p = Parser(sql)  # get the parser
            temp_flag = 0  # denote if the keyword is encountered
            # sub_expression = ''
            temp_tokens = []

            for tok in p.tokens:
                if tok.value == 'HAVING':
                    temp_flag = 1
                if temp_flag == 1 and tok.value != 'HAVING' and tok.value != 'AND' and tok.value != 'OR' and tok.value != 'COUNT' and tok.value != 'AVG' and tok.value != 'SUM' and tok.value != 'MIN' and tok.value != 'MAX' and tok.value != 'BETWEEN' and tok.value != 'between' and tok.value != 'DISTINCT' and tok.is_keyword:
                    break
                if temp_flag == 1:
                    temp_tokens.append(tok.value)

            have_clause = ' '.join(temp_tokens) # get the sub expression for having

            temp_subexpression += ' ' + have_clause
            temp_explanation += (' that ' + naturalCondition(have_clause, sql))


        explanations.append(temp_explanation)

        sub_expression.append(temp_subexpression)



    '''
        get the subexpression and explanation for "ORDER BY ... (ASC/DESC) (LIMIT (value))"
    '''
    if 'ORDER BY' in sql:
        temp_explanation = ''
        order_clause = getSubExpressionBeforeNextKeyword(sql, 'ORDER BY')

        noun = getNouns(order_clause, sql)
        temp_explanation = 'Order these records based on the ' + noun

        sorting = ' ' # used to express ASC or DESC
        limit_clause = ''
        if 'ASC' or 'DESC' in sql:
            if 'ASC' in sql:
                temp_explanation += ' and sort them in ascending order'
                sorting = ' ASC '
            elif 'DESC' in sql:
                temp_explanation += ' and sort them in descending order'
                sorting = ' DESC '

            if 'LIMIT' in sql:
                limit_clause = getSubExpressionBeforeNextKeyword(sql, 'LIMIT')
                for tok in p.tokens:
                    if tok.value == 'LIMIT':
                        if tok.next_token.position != -1:
                            if not tok.next_token.is_integer:
                                print("exception: the limit value should be an integer")
                            limit_value = tok.next_token.value
                            if int(limit_value) == 1:
                                temp_explanation += ', and return the top 1 record'
                            elif int(limit_value) > 1:
                                temp_explanation += ', and return the top ' + limit_value + ' records'
                        else:
                            temp_explanation += ', and return the top 1 record'

        order_clause = order_clause + sorting + limit_clause
        sub_expression.append(order_clause)

        explanations.append(temp_explanation)


    # replace '_' with ' ' in explanations
    # postprocess explanations
    for i in range(0, len(explanations)):
        explanations[i] = explanations[i].replace('_', ' ')
        explanations[i] = postprocess(explanations[i], sql)
        # replace back for decimal point: numberfloatreplacingprocessthisisjustapaddingstringhaha ---> .
        sub_expression[i] = sub_expression[i].replace('numberfloatreplacingprocessthisisjustapaddingstringhaha', '.')
        for key in g_dict.keys():
            sub_expression[i] = sub_expression[i].replace(g_dict[key], key)

    return [sub_expression, explanations]


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

# each 'SELECT' corresponds to 1 subquery
# decompose compound sql into atom sqls
# there 2 situations: INT or nested
def decompose(sql):
    global g_subSQL
    global high_level_explanation

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
        if p.tokens[i].parenthesis_level == 0 and (p.tokens[i].value == 'INTERSECT' or p.tokens[i].value == 'UNION' or p.tokens[i].value == 'EXCEPT'):
            pos = i # divide 2 sub-queries from this token

            # left sub-sql
            sql1 = ' '.join(token_list[0:pos])
            while sql1[0] == '(' and sql1[-1] == ')':
                sql1 = sql1.strip('()') # delete extra parenthesis
            #right sub-sql
            sql2 = ' '.join(token_list[pos+1:])
            while sql2[0] == '(' and sql2[-1] == ')':
                sql2 = sql2.strip('()') # delete extra parenthesis
            # recursively decompose
            res1 = decompose(sql1) # res: the xxth query result
            res2 = decompose(sql2)

            # construct result (which query this is)
            result = num2ordinalStr(len(high_level_explanation)+1) + ' query result'

            # construct replaced/modified subquery
            modified_subquery = res1 + ' ' + p.tokens[i].value.lower() + ' ' + res2

            # construct the explanation
            if p.tokens[i].value == 'INTERSECT':
                content = 'Keep the intersection of ' + res1 + ' and ' + res2 + '.'
            elif p.tokens[i].value == 'UNION':
                content = 'Keep the union of ' + res1 + ' and ' + res2 + '.'
            elif p.tokens[i].value == 'EXCEPT':
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
        if p.tokens[i].value == 'SELECT' and p.tokens[i].parenthesis_level == 1 and p.tokens[i].previous_token.value == '(':
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
            result = num2ordinalStr(len(high_level_explanation)+1) + ' query result'

            # construct replaced/modified subquery
            temp_concatenate_res_str = res1.replace(' ', '123') # to prevent this string is broken when parse the sql, and replace back later
            # modified_subquery = ' '.join(token_list[0:pos - 1]) + ' ' + temp_concatenate_res_str
            modified_subquery = sql.replace(sql1, temp_concatenate_res_str)

            # delete ’(' and ')' of modified_subquery and double check whether if the parenthesis include the subquery
            temp_str_tok_l = modified_subquery.split()
            temp_index = temp_str_tok_l.index(temp_concatenate_res_str)
            # check if the left of subquery is '(' and the right of subquery is ')'
            if temp_str_tok_l[temp_index-1] == '(' and temp_str_tok_l[temp_index+1] == ')':
                # delete the corresponding '(' and ')'
                del temp_str_tok_l[temp_index + 1]
                del temp_str_tok_l[temp_index - 1]
            else:
                print("Exception: the left of subquery should be ( and the right of subquery should be )")

            # get the modified_subquery without '(', ')'
            modified_subquery_new = ' '.join(temp_str_tok_l)

            # parse the sql and get the structured explanantion
            content = parseSQL(modified_subquery_new)

            # add '()' to the xxth query result
            res11 = '( ' + res1 + ' ) '

            # find the corresponding string in structured explanation, and replace back
            # the123first123query123result ---> the first query
            for i in range(0, len(content[0])):
                content[0][i] = content[0][i].replace(temp_concatenate_res_str, res11)
            for i in range(0, len(content[1])):
                content[1][i] = content[1][i].replace(temp_concatenate_res_str, res1)

            temp_explanation_dict['number'] = result
            temp_explanation_dict['subquery'] = modified_subquery
            temp_explanation_dict['explanation'] = content
            temp_explanation_dict['supplement'] = result + ' uses ' + res1 # add supplement explanation

            # add it to high level explanation list
            high_level_explanation.append(temp_explanation_dict)

            return result

    # otherwise, there should no further high level
    # directly regard it as atomic sql
    # double check:
    if sql.count('SELECT') != 1:
        print("Exception: there should only 1 SELECT in the atomic query sentence!")
        return

    # construct result (which query this is)
    result = num2ordinalStr(len(high_level_explanation)+1) + ' query result'
    # parse the sql and get the structured explanantion
    content = parseSQL(sql)

    temp_explanation_dict['number'] = result
    temp_explanation_dict['subquery'] = sql  # doesn't change
    temp_explanation_dict['explanation'] = content

    # add it to high level explanation list
    high_level_explanation.append(temp_explanation_dict)

    return result

    # if ' INTERSECT ' in sql:
    #     temp_sub = sql.split(' INTERSECT ', 1)
    #     g_subSQL[num] = temp_sub[0]
    #     g_subSQL[num + 1] = temp_sub[1]
    #     # explanation_for_compound[num-1] = 'keep the intersection of #' + str(num) + ' and #' + str(num+1)
    #     explanation_for_compound[num - 1] = 'keep the intersection of the two query results'
    #     # replace the nested clause with <x>
    #     g_subSQL[num - 1] = g_subSQL[num - 1].replace(temp_sub[0], '#' + str(num))
    #     g_subSQL[num - 1] = g_subSQL[num - 1].replace(temp_sub[1], '#' + str(num+1))
    #     # recursion
    #     decompose(temp_sub[0])
    #     decompose(temp_sub[1])
    # elif ' UNION ' in sql:
    #     temp_sub = sql.split(' UNION ', 1)
    #     g_subSQL[num] = temp_sub[0]
    #     g_subSQL[num + 1] = temp_sub[1]
    #     explanation_for_compound[num-1] = 'keep the union of #' + str(num) + ' and #' + str(num+1)
    #     explanation_for_compound[num - 1] = 'keep the union of the two query results'
    #     # replace the nested clause with <x>
    #     g_subSQL[num - 1] = g_subSQL[num - 1].replace(temp_sub[0], '#' + str(num))
    #     g_subSQL[num - 1] = g_subSQL[num - 1].replace(temp_sub[1], '#' + str(num+1))
    #     # recursion
    #     decompose(temp_sub[0])
    #     decompose(temp_sub[1])
    # elif ' EXCEPT ' in sql:
    #     temp_sub = sql.split(' EXCEPT ', 1)
    #     g_subSQL[num] = temp_sub[0]
    #     g_subSQL[num + 1] = temp_sub[1]
    #     # explanation_for_compound[num-1] = 'keep the record of #' + str(num) + ' except contents in #' + str(num+1)
    #     explanation_for_compound[num - 1] = 'keep the records in the first query but not in the second query'
    #     # replace the nested clause with <x>
    #     g_subSQL[num - 1] = g_subSQL[num - 1].replace(temp_sub[0], '#' + str(num))
    #     g_subSQL[num - 1] = g_subSQL[num - 1].replace(temp_sub[1], '#' + str(num+1))
    #     # recursion
    #     decompose(temp_sub[0])
    #     decompose(temp_sub[1])

    # num = len(g_subSQL)  # number for this sub SQL
    # # 2. situation for nested
    # stack = 1 # used to match '(' and ')'
    # idx = 0 # used to find all nested subs
    # for i in range(0, len(p.tokens)):
    #     if p.tokens[i].value == 'SELECT' and p.tokens[i].previous_token.value == '(':
    #         sub_tokens = []
    #         # detect the nested sub query, now find the range
    #         for j in range(i, len(p.tokens)):
    #             if p.tokens[j].value == '(':
    #                 stack += 1
    #             elif p.tokens[j].value == ')':
    #                 stack -= 1
    #             # successfully matched
    #             if stack == 0:
    #                 sub_nested_sql = ' '.join(sub_tokens)
    #                 explanation_for_compound[num-1] = 'the result of #' + str(num-1) + ' is based on #' + str(num)
    #                 g_subSQL[num] = sub_nested_sql
    #                 # replace the nested clause with <x>
    #                 temp_str = '#' + str(num)
    #                 g_subSQL[num-1] = g_subSQL[num-1].replace(sub_nested_sql, temp_str)
    #                 decompose(sub_nested_sql)
    #                 idx = j+1
    #                 break
    #             sub_tokens.append(p.tokens[j].value)
    #     if idx > i:
    #         i = idx

    # sub_lists = []
    #
    # temp_sub_dict = {'start': -1, 'end': -1, 'level': -1, 'query': ''}
    # # query: INT may occur in the end of the subquery
    #
    # # find all the 'SELECT'
    # for tok in p.tokens:
    #     if tok.value == 'SELECT':
    #         # query level
    #         temp_sub_dict['level'] = tok.parenthesis_level
    #         # start index
    #         temp_sub_dict['start'] = tok.position
    #         # end index
    #         # find the end index
    #         # <1>
    #         # 'SELECT' is the first token: the end index should be (1) next 'SELECT' in the same level or (2) the end of SQL
    #         if tok.position == 0:
    #             temp_sub_dict['query'] += 'SELECT'
    #             temp_tok = tok.next_token
    #             while True:
    #                 # next token is 'SELECT'
    #                 if temp_tok.next_token.value == 'SELECT' and temp_tok.next_token.parenthesis_level == temp_sub_dict['level']:
    #                     temp_sub_dict['end'] = temp_tok.position
    #                     break
    #                 # the end of SQL sentence
    #                 elif temp_tok.next_token.value == '' and temp_tok.next_token.position == -1:
    #                     temp_sub_dict['end'] = temp_tok.position
    #                     break
    #                 # otherwise
    #                 else:
    #                     temp_sub_dict['query'] += (' ' + temp_tok.value)
    #                     temp_tok = temp_tok.next_token
    #         # <2>
    #         # nested: '(SELECT'
    #         # need to match the corresponding ')'
    #         elif tok.previous_token.value == '(':
    #             parethesis_num = 0
    #             temp_sub_dict['query'] += 'SELECT'
    #             temp_tok = tok.next_token
    #             while True:
    #                 # next token is the matched ')'
    #                 # check parethesis_num
    #                 if temp_tok.next_token.value == '(':
    #                     parethesis_num += 1
    #                 elif temp_tok.next_token.value == ')':
    #                     parethesis_num -= 1
    #
    #                 # check the end of this subquery
    #                 # when matched ')'
    #                 if temp_tok.next_token.value == ')' and temp_tok.next_token.parenthesis_level == temp_sub_dict['level'] - 1 and parethesis_num == 0:
    #                     temp_sub_dict['end'] = temp_tok.position
    #                     break
    #                 # error check: no matched ')' when encountering the end of SQL sentence
    #                 elif temp_tok.next_token.value == '' and temp_tok.next_token.position == -1 and parethesis_num != 0:
    #                     print("no matched ) when encountering the end of SQL sentence")
    #                     break
    #                 else:
    #                     temp_sub_dict['query'] += (' ' + temp_tok.value)
    #                     temp_tok = temp_tok.next_token
    #
    #         # <3>
    #         # INT SELECT ...
    #         elif tok.previous_token.value == 'INTERSECT' or tok.previous_token.value == 'UNION' or tok.previous_token.value == 'EXCEPT':
    #             temp_sub_dict['query'] += 'SELECT'
    #             temp_tok = tok.next_token
    #             while True:
    #                 # next token is 'SELECT'
    #                 if temp_tok.next_token.value == 'SELECT' and temp_tok.next_token.parenthesis_level == temp_sub_dict['level']:
    #                     temp_sub_dict['end'] = temp_tok.position
    #                     break
    #                 # the end of SQL sentence
    #                 elif temp_tok.next_token.value == '' and temp_tok.next_token.position == -1:
    #                     temp_sub_dict['end'] = temp_tok.position
    #                     break
    #                 # otherwise
    #                 else:
    #                     temp_sub_dict['query'] += (' ' + temp_tok.value)
    #                     temp_tok = temp_tok.next_token
    #         # add the temp subquery dictionary to the sub query list
    #         sub_lists.append(temp_sub_dict)
    #         # initialize the dict
    #         temp_sub_dict = {'start': -1, 'end': -1, 'level': -1, 'query': ''}
    #
    # # sort the list by parenthesis level
    # sort_subquery_list = sorted(sub_lists, key = lambda i: i['level'], reverse=True)
    #
    # # construct relationships between subqueries
    # # For each subquery, if


# global dictionary for content inside '' or ""
g_dict = {}

def preprocessSQL(sql):
    # save all values in '' or "" into a global list
    # assume it can't be nested


    # separate ' and "
    sql = sql.replace('\'', ' \' ')
    sql = sql.replace('\"', ' \" ')
    sql = re.sub(' +', ' ', sql)  # replace multiple spaces to 1


    # get the dictionary
    temp_token_list = sql.split() # to tokens
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
            del temp_token_list[i: j+1]
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
            temp_token_list[i] = temp_token_list[i].replace('.', 'numberfloatreplacingprocessthisisjustapaddingstringhaha')

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

    sql = re.sub(' +', ' ', sql) # replace multiple spaces to 1

    res = sql
    res = capitalizeKeyword(res)  # capitalize keywords

    # remove '', ""
    res = res.replace('\'', '')
    res = res.replace('\"', '')

    res = re.sub(' +', ' ', res)  # replace multiple spaces to 1





    # # exceptions
    # # WHERE dept_name  =  'Comp. Sci.'
    # res = res.replace('. ', ' ')
    # res = res.strip('. ')

    return res

def postprocess(explanation, ori_sql):
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
        result = result.replace(g_dict[key], key)

    # replace back numberfloatreplacingprocessthisisjustapaddingstringhaha ---> .
    result = result.replace('numberfloatreplacingprocessthisisjustapaddingstringhaha', '.')

    return result

# this function will replace all aliases in SQL with the original table names
def replaceAlias(sql):
    # format assumption: table AS alias
    # find all 'AS' in the SQL
    res = sql

    # construct the parser
    parser = Parser(sql)

    dict = parser.tables_aliases

    # replace all aliases in the original SQL
    for key in dict:
        temp_alias = key + '.'
        temp_table = dict[key] + '.'
        res = res.replace(temp_alias, temp_table)

    return res

# instruction = input('input the instruction > ')

def sql2nl(sql):

    global  high_level_explanation
    global g_p

    high_level_explanation = []

    sql = preprocessSQL(sql)
    parsed = sqlparse.parse(sql)
    # global g_p
    g_p = Parser(sql)  # get the parser
    # construct high level explanation
    decompose(sql)


    for i in range(0, len(high_level_explanation)):
        print('\nStart ' + high_level_explanation[i]['number'].replace('query result', 'query') + ',')

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
        relation = [tokL2[i], tokL2[i+1]]
        # check if this relationship exists in the original sequence
        flag = 0
        for j in range(len(tokL1) - 1):
            if flag == 1:
                break
            if tokL1[j] == relation[0]:
                # check the remaining tokens
                for k in range(j+1, len(tokL1)):
                    if tokL1[k] == relation[1]:
                        match_num += 1
                        flag = 1
                        break

    return match_num / (len(tokL2) - 1)

# return a number from 0-1 indicating the matching accuracy
# if the category is incorrect or the model predicts multiple, the score is 0
def matchScore(gold_sql, pred_sql):
    # preprocess
    gold_sql = gold_sql.lower()
    pred_sql = pred_sql.lower()

    tokL1 = gold_sql.split()
    tokL2 = pred_sql.split()

    # the first word are not same, the score is 0
    if tokL1[0].lower() != tokL2[0].lower():
        # return [0,1]
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

    # discuss for each situation
    if tokL2[0] == 'select':
        matched_num = 1
        total_num = 1  # including select
        # parse select nouns

        # get nouns before from of predicted sql
        # separated by from
        temp_tok_list = re.split('from', pred_sql)
        temp_str = temp_tok_list[0].replace('select', '').strip(' ')
        temp_var_pred = temp_tok_list[1]
        temp_tok_list = re.split(',| ', temp_str)  # get nouns before from
        select_nouns_pred = []
        # remove ''
        for ele in temp_tok_list:
            if ele != '':
                select_nouns_pred.append(ele)

        # get nouns before 'from' of gold sql
        temp_tok_list = re.split('from', gold_sql)
        temp_str = temp_tok_list[0].replace('select', '').strip(' ')
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

        # compare join (neglecting on ...)
        temp_tok_list1 = re.split('join', temp_var_pred)
        temp_tok_list2 = re.split('join', temp_var_gold)

        # calculate the portion of join nouns
        for tok in temp_tok_list1:
            total_num += 1
            if tok in temp_tok_list2:
                matched_num += 1

    elif tokL2[0] == 'where':
        matched_num = 1
        total_num = 1  # including where

        # conditions are separate by 'and', 'or'
        temp_str_pred = pred_sql.replace('where ', '').strip(' ')
        conditions_pred = re.split(' and | or ', temp_str_pred)

        # conditions are separate by 'and', 'or'
        temp_str_gold = gold_sql.replace('where ', '').strip(' ')
        conditions_gold = re.split(' and | or ', temp_str_gold)

        # calculate the portion of select_nouns
        for tok in conditions_pred:
            total_num += 1
            if tok in conditions_gold:
                matched_num += 1

        # calculate conjunction
        total_num += pred_sql.count('and')
        total_num += pred_sql.count('or')

        matched_num += max(pred_sql.count('and'), gold_sql.count('and'))
        matched_num += max(pred_sql.count('or'), gold_sql.count('or'))

    elif tokL2[0] == 'group':
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

            # calculate the portion of select_nouns
            for tok in conditions_pred:
                total_num += 1
                if tok in conditions_gold:
                    matched_num += 1

            # calculate conjunction
            total_num += pred_sql.count('and')
            total_num += pred_sql.count('or')

            matched_num += max(pred_sql.count('and'), gold_sql.count('and'))
            matched_num += max(pred_sql.count('or'), gold_sql.count('or'))

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



    elif tokL2[0] == 'order':
        # order by ... asc/desc limit value
        # regarding it as nouns

        matched_num = 1
        total_num = 1  # including select
        # parse select nouns

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

instruction = 'newTest'
if instruction == 'shell':
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

        for i in range(0, len(high_level_explanation)):
            print('\nStart ' + high_level_explanation[i]['number'].replace('query result', 'query') + ',')

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
    examples = json.load(open("dataset/spider/merge.json"))

    start_num = 0 # last end position
    for i in range(start_num, len(examples)):
        # test position
        print("\n##### " + str(i) + " ######")
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
        parsed = sqlparse.parse(sql)
        g_p = Parser(sql)  # get the parser
        # construct high level explanation
        decompose(sql)
        for i in range(0, len(high_level_explanation)):
            print('\nStart ' + high_level_explanation[i]['number'].replace('query result', 'query') + ',')

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

    keword_distribution = {'SELECT': 0, 'FROM': 0, 'JOIN': 0, 'AS': 0, 'ON': 0, 'WHERE': 0, 'GROUP BY': 0, 'HAVING': 0, 'ORDER BY': 0, 'LIMIT': 0}

    unnamed = [] # exception: subclause not belongs to these category


    data = {}

    GT_sql = [] # ground truth sql
    questions = [] # NL questions
    db_ids = [] # database id
    example_num = [] # the number of example

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
        pure_explanation = '' # pure explanation in 1 sentence
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
                pure_explanation += '\nStart ' + high_level_explanation[j]['number'].replace('query result', 'query') + ',\n' # edit pure explanation
            high_level_num.append(str(j+1)) # number of high level sub query (for each select)
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

    GT_sql = [] # ground truth sql
    questions = [] # NL questions
    db_ids = [] # database id
    example_num = [] # the number of example

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
        pure_explanation = '' # pure explanation in 1 sentence
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
                pure_explanation += '\nStart ' + high_level_explanation[j]['number'].replace('query result', 'query') + ',\n' # edit pure explanation
            high_level_num.append(str(j+1)) # number of high level sub query (for each select)
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

    data_num = 500 # the number of data needed to be generated
    # open the data file
    F = open("randomData.txt", "w")

    for i in range(data_num):
        print('############## ', i, ' ###############')
        sqlOB = randomAtomicSQL()
        ran_sql = sqlOB.sql_str # get the randomly generated SQL

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

elif instruction == 'newTest':
    sql = "SELECT T1.school_id FROM school AS T1 JOIN here AS T2 WHERE T1.age = 26 and T2.name = 'TOM' INTERSECT SELECT T3.isofficial FROM countrylanguage as T3 WHERE T3.language = 'English'"
    # res = sql2nl(sql)
    # sql = "SELECT singer.name FROM singer WHERE singer.is_male = 'pet' OR singer.is_male = 'pets'"
    sql = "SELECT cars_data.horsepower FROM cars_data WHERE cars_data.horsepower = 't' AND cars_data.horsepower <= 1 AND cars_data.horsepower <= 1 ORDER BY cars_data.horsepower DESC"
    sql = "SELECT cars_data.year FROM cars_data WHERE cars_data.year = (SELECT cars_data.year FROM cars_data JOIN  ON cars_data.year = )"


    sql1 = "SELECT name , country , age FROM singer"
    sql2 = "select singer.name , singer.country , singer.age from singer"

    score = matchScore(sql1, sql2)
    score2 = sequentialMatch(sql1, sql2)

    # sql = "WHERE student.sex = 'F' AND pets.pettype = 'dog'"
    # sql1 = remove_select_from_for_structured_explanation(sql)

    print(score2)
