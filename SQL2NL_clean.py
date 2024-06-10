import copy
from sql_metadata import Parser
import re
import sqlparse
import random
import string

global g_p  # global Parser
global data_flag

#if true, the subexpression is for data generation;    (obsolete)
# otherwise, for explanation    '...not in ()'
data_flag = False  # just set it to False as default


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

natural_name_flag = False



g_schema = {} # the schema of current SQL

# randomly generate a string without digits
# lower case (for conveniently lower sql sentence)
def generateRandomLetterString():
    ran_num = random.randint(2, 12) # length: 2-12
    generated_str = ''
    for i in range(ran_num):
        ran_str = ''.join(random.sample(string.ascii_letters, 1))
        generated_str += ran_str

    # the random string shouldn't be SQL keyword
    parsed = sqlparse.parse(generated_str)
    while parsed[0].tokens[0].is_keyword or generated_str.lower() == 'in' or generated_str.lower() == 'between' or generated_str.lower() == 'like' or generated_str.lower() == 'not' or generated_str.lower() == 'of':
        generated_str = ''
        for i in range(ran_num):
            ran_str = ''.join(random.sample(string.ascii_letters, 1))
            generated_str += ran_str

        parsed = sqlparse.parse(generated_str)

    generated_str = generated_str.lower()
    return generated_str

def preprocessSQL(sql):
    
    # save all values in '' or "" into a global list
    # assume it can't be nested
    global g_dict
    global replace_dict
    # separate ' and "
    # sql = sql.replace('\'', ' \' ')
    # sql = sql.replace('\"', ' \" ')

    # add quotes
    sql = addQuotes(sql)

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
        temp_subquery_str = simpleCompose(unit['explanation'][0], '')
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



if __name__ == "__main__":
    instruction = 'sql2nl'
    
    if instruction == 'sql2nl':
        while True:
            sql = input("\ninput the sql > ")
            result_explanation = sql2nl(sql)
            print('\nDATA:\n' + '-' * 30 + '\n' + str(result_explanation))


# test_sql = 'SELECT first_name , last_name FROM players ORDER BY birth_date GROUP BY birth_date HAVING COUNT ( * ) > 1 LIMIT 1 INTERSECT SELECT first_name , last_name FROM players ORDER BY birth_date GROUP BY birth_date HAVING COUNT ( * ) > 1 LIMIT 1 INTERSECT SELECT first_name , last_name FROM players WHERE first_name = TOM ORDER BY birth_date GROUP BY birth_date HAVING COUNT ( * ) > 1 LIMIT 1'