import random
import string
import sqlparse

# a class used to randomly generate a atomic SQL sentence


# randomly if with certain probability
def randomIf(prob):
    prob = int(prob)
    # Probability = prob / 100
    list = []
    for i in range(prob):
        list.append(1)  # put 1s (of number = prob) into list
    for x in range(100 - prob):
        list.append(0)  # put 0s (of number = 100 - prob) into list
    a = random.choice(list)  # randomly choose one from list
    if a == 0:
        return False
    if a == 1:
        return True

# randomly add a function to this noun
def randomAddFunc(noun):
    funcs = ['MAX', 'MIN', 'AVG', 'COUNT', 'SUM']
    func = random.choice(funcs)
    res = func + '(' + noun + ')'

    if func == 'COUNT':
        if randomIf(20):
            res = 'COUNT(*)'

    return res

# randomly generate a string
def generateRandomString():
    ran_num = random.randint(2, 12) # length: 2-12
    generated_str = ''
    for i in range(ran_num):
        ran_str = ''.join(random.sample(string.ascii_letters + string.digits + "_", 1))
        generated_str += ran_str

    # the random string shouldn't be SQL keyword
    parsed = sqlparse.parse(generated_str)
    while parsed[0].tokens[0].is_keyword or generated_str.lower() == 'in' or generated_str.lower() == 'between' or generated_str.lower() == 'like' or generated_str.lower() == 'not' or generated_str.lower() == 'of':
        generated_str = ''
        for i in range(ran_num):
            ran_str = ''.join(random.sample(string.ascii_letters + string.digits + "_", 1))
            generated_str += ran_str

        parsed = sqlparse.parse(generated_str)

    return generated_str

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


# def generateRandomNoun():
#     # first, generate a random string
#     ran_str = generateRandomString()
#
#     # add function
#     if randomIf(20):
#         res = randomAddFunc(ran_str)

def generateRandomCondition():
    operators = ['>', '<', '>=', '<=', '=', '!=', 'LIKE', 'IN', 'NOT IN']
    opt = random.choice(operators)

    res = []

    # In most cases, the first operand should be noun, while the second operand could be noun or value

    # operand 1
    operand1 = generateRandomString()
    res.append(operand1)

    # operator
    res.append(opt)

    # operand 2
    if randomIf(80) and opt not in ['LIKE', 'IN', 'NOT IN']:
        operand2 = generateRandomValue()
    else:
        operand2 = generateRandomString()

    res.append(operand2)

    return res

# randomly generate value
def generateRandomValue():
    ran_num = random.randint(0, 100)
    return ran_num

# randomly generate limit value
def generateRandomLimitValue():
    ran_num = random.randint(1, 10)
    return ran_num



class randomAtomicSQL():

    def __init__(self):
        self.SQL = [] # SQL is a list where keyword are strings and place holders are represented by 0, 1,...

        # maintain table names
        self.tables = []

        # maintain index but not in table
        # which will be used to add table names to nouns at the end
        self.not_table_noun_index = []

        # generate a random SQL for this object
        self.completeSQL()

        # the final SQL string
        self.sql_str = " ".join([str(_) for _ in self.SQL])

    # append <condition> to the end of current self.SQL
    def addCondition(self):

        # possibly add 'NOT'
        # if randomIf(10):
        #     self.SQL.append('NOT')
        temp_condition = generateRandomCondition()
        self.not_table_noun_index.append(len(self.SQL))
        self.SQL.append(temp_condition[0]) # operand1
        self.SQL.append(temp_condition[1]) # operator
        # check if operand 2 is a number
        if not str(temp_condition[2]).isdigit():
            self.not_table_noun_index.append(len(self.SQL))
        self.SQL.append(temp_condition[2])  # operand2

        prob = 40
        while randomIf(prob):

            # AND
            if randomIf(50):
                self.SQL.append('AND')
                # # possibly add 'NOT'
                # if randomIf(10):
                #     self.SQL.append('NOT')
                temp_condition = generateRandomCondition()
                self.not_table_noun_index.append(len(self.SQL))
                self.SQL.append(temp_condition[0])  # operand1
                self.SQL.append(temp_condition[1])  # operator
                # check if operand 2 is a number
                if not str(temp_condition[2]).isdigit():
                    self.not_table_noun_index.append(len(self.SQL))
                self.SQL.append(temp_condition[2])  # operand2
            # OR
            else:
                self.SQL.append('OR')
                # # possibly add 'NOT'
                # if randomIf(10):
                #     self.SQL.append('NOT')
                temp_condition = generateRandomCondition()
                self.not_table_noun_index.append(len(self.SQL))
                self.SQL.append(temp_condition[0])  # operand1
                self.SQL.append(temp_condition[1])  # operator
                # check if operand 2 is a number
                if not str(temp_condition[2]).isdigit():
                    self.not_table_noun_index.append(len(self.SQL))
                self.SQL.append(temp_condition[2])  # operand2

            # decrease the probability
            prob = prob / 2 + 1


    def completeSQL(self):
        # there must be 'SELECT ... FROM ...'
        self.SQL.append('SELECT')

        temp_noun = generateRandomString()
        # add DISTINCT
        if randomIf(10):
            temp_noun = 'DISTINCT ' + temp_noun
        self.not_table_noun_index.append(len(self.SQL))
        self.SQL.append(temp_noun)

        # there may be multiple nouns after 'SELECT'
        prob = 20
        while randomIf(prob):
            self.SQL.append(',')
            temp_noun = generateRandomString()
            # add DISTINCT
            if randomIf(10):
                temp_noun = 'DISTINCT ' + temp_noun
            self.not_table_noun_index.append(len(self.SQL))
            self.SQL.append(temp_noun)

            # decrease the probability
            prob = prob / 2 + 1

        self.SQL.append('FROM')
        temp_noun = generateRandomString()
        self.SQL.append(temp_noun)
        self.tables.append(temp_noun)

         # add 'JOIN'
        prob = 50
        while randomIf(prob):
            self.SQL.append('JOIN')
            temp_noun = generateRandomString()
            self.SQL.append(temp_noun)
            self.tables.append(temp_noun)

            # add 'AS'
            if randomIf(50):
                self.SQL.append('AS')
                temp_noun = generateRandomString()
                self.SQL.append(temp_noun)
                self.tables.append(temp_noun) # alias will also be added into table


            # add 'ON' <condition>
            if randomIf(50):
                self.SQL.append('ON')
                self.addCondition()

            # decrease the probability
            prob = prob / 2 + 1

        # add 'WHERE' <condition>
        if randomIf(50):
            self.SQL.append('WHERE')
            self.addCondition()

        # add GROUP BY <nouns> [having <condition>]
        if randomIf(50):
            self.SQL.append('GROUP BY')
            temp_noun = generateRandomString()
            self.not_table_noun_index.append(len(self.SQL))
            self.SQL.append(temp_noun)

            # having <condition>
            if randomIf(50):
                self.SQL.append('HAVING')
                self.addCondition()

        # add 'ORDER BY <nouns> [<sorting>] [LIMIT <number>]'
        if randomIf(50):
            self.SQL.append('ORDER BY')
            temp_noun = generateRandomString()
            self.not_table_noun_index.append(len(self.SQL))
            self.SQL.append(temp_noun)

            # ASC or DESC
            if randomIf(85):
                if randomIf(50):
                    self.SQL.append('ASC')
                else:
                    self.SQL.append('DESC')

            if randomIf(50):
                self.SQL.append('LIMIT')
                if randomIf(75):
                    self.SQL.append(1)
                else:
                    temp_num = generateRandomValue()
                    self.SQL.append(temp_num)

        # for not_table_nouns:
        # possibly
        # 1) X -> Y.X
        # 2) add function
        # 3) add 'DISTINCT'

        for idx in self.not_table_noun_index:
            # 1) X -> Y.X
            rand_table = random.choice(self.tables)
            if randomIf(50):
                self.SQL[idx] = rand_table + '.' + self.SQL[idx]

            # 2) add function
            if randomIf(40):
                self.SQL[idx] = randomAddFunc(self.SQL[idx])

            # # 3) add 'DISTINCT'
            # if randomIf(10):
            #     self.SQL[idx] = 'DISTINCT ' + self.SQL[idx]




# for i in range(10000):
#     sqlOB = randomAtomicSQL()
#
#     print(sqlOB.sql_str)