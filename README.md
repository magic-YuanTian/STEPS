# STEPS

![image](https://github.com/magic-YuanTian/STEPS/assets/75125334/74bd2efb-fa87-48c7-8088-f096cfeb5393)


### Paper
[Interactive Text-to-SQL Generation via Editable Step-by-Step Explanations](https://arxiv.org/abs/2305.07372)


### Dataset
1. Download the [original Spider dataset](https://drive.google.com/uc?export=download&id=1TqleXec_OykOYFREKKtschzY29dUcVAQ)
2. Generate text to clause dataset using SQL2NL/SQL2NL.py
  - instruction = 'trainingData'.
  - You can design your own explanation template within the method "parseSQL()".
  - python SQL2NL.py and you will get the dataset under "dataset/structured/spider/train_spider.json".
  - You could also download [our raw text-to-clause dataset](https://drive.google.com/file/d/1f1fnJK2vGuRpaQOeMlBD10tQMDH3dR83/view?usp=sharing). Please put it in the same directory as the original Spider dataset and include all the databases (For more information, please refer to https://github.com/taoyds/spider)
3. Paraphrase the text-to-clause dataset (optional)
  - You could paraphrase the dataset by [Quillbot](https://quillbot.com/) with our automated script based on [PyAutoGUI](https://pyautogui.readthedocs.io/en/latest/).
  - Please check the [script](Paraphrase/main.py) and all screenshots under [here](Paraphrase/img). These screenshots are used to position the cursor during the automation. Due to subtle resolution/theme/version differences, the screenshots may not be identified on your computer (even if a human can), you may need to take your screenshots on your computer and replace them manually.



**Distribution of dataset used for this project:**
![clause_distribution](https://github.com/user-attachments/assets/3f3b5cbb-8889-4497-8619-c07f3e5346c6)





### Models
1. Our text-to-clause model is based on [SmBoP](https://github.com/OhadRubin/SmBop), and you can strictly follow their environment and settings.
2. You can directly download and reuse our [check point](https://purdue0-my.sharepoint.com/personal/tian211_purdue_edu/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Ftian211%5Fpurdue%5Fedu%2FDocuments%2FSTEPS%2FmodelS%2Etar%2Egz&parent=%2Fpersonal%2Ftian211%5Fpurdue%5Fedu%2FDocuments%2FSTEPS) ([HuggingFace](https://huggingface.co/DoctorChaos/text-to-SQL-clause-smbop/blob/main/README.md)) as well as configuration file. 
  - Please replace the original configuration file with ours!

### Rule-based NL Explanation Generation
[SQL2NL_clean.py](https://github.com/magic-YuanTian/STEPS/blob/main/SQL2NL_clean.py) includes a simple example to run:

```
from SQL2NL_clean import sql2nl
SQL = "SELECT * FROM STUDENT"  # input your SQL here
explanation_data = sql2nl(SQL)  # Generate explanation data
```

The `sql2nl` method automatically outputs the step-by-step explanation to console:

<img width="1079" alt="Screenshot 2024-06-10 at 11 03 39 AM" src="https://github.com/magic-YuanTian/STEPS/assets/75125334/282079ad-1f48-474c-bec1-f9e991a68be2">

The `explanation_data` includes the JSON format data:

```
[
    {'number': 'Start first query,', 'subquery': 'SELECT first_name , last_name FROM players GROUP BY birth_date HAVING COUNT ( * ) > 1 ORDER BY birth_date LIMIT 1', 'explanation': [
            {'subexpression': 'FROM players', 'explanation': 'In table players'
            },
            {'subexpression': 'GROUP BY birth_date', 'explanation': 'Group the records based on the birth date'
            },
            {'subexpression': 'HAVING COUNT ( * ) > 1', 'explanation': 'Keep the groups where the number of records is greater than 1'
            },
            {'subexpression': 'ORDER BY birth_date LIMIT 1', 'explanation': 'Sort the records in ascending order based on the birth date, and return the first record'
            },
            {'subexpression': 'SELECT first_name , last_name', 'explanation': 'Return the first name and the last name'
            }
        ], 'supplement': ''
    },
    {'number': 'Start second query,', 'subquery': 'SELECT first_name , last_name FROM players WHERE first_name = "TOM" GROUP BY birth_date HAVING COUNT ( * ) > 1 ORDER BY birth_date LIMIT 1', 'explanation': [
            {'subexpression': 'FROM players', 'explanation': 'In table players'
            },
            {'subexpression': 'WHERE first_name = "TOM"', 'explanation': 'Keep the records where the first name is "TOM"'
            },
            {'subexpression': 'GROUP BY birth_date', 'explanation': 'Group the records based on the birth date'
            },
            {'subexpression': 'HAVING COUNT ( * ) > 1', 'explanation': 'Keep the groups where the number of records is greater than 1'
            },
            {'subexpression': 'ORDER BY birth_date LIMIT 1', 'explanation': 'Sort the records in ascending order based on the birth date, and return the first record'
            },
            {'subexpression': 'SELECT first_name , last_name', 'explanation': 'Return the first name and the last name'
            }
        ], 'supplement': ''
    },
    {'number': 'Start third query,', 'subquery': 'SELECT first_name , last_name FROM players GROUP BY birth_date HAVING COUNT ( * ) > 1 ORDER BY birth_date LIMIT 1 INTERSECT SELECT first_name , last_name FROM players WHERE first_name = "TOM" GROUP BY birth_date HAVING COUNT ( * ) > 1 ORDER BY birth_date LIMIT 1', 'explanation': [
            {'subexpression': 'SELECT *', 'explanation': 'Keep the intersection of first query result and second query result.'
            }
        ], 'supplement': ''
    }
]
```



**To help you understand the project logic, we also encapsulate most of the project folder (This does not include the interface). You can directly download it to check the configuration. [Folder](https://purdue0-my.sharepoint.com/:u:/g/personal/tian211_purdue_edu/ESrKiv9auWpFpUdVL3pv5XUBYroue8a7OC6fmdMsXxdvaQ?e=Ct5W1V)**

*This repository is currently being updated, and more details will be provided in the future.*
*If you have any questions, please feel free to email* ***tian211@purdue.edu***

*Thanks!*
