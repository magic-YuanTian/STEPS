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


### Models
1. Our text-to-clause model is based on [SmBoP](https://github.com/OhadRubin/SmBop), and you can strictly follow their environment and settings.
2. You can directly download and reuse our [check point](https://purdue0-my.sharepoint.com/personal/tian211_purdue_edu/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Ftian211%5Fpurdue%5Fedu%2FDocuments%2FSTEPS%2FmodelS%2Etar%2Egz&parent=%2Fpersonal%2Ftian211%5Fpurdue%5Fedu%2FDocuments%2FSTEPS) ([huggingface] link(https://huggingface.co/DoctorChaos/text-to-SQL-clause-smbop/blob/main/README.md)) as well as configuration file. 
  - Please replace the original configuration file with ours!


**To help you understand the project logic, we also encapsulate most of the project folder. You can directly download it to check the configuration. [Folder](https://purdue0-my.sharepoint.com/personal/tian211_purdue_edu/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Ftian211%5Fpurdue%5Fedu%2FDocuments%2FSTEPS%2FSTEPS%2Ezip&parent=%2Fpersonal%2Ftian211%5Fpurdue%5Fedu%2FDocuments%2FSTEPS&ct=1708191688229&or=OWA%2DNT&cid=2f4d10de%2Dfdb2%2D39ba%2Db806%2D6364779fc664&ga=1)**

*This repository is currently being updated, and more details will be provided in the future.*
*If you have any questions, please feel free to email* ***tian211@purdue.edu***

*Thanks!*
