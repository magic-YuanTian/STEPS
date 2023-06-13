# STEPS
Interactive SQL generation via editable step-by-step explanation


### Dataset
1. Download the [original Spider dataset](https://drive.google.com/uc?export=download&id=1TqleXec_OykOYFREKKtschzY29dUcVAQ)
2. Generate text to clause dataset using SQL2NL/SQL2NL.py
  - instruction = 'trainingData'.
  - You can design your own the explanation template within the method "parseSQL()".
  - python SQL2NL.py and you will get the dataset under "dataset/structured/spider/train_spider.json".
  - You could also download [our raw (unparaphrased) text-to-clause dataset](). Please put it in the same directory of the original Spider dataset and inlcude all the databases (For more information, please refer to https://github.com/taoyds/spider)
3. Paraphrase the text-to-clause dataset (optional)
  - You could paraphrase the dataset by [Quillbot](https://quillbot.com/) with our automated script based on [PyAutoGUI](https://pyautogui.readthedocs.io/en/latest/).
  - Please check the [script](Pharaphrase/main.py) and all screenshots under [here](Pharaphrase/img). These screenshots are used to position the cursor during the automation. Due to subtle resolution/theme/version difference, the screenshots may not be identifid on your computer (even if human can), you may need to manually take your screenshots on your computer and replace them.
