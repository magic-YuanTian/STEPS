import pyautogui
import json
import pyperclip
import os
import time
import random
import copy

results = []

def paraphraseUnit(ex, times):
    #
    # paraphrase unit
    #
    ori_explanation = ex["question"]

    # get the coordinate of paraphrase button, copy button
    time.sleep(0.1)

    # if no such image appears, keep doing it
    temp_counter = 0
    paraphrase_button_coordinates = pyautogui.locateOnScreen('img/paraphrase.png', confidence=0.8)
    while paraphrase_button_coordinates is None:
        temp_counter += 1
        # stuck, so refresh the page
        if temp_counter > 90:
            temp_counter = 0
            refresh_button_coordinates = pyautogui.locateOnScreen('img/refresh.png', confidence=0.8)
            x_refresh, y_refresh = pyautogui.center(refresh_button_coordinates)
            # time.sleep(2)
            pyautogui.leftClick(x_refresh, y_refresh)
            time.sleep(2)
            print("Try to detect paraphrase button one more time...(after refresh)")
            paraphrase_button_coordinates = pyautogui.locateOnScreen('img/paraphrase.png', confidence=0.8)
            x_para, y_para = pyautogui.center(paraphrase_button_coordinates)
            # copy the original explanation to the clipboard
            pyperclip.copy(ori_explanation)
            if paraphrase_button_coordinates is not None:
                pyautogui.leftClick(x_para - 100, y_para - 100)  # move to input area
            else:
                print("paraphrase button not detected")
            # ctrl+a, and delete all teh content in this area
            pyautogui.keyDown('ctrl')
            pyautogui.press('a')
            pyautogui.keyUp('ctrl')
            pyautogui.press('backspace')
            # ctrl+v paste original explanation
            pyautogui.keyDown('ctrl')
            pyautogui.press('v')
            pyautogui.keyUp('ctrl')

        print("Try to detect paraphrase button one more time...")
        paraphrase_button_coordinates = pyautogui.locateOnScreen('img/paraphrase.png', confidence=0.8)
    x_para, y_para = pyautogui.center(paraphrase_button_coordinates)

    # copy the original explanation to the clipboard
    pyperclip.copy(ori_explanation)

    if paraphrase_button_coordinates is not None:
        pyautogui.leftClick(x_para - 100, y_para - 100)  # move to input area
    else:
        print("paraphrase button not detected")

    # ctrl+a, and delete all teh content in this area
    pyautogui.keyDown('ctrl')
    pyautogui.press('a')
    pyautogui.keyUp('ctrl')
    pyautogui.press('backspace')

    # ctrl+v paste original explanation
    pyautogui.keyDown('ctrl')
    pyautogui.press('v')
    pyautogui.keyUp('ctrl')

    # paraphrase and get new data
    # repeat {times} times
    for i in range(times):

        # click the paraphrase button
        pyautogui.leftClick(x_para, y_para)

        time.sleep(0.1)  # wait after rephrase button is clicked

        # should wait until copy button appears (means the paraphrasing is finished)
        temp_counter = 0
        copy_button_coordinates = pyautogui.locateOnScreen('img/copy_button.png', confidence=0.8)
        while copy_button_coordinates is None:
            pyautogui.leftClick(x_para, y_para)
            temp_counter += 1
            # stuck, so refresh the page
            if temp_counter > 90:
                temp_counter = 0
                refresh_button_coordinates = pyautogui.locateOnScreen('img/refresh.png', confidence=0.8)
                while refresh_button_coordinates is None:
                    refresh_button_coordinates = pyautogui.locateOnScreen('img/refresh.png', confidence=0.8)
                    print("Trying to refresh ....")
                x_refresh, y_refresh = pyautogui.center(refresh_button_coordinates)
                pyautogui.leftClick(x_refresh, y_refresh)
                time.sleep(2)
                print("Try to detect paraphrase button one more time...(after refresh)")
                # paraphrase_button_coordinates = pyautogui.locateOnScreen('img/initial_paraphrase.png', confidence=0.8)
                # x_para, y_para = pyautogui.center(paraphrase_button_coordinates)
                # copy the original explanation to the clipboard
                pyperclip.copy(ori_explanation)
                pyautogui.leftClick(x_para - 100, y_para - 100)  # move to input area
                # ctrl+a, and delete all teh content in this area
                pyautogui.keyDown('ctrl')
                pyautogui.press('a')
                pyautogui.keyUp('ctrl')
                pyautogui.press('backspace')
                # ctrl+v paste original explanation
                pyautogui.keyDown('ctrl')
                pyautogui.press('v')
                pyautogui.keyUp('ctrl')
                # click the paraphrase button
                pyautogui.leftClick(x_para, y_para)

            print("Try to detect copy button one more time...")
            copy_button_coordinates = pyautogui.locateOnScreen('img/copy_button.png', confidence=0.8)
        x_copy, y_copy = pyautogui.center(copy_button_coordinates)

        # copy teh paraphrased text
        if copy_button_coordinates is not None:
            # click the copy button
            pyautogui.leftClick(x_copy, y_copy)

        new_explanation = pyperclip.paste()  # get the paraphrased explanation from the clipboard
        # update and insert the new data
        ex_temp = copy.deepcopy(ex) # prevent from changing ex
        ex_temp["question"] = new_explanation
        ex_temp["question_toks"] = new_explanation.split()
        results.append(ex_temp)
        print('original: ', ex["question"])
        print('paraphrased: ', new_explanation)

if __name__ == '__main__':

    # set some hyper-parameters
    pyautogui.PAUSE = 0.01  # pause time
    pyautogui.FAILSAFE = True # move the cursor to left up corner, and then the program ends (throw an exception)

    # print screen size (resolution)
    print("screen size: ", pyautogui.size())



    # open the data file, and load data
    input_path = "data/train_spider.json"
    input_file = open(input_path, "r")
    json_obj = json.load(input_file)
    input_file.close()

    # step 0: # retain the original template-based explanation
    for cnt, ex in enumerate(json_obj):
        ex["template explanation"] = ex["question"]

    # step 1: paraphrase using possible verb replacement
        replace_dict = {
            "Get": ['Find out', 'Find', 'Discover', 'Show', 'Show me', 'Determine', 'Demonstrate', 'Give me', 'Obtain',
                    'Select', 'Choose', 'Search', 'Display', 'List', 'Acquire', 'Gain'],

            "Keep the records that": ['Make sure', 'Make', 'Where', 'Filter the records that'],

            "greater than": ['more than', 'exceed', 'no less than', 'over', 'above', 'larger than', 'beyond',
                             'in excess of', 'transcend', 'surpass'],

            "less than": ['lower than', 'no more than', 'below', 'lesser', 'under', 'underneath', 'not so much as',
                          'beneath'],

            "based on": ['according to', 'in terms of', 'specified by', 'built on', 'established on'],

            "that has": ['associated with', 'connected to'],

            "distinct": ['different', 'disparate', 'distinctive', 'particular', 'diverse', 'dissimilar', 'unique'],

            "ascending": ['increasing', 'ascendant', 'growing', 'rising', 'soaring', 'climbing', 'mounting'],

            "descending": ['decreasing', 'descendant', 'falling', 'declining', 'dropping', 'lessening', 'diminishing'],

            "maximum": ['max', 'maximum', 'utmost', 'greatest', 'most', 'topmost', 'highest', 'top', 'largest', 'biggest'],

            "minimum": ['lowest', 'smallest', 'least', 'min', 'minimal', 'bottom', 'bottommost', 'lowermost'],

            "number of": ['amount of', 'quantity of', 'total of'],

            "all": ['each', 'every', 'any', 'whole', 'entire', 'totality'],

            "in the form of": ['appearing as', 'with the appearance of', 'in the shape of'],

            "Group": ['Group', 'Batch', 'Organize', 'Categorize', 'Classify', 'Arrange', 'Separate', 'Label', 'Tag', 'Mark',
                      'Pack', 'Collect', 'Assemble', 'Distribute', 'Combine', 'Gather', 'Merge', 'Put together', 'Index',
                      'Concentrate'],

            "Order": ['Order', 'Sort', 'Rank', 'Array', 'Line up', 'Sequence']
        }

    prob = 0.4
    for cnt, ex in enumerate(json_obj):
        new_exp = copy.deepcopy(ex["question"])
        for key in replace_dict.keys():
            # generate a random number
            rand_num = random.random()
            if rand_num > prob:
                # randomly pick one from the list
                replace_verb = random.choice(replace_dict[key])
                new_exp = new_exp.replace(key, replace_verb)
        # update the explanation
        json_obj[cnt]["question"] = new_exp
        print('original: ', ex["question"])
        print('replaced: ', new_exp)

    # step 2: paraphrase using wordnet

    # step 3: paraphrase using Quillbot
    # For each sentence, paraphrase multiple times
    # Creative: 2 times + Expand 3 times

    output_path = "data/output.json"

    results = []
    start_idx = 7900
    for cnt, ex in enumerate(json_obj):

        # jump to the start location
        if cnt <= start_idx:
            print(cnt)
            continue

        # handle suspicious detection
        suspicious_coordinates = pyautogui.locateOnScreen('img/understand.png', confidence=0.8)
        if suspicious_coordinates is not None:
            print("suspicious detection appears")
            # detect understand button
            understand_button_coordinates = pyautogui.locateOnScreen('img/understand.png', confidence=0.8)
            x_understand, y_understand = pyautogui.center(understand_button_coordinates)
            time.sleep(610) # sleep 10 min
            pyautogui.leftClick(x_understand, y_understand)
            time.sleep(60)

        print('-'*30 + ' ' + str(cnt) + ' / ' + str(len(json_obj)) + '-'*30)

        # 2 times for creative
        creative_button_coordinates = pyautogui.locateOnScreen('img/creative.png', confidence=0.6)
        x_creative, y_creative = pyautogui.center(creative_button_coordinates)
        pyautogui.leftClick(x_creative, y_creative)
        paraphraseUnit(ex, 3)


        # 3 times for Expand
        expand_button_coordinates = pyautogui.locateOnScreen('img/expand.png', confidence=0.6)
        x_expand, y_expand = pyautogui.center(expand_button_coordinates)
        pyautogui.leftClick(x_expand, y_expand)
        paraphraseUnit(ex, 4)

        # dynamically update the file (prevent interrupt)
        # next time the file will be reopen so the previous content will be overwritten
        if cnt % 50 == 0:
            out_file = open(output_path, "w")
            outputContent = json.dumps(results)
            out_file.write(outputContent)
            out_file.close()


    # write new data into the output file
    out_file = open(output_path, "w")
    outputContent = json.dumps(results)
    out_file.write(outputContent)

    out_file.close()



    print('Program ends')

