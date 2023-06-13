import pyautogui
import json
import pyperclip
import clipboard
import random

# pyautogui.PAUSE = 0.01  # pause time
# pyautogui.FAILSAFE = True  # move the cursor to left up corner, and then the program ends (throw an exception)
#
# paraphrase_button_coordinates = pyautogui.locateOnScreen('img/copy_button.png', confidence=0.8)
# x_para, y_para = pyautogui.center(paraphrase_button_coordinates)
# print(x_para)
# print(y_para)

# while True:
#     print(pyautogui.position())


# aaa = 'hello world'
# clipboard.copy(aaa)
# bbb = clipboard.paste()
# print(bbb)

# for i in range(100):
#     print(random.random())


input_path = "data/output.json"
input_file = open(input_path, "r+")
json_obj = json.load(input_file)
input_file.close()

new_json_obj = json_obj[:12320]

output_path = "data/output2.json"
out_file = open(output_path, "w")
outputContent = json.dumps(new_json_obj)
out_file.write(outputContent)

out_file.close()



# number = 12320
for cnt, ex in enumerate(new_json_obj):
    if cnt % 7 == 0:
        print('\n\n')
        print('original:')
        print(ex['template explanation'])
        print('\n')

    # print('paraphrased:')
    print(ex['question'])