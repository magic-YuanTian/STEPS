# test shell for explanation->subexpression

import os
import sys
import sqlite3
from json import *
import json
# import datetime, pytimeparse, time
import datetime, time
from backend.SQL2NL.SQL2NL import *
from pandas import DataFrame
import Levenshtein

import nltk
import pathlib
import gdown
import argparse
import torch
from backend.SmBop import *

from allennlp.models.archival import Archive, load_archive, archive_model
from allennlp.data.vocabulary import Vocabulary
from backend.SmBop.smbop.modules.relation_transformer import *
from allennlp.common import Params
from backend.SmBop.smbop.models.smbop import SmbopParser
from backend.SmBop.smbop.modules.lxmert import LxmertCrossAttentionLayer
from backend.SmBop.smbop.dataset_readers.spider import SmbopSpiderDatasetReader
import itertools
import backend.SmBop.smbop.utils.node_util as node_util
import numpy as np
from allennlp.models import Model
from allennlp.common.params import *
from allennlp.data import DatasetReader, Instance
import tqdm
from allennlp.predictors import Predictor
import json
from backend.SQL2NL.explanation2subexpression import *

# here load structured SmBop
from backend.structuredSmBop import run_structure



# pathlib.Path(f"cache").mkdir(exist_o       k=True)
# url = 'https://drive.google.com/u/0/uc?id=1pQvg2sT7h9t_srgmN1nGGMfIPa62U9ag'
# output = 'model.tar.gz'
# gdown.download(url, output, quiet=False)
# url = 'https://drive.google.com/uc?export=download&id=1_AckYkinAnhqmRQtGsQgUKAnTHxxX5J0'
# output = 'spider.zip'
# gdown.download(url, output, quiet=False)

def editDistance(gold_sql, pred_sql):

    # preprocess
    gold_sql = gold_sql.lower()
    pred_sql = pred_sql.lower()

    # merge a single operators
    gold_sql = re.sub('> +=', '>=', gold_sql)
    gold_sql = re.sub('< +=', '<=', gold_sql)
    gold_sql = re.sub('! +=', '!=', gold_sql)

    pred_sql = re.sub('> +=', '>=', pred_sql)
    pred_sql = re.sub('< +=', '<=', pred_sql)
    pred_sql = re.sub('! +=', '!=', pred_sql)

    tokL1 = gold_sql.split()
    tokL2 = pred_sql.split()

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

    pred_sql = pred_sql.replace('\'', '')
    pred_sql = pred_sql.replace('\"', '')
    gold_sql = gold_sql.replace('\'', '')
    gold_sql = gold_sql.replace('\"', '')

    result = Levenshtein.distance(pred_sql, gold_sql)

    return result


def inference(question, db_id):
    instance = predictor._dataset_reader.text_to_instance(
        utterance=question, db_id=db_id,
    )

    predictor._dataset_reader.apply_token_indexers(instance)

    with torch.cuda.amp.autocast(enabled=True):
        out = predictor._model.forward_on_instances(
            # [instance, instance_0]
            [instance]
        )
        return out[0]["sql_list"]


os.chdir("backend/SmBop")
print('Starting...')

overrides = {
    "dataset_reader": {
        "tables_file": "dataset/tables.json",
        "dataset_path": "dataset/database",
    },
    "validation_dataset_reader": {
        "tables_file": "dataset/tables.json",
        "dataset_path": "dataset/database",
    }
}
predictor = Predictor.from_path(
    "pretrained_original_model.tar.gz", cuda_device=0, overrides=overrides
)

instance_0 = predictor._dataset_reader.text_to_instance(
    utterance="asds", db_id="aircraft"
)
predictor._dataset_reader.apply_token_indexers(instance_0)

os.chdir("../..")

print(inference("Get the name?","cinema"))

if __name__ == "__main__":

    instruction = 'paraphrased_match'

    if instruction == 'shell':
        while True:
            # dbid = input("\nplease input database id > ")
            dbid = 'department_management'
            question = input("\nplease input the explanation > ")
            try:
                subexpression = inference(question, dbid)
                print("subexpression: " + subexpression)
                pred = remove_select_from_for_structured_explanation(subexpression)
                print("pred: " + pred)
            except Exception as e:
                print(e)
                print("check the database id")

    # test the accuracy of model and save it to an excel
    elif instruction == 'save':
        print(os.getcwd())
        path = "backend/SQL2NL/dataset/spider/dev.json"
        data_list = getStructuredData(path)

        dbid_l = []
        exp_l = []
        sub_l = []
        pred_ori_l = []


        pred_l = []
        category_match_l = []
        sequential_match_l = []
        match_score_l = []

        cnt = 0
        matched = 0  # category matched
        sequential_match_total_score = 0
        general_match_total_score = 0

        for p in range(0, len(data_list)):
            # for p in range(118, 121):

            print("----------- ", p, '-------------')
            dt = data_list[p]

            try:
                pred1 = ''
                pred = ''
                pred1 = inference(dt['explanation'], dt['dbid'])
                pred = remove_select_from_for_structured_explanation(pred1)
            except:
                print('error in inference!!')
                print('\nexplanation: ', dt['explanation'])
                print('correct: ', dt['subexpression'])
                print('ori: ' + pred1)
                print('postprocess: ' + pred)
                continue

            match_bit = categoryMatch(dt['subexpression'], pred)
            temp_seq_matched = sequentialMatch(dt['subexpression'], pred)
            temp_general_matched = matchScore(dt['subexpression'], pred)
            cnt += 1
            if match_bit == 1:
                matched += 1
            sequential_match_total_score += temp_seq_matched
            general_match_total_score += temp_general_matched

            # updata data list
            dbid_l.append(dt['dbid'])
            exp_l.append(dt['explanation'])
            sub_l.append(dt['subexpression'])
            pred_ori_l.append(pred1)
            pred_l.append(pred)
            category_match_l.append(match_bit)
            sequential_match_l.append(temp_seq_matched)
            match_score_l.append(temp_general_matched)

        csv_data = {}
        csv_data['Database id'] = dbid_l
        csv_data['Explanation'] = exp_l
        csv_data['True subexpression'] = sub_l
        csv_data['Raw predicted subexpression'] = pred_ori_l
        csv_data['Processed Predicted subexpression'] = pred_l
        csv_data['categroy match'] = category_match_l
        csv_data['sequential match'] = sequential_match_l
        csv_data['matching score'] = match_score_l

        df = DataFrame(csv_data)
        df.to_excel("sturctured_csv_data.xls")

        print('number: ', cnt)
        print('matched: ', matched)
        print("total sequential matched score: ", sequential_match_total_score)
        print("total general matched score: ", general_match_total_score)

    # test the accuracy for exp2sub expert system
    elif instruction == 'expert':
        print(os.getcwd())
        path = "backend/SQL2NL/dataset/spider/dev.json"
        data_list = getStructuredData(path)

        dbid_l = []
        exp_l = []
        sub_l = []
        pred_ori_l = []
        pred_l = []
        category_match_l = []
        sequential_match_l = []
        match_score_l = []

        cnt = 0
        matched = 0  # category matched
        sequential_match_total_score = 0
        general_match_total_score = 0

        os.chdir("backend/SQL2NL")

        for p in range(0, len(data_list)):
            # for p in range(118, 121):
            dt = data_list[p]

            # don't calculating when no such db
            db_id_list = os.listdir('../../DBjson/')

            if dt['dbid'] not in db_id_list:
                continue

            try:
                pred1 = ''
                pred = ''
                pred1 = exp2subexpression(dt['explanation'], dt['dbid'])
                pred1 = pred1.lower()
                pred1 = removeTables(pred1)
                pred = pred1
                print('pred1:\n', pred1 )
            except:
                print('error in inference!!')
                print('\nexplanation: ', dt['explanation'])
                print('correct: ', dt['subexpression'])
                print('ori: ' + pred1)
                print('postprocess: ' + pred)
                continue

            try:
                match_bit = categoryMatch(dt['subexpression'].lower(), pred)
                temp_seq_matched = sequentialMatch(dt['subexpression'].lower(), pred)
                temp_general_matched = matchScore(dt['subexpression'].lower(), pred)
            except Exception as e:
                print(e)


            cnt += 1
            if match_bit == 1:
                matched += 1
            sequential_match_total_score += temp_seq_matched
            general_match_total_score += temp_general_matched

            # updata data list
            dbid_l.append(dt['dbid'])
            exp_l.append(dt['explanation'])
            sub_l.append(dt['subexpression'])
            pred_ori_l.append(pred1)
            pred_l.append(pred)
            category_match_l.append(match_bit)
            sequential_match_l.append(temp_seq_matched)
            match_score_l.append(temp_general_matched)

        os.chdir("../..")

        csv_data = {}
        csv_data['Database id'] = dbid_l
        csv_data['Explanation'] = exp_l
        csv_data['True subexpression'] = sub_l
        csv_data['Raw predicted subexpression'] = pred_ori_l
        csv_data['Processed Predicted subexpression'] = pred_l
        csv_data['categroy match'] = category_match_l
        csv_data['sequential match'] = sequential_match_l
        csv_data['matching score'] = match_score_l

        df = DataFrame(csv_data)
        df.to_excel("sturctured_csv_data1.xls")

        print('number: ', cnt)
        print('matched: ', matched)
        print("total sequential matched score: ", sequential_match_total_score)
        print("total general matched score: ", general_match_total_score)

    # for paraphrased data
    elif instruction == 'paraphrased_match':

        input_path = "backend/SQL2NL/dataset/spider/paraphrased/dev_paraphrased.json"
        input_file = open(input_path, "r")
        json_obj = json.load(input_file)
        input_file.close()

        data = {}

        dbid_list = []
        subexpression_list = []
        template_explanation_list = []
        paraphrased_explanation_list = []
        prediction_list = []

        category_match_list = []
        sequential_match_list = []
        match_score_list = []

        # some global counter numbers
        counter = 0
        matched = 0  # category matched
        sequential_match_total_score = 0
        general_match_total_score = 0

        for cnt, ex in enumerate(json_obj):
            print(cnt)
            # inference
            pred = run_structure.inference_structure(ex['question'], ex['db_id'])

            # calculate the matching score
            try:
                match_bit = categoryMatch(ex['query'].lower(), pred)
                temp_seq_matched = sequentialMatch(ex['query'].lower(), pred)
                temp_general_matched = matchScore(ex['query'].lower(), pred)
            except Exception as e:
                print(e)

            # update total scores
            counter += 1
            if match_bit == 1:
                matched += 1
            sequential_match_total_score += temp_seq_matched
            general_match_total_score += temp_general_matched

            # add new records to the data list
            dbid_list.append(ex['db_id'])
            subexpression_list.append(ex['query'])
            template_explanation_list.append(ex['template explanation'])
            paraphrased_explanation_list.append(ex['question'])
            prediction_list.append(pred)
            category_match_list.append(match_bit)
            sequential_match_list.append(temp_seq_matched)
            match_score_list.append(temp_general_matched)



        data['DB id'] = dbid_list
        data['SQL subexpression'] = subexpression_list
        data['Template explanation'] = template_explanation_list
        data['Pharaphrased explanation'] = paraphrased_explanation_list
        data['Predicted subexpression'] = prediction_list
        data['categroy match'] = category_match_list
        data['sequential match'] = sequential_match_list
        data['matching score'] = match_score_list

        df = DataFrame(data)
        df.to_excel("paraphrased_result_csv.xls")

        print('number: ', counter)
        print('matched: ', matched)
        print("total sequential matched score: ", sequential_match_total_score)
        print("total general matched score: ", general_match_total_score)

    elif instruction == 'match_original':

        input_path = "backend/SQL2NL/dataset/spider/train_spider.json"
        input_file = open(input_path, "r")
        json_obj = json.load(input_file)
        input_file.close()

        data = {}

        dbid_list = []
        question_list = []
        subexpression_list = []
        prediction_list = []
        edit_distance_list = []


        for cnt, ex in enumerate(json_obj):
            print(cnt)
            # inference
            pred = inference(ex['question'], ex['db_id'])

            # add new records to the data list
            dbid_list.append(ex['db_id'])
            question_list.append(ex['question'])
            subexpression_list.append(ex['query'])
            prediction_list.append(pred)
            edit_distance_list.append(editDistance(pred, ex['query']))


        data['DB id'] = dbid_list
        data['Question'] = question_list
        data['SQL'] = subexpression_list
        data['Predicted subexpression'] = prediction_list
        data['edit distance'] = edit_distance_list


        df = DataFrame(data)
        df.to_excel("original_result_csv.xls")


