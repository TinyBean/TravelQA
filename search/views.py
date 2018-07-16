# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.http import HttpResponse
from django.template import RequestContext

from django.shortcuts import render

import requests
from Queue import Queue
import urllib
try:
    import simplejson as json
except:
    import json
from collections import defaultdict
import jieba
import jieba.posseg as psg
import re
import cPickle
import build_dict

attr_map = build_dict.load_attr_map("./search/data/attr_map.txt")
attr_ac = cPickle.load(open("./search/data/attr_ac.pkl","rb"))
ent_dict = build_dict.load_entity_dict("./search/data/data_entity.txt")

def home(request):
    return render(request, "home.html", {})

def search(request):
    question = request.GET['question']
    lf_question = translate_NL2LF(question)
    answer, msg, query_type = _parse_query(lf_question)
    # answer, msg, query_type = _parse_query(question)
    if msg == 'done':
        if query_type == 1:
            return render(request, "entity.html", {"question":question, "ans":answer})
        elif query_type == 4:
            return render(request, "entity_list.html", {"question":question, "ans":answer})
        elif query_type == 3:
            if isinstance(answer, int):
                answer = str(answer)
            return render(request, "message.html", {"question":question, "ans":answer})
    elif msg == 'none':
        return render(request, "message.html", {"question":question, "ans":"find nothing"})
    else:
        return render(request, "message.html", {"question":question, "ans":answer + " " + msg})

def _parse_query(question):
    answer, query_type = "", None
    question = question.replace(" ","")
    parts = re.split("：|:", question)
    en = _entity_linking(parts[0])
    if len(parts) < 2:
        if len(en):
            query_type = 1
            answer,msg = _search_single_subj(en[-1])
        else:
            return question, '未识别到实体',-1
    elif 'AND' in question or 'OR' in question:
        query_type = 4
        bool_ops = re.findall('AND|OR',question)
        exps = re.split('AND|OR',question)       
        answer,msg = _search_multi_PO(exps, bool_ops)
        # answer = '#'.join(answer)
    elif len(_map_predicate(parts[0])) != 0:
        query_type = 4
        answer, msg = _search_multi_PO([question],[])    
    elif len(en):
        query_type = 3
        answer, msg = _search_multihop_SP(parts)
    else:
        msg = '未识别到实体或属性: ' + parts[0]

    return answer, msg, query_type

def _search_multihop_SP(parts):
    has_done = parts[0]
    v = parts[0]
    for i in range(1, len(parts)):
        en = _entity_linking(v)
        if not len(en):
            return '执行到: ' + has_done, '==> 对应的结果为:' + v + ', 知识库中没有该实体: ' + v
        card, _ = _search_single_subj(en[0])
        p = _map_predicate(parts[i])
        if not len(p):
            return '执行到: ' + has_done, '==> 知识库中没有该属性: ' + parts[i]
        p = p[0]
        if p not in card:
            return '执行到: ' + has_done, '==> 实体 ' + card['subj'] + ' 没有属性 ' + p
        v = card[p]
        if isinstance(v,int):
            v = str(v)
        has_done += ":" + parts[i]
    return v, 'done'

def _search_multi_PO(exps, bool_ops):
    #解析逻辑表达式，构建查询语句
    at_list = []
    po_list = []

    for e in exps:
        if e == "":
            return "", 'AND 或 OR 后不能为空'

        if e[0:3] == 'NOT':
            e = e[3:]
        elif 'NOT' in e:
            return e, 'NOT请放在PO对前面'

        op = re.findall("：|:",e)
        if len(op) != 1:
            return e, '语法错误'
        op = op[0]
        pred, obj = e.split(op)
        at_list.append(pred)
        po_list.append(obj)

    should_list = {}
    must_list = {}
    temp_at = at_list[0]
    temp_po = po_list[0]
    if bool_ops:
        for i in range(len(bool_ops)):
            if bool_ops[i] == 'OR':
                if at_list[i] == at_list[i+1]:
                    temp_po = temp_po + ' ' + po_list[i+1]
                    continue

                should_list[temp_at] = temp_po
                temp_at = at_list[i+1]
                temp_po = po_list[i+1]
            elif bool_ops[i] == 'AND':
                must_list[at_list[i+1]] = None
                must_list[temp_at] = temp_po
                temp_at = at_list[i+1]
                temp_po = po_list[i+1]
        if len(must_list) == 0:
            must_list[temp_at] = temp_po
        if temp_at in must_list:
            must_list[temp_at] = temp_po
        else:
            should_list[temp_at] = temp_po
    else:
        must_list[temp_at] = temp_po

    query = '{"query":{"bool":{'
    if must_list:
        i = 1
        query += '"must":['
        for m in must_list:
            query += '{"match":{"' + m + '":"' + must_list[m] + '"}}'
            if i < len(must_list):
                query += ','
            i += 1
        query += ']'
        
        if should_list:
            j = 1
            query += ',"should":['
            for s in should_list:
                query += '{"match":{"' + s + '":"' + should_list[s] + '"}}'
                if j < len(should_list):
                    query += ','
                j += 1
            query += ']'
    query += '}}}'
    
    query = query.encode('utf-8')
    print query
    payload = {"Content-Type":"application/json"}
    response = requests.get("http://localhost:9200/travel/view/_search", data = query, headers = payload)
    res = json.loads(response.content)

    if res['hits']['total'] == 0:
        return None,'none'
    else:
        ans = {}
        for e in res['hits']['hits']:
            name = e['_source']['subj']
            ans[name] = "/search?question="+name

        return ans, 'done'
    #return query.decode('utf-8'), 'done'


def _search_single_subj(entity_name):
    #按实体名查询实体所有属性
    query = json.dumps({"query": { "bool":{"filter":{"term" :{"subj" : entity_name}}}}})
    payload = {"Content-Type":"application/json"}
    response = requests.get("http://localhost:9200/travel/view/_search", data = query.encode('utf-8'), headers = payload)
    res = json.loads(response.content)

    if res['hits']['total'] == 0:
        return None, 'entity'
    else:
        card = dict()
        card['subj'] = entity_name
        s = res['hits']['hits'][0]['_source']
        if 'location' in s:
            card['location'] = s['location']
        if 'grade' in s:
            card['grade'] = s['grade']
        for po in s['po']:
            if po['pred'] in card:
                card[po['pred']] += ' ' + po['obj']
            else:
                card[po['pred']] = po['obj']
        return card, 'done'

def _search_single_subj_pred_pair(entity_name, attr_name):
    #按实体和属性名查询属性值
    query = '{"query": {"constant_score": {"filter": {"bool": {"must": {"term": {"pred": "' + \
        attr_name + '"}},"must":{"term":{"subj":"' + entity_name + '"}}}}}}}'
    query = query.encode('utf-8')
    payload = {"Content-Type":"application/json"}
    response = requests.get("http://localhost:9200/travel/view/_search", data = query, headers = payload)
    res = json.loads(response.content)

    if res['hits']['total'] == 0:
        ans, _ = _search_single_subj(entity_name)
        return ans, 'str'
    else:
        obj = res['hits']['hits'][0]['_source']['obj']
        # obj_en, _ = _search_single_subj(obj)
        # if obj_en is not None:
        #     return obj_en, 'entity'
        # else:
        return obj, 'str'

def translate_NL2LF(nl_query):
    '''
    使用基于模板的方法将自然语言查询转化为logic form
    '''
    entity_list = _entity_linking(nl_query)
    attr_list = _map_predicate(nl_query)
    lf_query = ""
    if entity_list: #找到实体名
        if not attr_list:
            lf_query = entity_list[0]
        else:
            if len(attr_list) == 1:
                lf_query = "{}:{}".format(entity_list[0], attr_list[0])
            else:
                lf_query = entity_list[0]
                for pred in attr_list:
                    lf_query += ":" + pred
    else: #找不到实体名，按照属性值查询
        val_d = _val_linking(nl_query)
        
        attr_pos = {}
        val_pos = {}
        for a in attr_list:
            attr_pos[a] = nl_query.find(a)
        for v in val_d:
            val_pos[v] = nl_query.find(v)
        retain_attr = []
        for a in attr_pos:
            to_retain = True
            for v in val_pos: #实体名与属性名一一对应
                if (attr_pos[a] >= val_pos[v] and attr_pos[a] + len(a) <= val_pos[v] + len(v)) or \
                    (val_d[v] == a and attr_pos[a] + len(a) >= val_pos[v] - 2):
                    to_retain = False
                    break
            if to_retain: #对应不了的记录
                retain_attr.append(a)
        tmp = {}
        for v in val_pos:
            to_retain = True
            for a in attr_pos:
                if(val_pos[v] >= attr_pos[a] and val_pos[v] + len(v) <= attr_pos[a] + len(a)):
                    to_retain = False
                    break
            if to_retain:
                tmp[v] = val_d[v]
        val_d = tmp

        final_val_d= {}
        for v in val_d:
            if not (v.isdigit() or v in '大于' or v in '小于'):
                final_val_d[v] = val_d[v]

        prev_pred = []
        for v in final_val_d:
            pred = final_val_d[v]
            if pred in prev_pred:
                lf_query += ' OR ' + '{}:{}'.format(pred, v)
            else:
                if not lf_query:
                    lf_query = '{}:{}'.format(pred, v)
                else:
                    lf_query += ' AND ' + '{}:{}'.format(pred, v)
                prev_pred.append(pred)
    return lf_query

def _remove_dup(word_list):
    '''
    args:
        word_list: 一个字符串的list
    '''
    distinct_word_list = []
    for i in range(len(word_list)):
        is_dup = False
        for j in range(len(word_list)):
            if j != i and word_list[i] in word_list[j]:
                is_dup = True
                break
        if not is_dup:
            distinct_word_list.append(word_list[i])
    return distinct_word_list


def _map_predicate(pred_name, map_attr=True):   #找出一个字符串中是否包含知识库中的属性

    def _map_attr(word_list):
        ans = []
        for word in word_list:
            ans.append(attr_map[word.encode('utf-8')][0].decode('utf-8'))
        return ans

    match = []
    for w in attr_ac.iter(pred_name.encode('utf-8')):
        match.append(w[1][1].decode('utf-8'))
    if not len(match):
        return []

    ans = _remove_dup(match)
    if map_attr:
        ans = _map_attr(ans)
    return ans

def _generate_ngram_word(word_list_gen):
    '''
    args:
        word_list_gen: 一个字符串的迭代器
    '''
    word_list = []
    for w in word_list_gen:
        word_list.append(w)
    n = len(word_list)
    ans = []
    for i in range(1, n+1):
        for j in range(0,n+1-i):
            ans.append(''.join(word_list[j:j+i]))
    return ans

def _entity_linking(entity_name):    #找出一个字符串中是否包含知识库中的实体，这里是字典匹配，可以用检索代替
    parts = re.split(r'的|是|有', entity_name)
    ans = []
    for p in parts:
        pp = jieba.cut(p)
        if pp is not None:
            for phrase in _generate_ngram_word(pp):
                if phrase.encode('utf-8') in ent_dict:
                    ans.append(phrase)
    return ans

def _val_linking(nl_query):
    parts = psg.cut(nl_query)
    ans = {}
    for word, tag in parts:
        word = word.upper()
        if tag.startswith('n') or tag.startswith('e'):
            trs = _val_query(word.encode('utf-8'))
            if trs == 'grade':
                if word.startswith('A'):
                    ans[word] = trs
                else:
                    continue
            ans[word] = trs
            

    return ans

def _val_query(word):
    query_l = '{"query":{"match":{"location":"' + word.decode('utf-8') + '"}}}'
    query_g = '{"query":{"match":{"grade":"' + word.decode('utf-8') + '"}}}'
    payload = {"Content-Type":"application/json"}
    res_l = requests.get("http://localhost:9200/travel/view/_search", data = query_l.encode('utf-8'), headers = payload)
    res_g = requests.get("http://localhost:9200/travel/view/_search", data = query_g.encode('utf-8'), headers = payload)
    r1 = json.loads(res_l.content)
    r2 = json.loads(res_g.content)
    num_l = r1['hits']['total']
    num_g = r2['hits']['total']
    if num_l or num_g:
        if num_l >= num_g:
            return 'location'
        else:
            return 'grade'
    return None
