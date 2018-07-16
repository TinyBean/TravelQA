#coding:utf-8
from __future__ import unicode_literals
try:
	import simplejson as json
except:
	import json
import os
import re
from collections import defaultdict
import sys 
reload(sys) 
sys.setdefaultencoding('utf-8')

def transform_triple2json(input_file):
	'''
	将三元组转化成json,并且记录entity列表和attribute列表
	一个三元组对应个一个文档
	'''	
	# dirname = os.path.dirname(input_file)
	# basename = os.path.basename(input_file)
	# out_name = basename[:basename.rfind(".")]

	f_input = open(input_file)
	f_ent = open("./data/data_entity.txt","w")
	f_attr = open("./data/data_attr.txt","w")
	f_json = open("./data/data.json","w")

	attr_dict = dict()
	entity_dict = dict()
	cnt = 0
	for line in f_input:
		parts = line.strip().split(",")
		entity = parts[0]
		attr = parts[1]
		attr_vals = parts[2]

		entity_dict[entity] = 1
		attr_dict[attr] = 1		

		# for val in attr_vals:
		new_doc = dict()
		new_doc['subj'] = entity
		new_doc['pred'] = attr
		new_doc['obj'] = attr_vals

		new_doc_j = json.dumps(new_doc, ensure_ascii=False)
		f_json.write(new_doc_j + "\n")

		cnt += 1
		if not (cnt % 10000):
			print cnt

	for en in entity_dict:
		f_ent.write(en + "\n")
	for at in attr_dict:
		f_attr.write(at + "\n")

def transform_entity2json(input_file):
	'''
	一个entity的所有属性为一个文档
	height,weight由于要支持range搜索，需要另存为int类型，要单独考虑
	'''

	f_input = open(input_file)
	f_json = open("./data/data.json","w")


	last = None
	new_ent = {'po':[]}
	for line in f_input:
		parts = line.strip().split(",")
		entity = parts[0]
		attr = parts[1]
		v = parts[2]
		if last is None:
			last = entity		

		if last is not None and entity != last:
			new_ent['subj'] = last.decode('utf-8')
			new_ent_j = json.dumps(new_ent, ensure_ascii=False)
			f_json.write(new_ent_j + "\n")
			last = entity
			new_ent = {}
			new_ent['po'] = []

		if attr == 'location':
			new_ent['location'] = v.decode('utf-8')
		elif attr == 'grade':
			new_ent['grade'] = v.decode('utf-8')
		else:
			new_ent['po'].append({'pred':attr.decode('utf-8'),"obj":v.decode('utf-8')})

	new_ent['subj'] = last.decode('utf-8')
	new_ent_j = json.dumps(new_ent, ensure_ascii=False)
	f_json.write(new_ent_j + "\n")

def clean_data(file):
	f = open(file)
	f_new = open('./data/data_new.csv','wb')

	pre_en = '八大处公园'
	pre_ped = 'name'
	for line in f:
		parts = line.strip().split(',')
		if parts[0] != pre_en or parts[1] != pre_ped:
			f_new.write(','.join(parts) + '\n')
			pre_en = parts[0]
			pre_ped = parts[1]

if __name__ == '__main__':
	# transform_triple2json("../data/Person.txt")
	# transform_triple2json("../data/Org.txt")
	# transform_triple2json("../data/Place.txt")

	#transform_triple2json("./data/data_e.csv")
	transform_entity2json('./data/data_new.csv')