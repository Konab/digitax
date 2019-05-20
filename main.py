import numpy as np
import pandas as pd
import requests

import psycopg2


def get_departs_by_city(name):
	cursor.execute(f"select * from address a join departments d on a.dep_id = d.id where a.city='{name}'")
	records = cursor.fetchall()
	return records


if __nama__ == '__main__':
	structure = 'data/structure.csv'
	struc = pd.read_csv(structure, sep=';', encoding='utf-8', dayfirst=True)
	trans = {
		'Код территориального органа': 'code',
		'Полное наименование': 'full_name',
		'Сокращенное наименование': 'short_name',
		'Фамилия, имя, отчество руководителя': 'director',
		'Фамилия, имя, отчество заместителя руководителя': 'deputy_director',
		'Описание задач и функций': 'describe',
		'Фактический адрес': 'address',
		'Номера телефонов справочных служб': 'phone',
		'Факс': 'facs',
		'Адрес сайта': 'url',
		'ИНН': 'INN',
		'КПП': 'KPP',
		'Адрес электронной почты': 'email'
	}
	dict_struc = {}
	for i in range(len(struc)):
		dict_struc[struc['ID'][i]] = trans[' '.join(struc['Name'][i].split())]

	new_data = requests.get('http://data.nalog.ru/opendata/7707329152-regional_office/data-03012016-structure-11192014.csv')
	data = 'data/main_data.csv'
	with open(data, 'wb') as f:
		f.write(new_data.content)
	data = pd.read_csv(data, sep=';', encoding='utf-8', dayfirst=True)
	curr_data = []
	for i in range(len(data)):
		print('---')
		curr_string = {}
		for d in data:
			curr = ' '.join(str(data[d][i]).split())
			curr_string[dict_struc[d]] = curr if len(str(curr).split(',')) == 1 else str(curr).split(',')
		curr_data.append(curr_string)

	# Подключаемся к базе
	conn = psycopg2.connect(dbname='', user='', 
							password='', host='')
	cursor = conn.cursor()
	for curr in curr_data:
		cursor.execute(f"insert into departments (code, full_name, short_name, director, describe, phone, facs, url, INN, KPP, email) values ({curr['code']}, {curr['full_name']}, {curr['short_name']}, {curr['director']}, {curr['describe']}, {curr['phone']}, {curr['facs']}, {curr['url']}, {curr['INN']}, {curr['KPP']}, {curr['email']})")
		id = cursor.execute(f"select id from departments where full_name={curr['full_name']}")['id']
		for direct in curr['deputy_director']:
			cursor.execute(f"insert into deputy_director (dep_id, name) values ({id}, {direct})")
		cursor.execute(f"insert into address (dep_id, index, area, city, street, house) values ({id}, {curr['address'][0]}, {curr['address'][1]}, {curr['address'][2]}, {curr['address'][3]}, {curr['address'][4]})")
	cursor.close()
	conn.close()
