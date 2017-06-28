#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/28 16:52
# @Author  : wutianxiong
# @File    : db.py
# @Software: PyCharm
from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta

TABLES = {}
TABLES['employees'] = (
    "CREATE TABLE `employees` ("
    "  `emp_no` int(11) NOT NULL AUTO_INCREMENT,"
    "  `birth_date` date NOT NULL,"
    "  `first_name` varchar(14) NOT NULL,"
    "  `last_name` varchar(16) NOT NULL,"
    "  `gender` enum('M','F') NOT NULL,"
    "  `hire_date` date NOT NULL,"
    "  PRIMARY KEY (`emp_no`)"
    ") ENGINE=InnoDB")


def connection():
    cnx = mysql.connector.connect(user='confluence', password='123456', host='202.115.44.159', database='test')
    return cnx


def create_table(tables, cursor):
    for name, ddl in tables.iteritems():
        try:
            print("Creating table {}: ".format(name), end=' ')
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")


def insert_data(cursor, add_query, data):
    cursor.execute(add_query, data)
    cursor.close()


add_employee = ("INSERT INTO employees "
                "(first_name, last_name, hire_date, gender, birth_date) "
                "VALUES (%s, %s, %s, %s, %s)")

tomorrow = datetime.now().date() + timedelta(days=1)

data_employee = ('Geert', 'Vanderkelen', tomorrow, 'M', date(1977, 6, 14))

if __name__ == "__main__":
    cnx = connection()
    insert_data(cnx.cursor(), add_employee, data_employee)
    cnx.commit()
    cnx.close
