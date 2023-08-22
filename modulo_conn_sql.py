# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 14:16:18 2020

@author: jsdelgadoc
"""

import pyodbc

class ConexionSQL():
    
    def __init__(self):
        
        self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      'SERVER=COOCCAPP11A;'
                      'DATABASE=BI_Tableau;'
                      'UID=usertableau;'
                      'PWD=usertableau$')

        self.cursor = self.conn.cursor()
        

    def getCursor(self):
        return self.cursor
        