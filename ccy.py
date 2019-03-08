# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 15:08:13 2019

@author: silvafm
"""
import pyodbc
import numpy as np


def get_dolar(date):

    # date = '20190226'
    # Conecta a servidor de Riesgo
    cnxn2 = pyodbc.connect('Driver={SQL Server};'
                           'Server=SRVREALSQLDEV01;'
                           'Database=Risk_chile;'
                           'Trusted_Connection=yes;')

    cursor = cnxn2.cursor()

    sel_query = "SELECT CCY_to_CLP FROM Risk_chile.dbo.EXCHANGE_RATES \
            WHERE 1=1 \
            AND CCY = 'USD' \
            AND DATE = ? "

    row = cursor.execute(sel_query, date).fetchone()

    cnxn2.close()

    return row[0]


def get_clf(date):

    # date = '20190226'
    # Conecta a servidor de Riesgo
    cnxn2 = pyodbc.connect('Driver={SQL Server};'
                           'Server=SRVREALSQLDEV01;'
                           'Database=Risk_chile;'
                           'Trusted_Connection=yes;')

    cursor = cnxn2.cursor()

    sel_query = "SELECT CCY_to_CLP FROM Risk_chile.dbo.EXCHANGE_RATES \
            WHERE 1=1 \
            AND CCY = 'CLF' \
            AND DATE = ? "

    row = cursor.execute(sel_query, date).fetchone()

    cnxn2.close()

    return row[0]


def get_eur(date):

    # date = '20190226'
    # Conecta a servidor de Riesgo
    cnxn2 = pyodbc.connect('Driver={SQL Server};'
                           'Server=SRVREALSQLDEV01;'
                           'Database=Risk_chile;'
                           'Trusted_Connection=yes;')

    cursor = cnxn2.cursor()

    sel_query = "SELECT CCY_to_CLP FROM Risk_chile.dbo.EXCHANGE_RATES \
            WHERE 1=1 \
            AND CCY = 'EUR' \
            AND DATE = ? "

    row = cursor.execute(sel_query, date).fetchone()

    cnxn2.close()

    return row[0]


def get_gbp(date):

    # date = '20190226'
    # Conecta a servidor de Riesgo
    cnxn2 = pyodbc.connect('Driver={SQL Server};'
                           'Server=SRVREALSQLDEV01;'
                           'Database=Risk_chile;'
                           'Trusted_Connection=yes;')

    cursor = cnxn2.cursor()

    sel_query = "SELECT CCY_to_CLP FROM Risk_chile.dbo.EXCHANGE_RATES \
            WHERE 1=1 \
            AND CCY = 'GBP' \
            AND DATE = ? "

    row = cursor.execute(sel_query, date).fetchone()

    cnxn2.close()

    return row[0]


def apply_usd_vl_nominal(c, date):

    conditions = [(c['DER_PAY_CCY'] == 'CLP'),
                  (c['DER_PAY_CCY'] == 'CLF'),
                  (c['DER_PAY_CCY'] == 'USD'),
                  (c['DER_PAY_CCY'] == 'GBP'),
                  (c['DER_PAY_CCY'] == 'EUR')]
    choices = [c['VL_NOMINAL']/get_dolar(date),
               c['VL_NOMINAL']*get_clf(date)/get_dolar(date),
               c['VL_NOMINAL'],
               c['VL_NOMINAL']*get_gbp(date)/get_dolar(date),
               c['VL_NOMINAL']*get_eur(date)/get_dolar(date)]

    return np.select(conditions, choices, default=0)


def apply_usd_asset_mtm(c, date):

    conditions = [(c['NDER_ASSET_MTM_OC'] != 0)]
    choices = [c['NDER_ASSET_MTM_OC']/get_dolar(date)]

    return np.select(conditions, choices, default=0)


def apply_pay_mtm_usd(c, date):

    conditions = [(c['DER_PAY_CCY'] == 'CLP'),
                  (c['DER_PAY_CCY'] == 'CLF'),
                  (c['DER_PAY_CCY'] == 'EUR'),
                  (c['DER_PAY_CCY'] == 'GBP'),
                  (c['DER_PAY_CCY'] == 'USD')]
    choices = [c['DER_PAY_MTM_OC']/get_dolar(date),
               c['DER_PAY_MTM_OC']*get_clf(date)/get_dolar(date),
               c['DER_PAY_MTM_OC']*get_eur(date)/get_dolar(date),
               c['DER_PAY_MTM_OC']*get_gbp(date)/get_dolar(date),
               c['DER_PAY_MTM_OC']]

    return np.select(conditions, choices, default=0)


def apply_rec_mtm_usd(c, date):

    conditions = [(c['DER_REC_CCY'] == 'CLP'),
                  (c['DER_REC_CCY'] == 'CLF'),
                  (c['DER_REC_CCY'] == 'EUR'),
                  (c['DER_REC_CCY'] == 'GBP'),
                  (c['DER_REC_CCY'] == 'USD')]
    choices = [c['DER_REC_MTM_OC']/get_dolar(date),
               c['DER_REC_MTM_OC']*get_clf(date)/get_dolar(date),
               c['DER_REC_MTM_OC']*get_eur(date)/get_dolar(date),
               c['DER_REC_MTM_OC']*get_gbp(date)/get_dolar(date),
               c['DER_REC_MTM_OC']]

    return np.select(conditions, choices, default=0)
