from __future__ import generators
import cx_Oracle
import json
#jdbc:oracle:thin:@tjptbc87.ge.ptlocal:1527:EAILGPRD

def ResultIter(cursor, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

FIELDS = [\
'CTRL_ID',\
'NPU',\
'DOMAIN',\
'TYPE',\
'NAME',\
'OPERATION',\
'VERSION',\
'SYSTEMCODE',\
#'STEP',\
'RETRY',\
#'SEQUENCE',\
#'ORIGIN',\
'COMPONENT',\
'ENGINE',\
#'PROCESSNAME',\
#'PROCESSID',\
'START_TIMESTAMP',\
'END_TIMESTAMP',\
'EXECUTION_TIME',\
'WAITING_TIME',\
'ECODE',\
#'EDESCRIPTION',\
'NCODE',\
#'NDESCRIPTION',\
'ISERROR',\
'HOST',\
#'CORRELATION_NPU',\
#'ENGINEINSTACEID',\
#'TIMESTAMP_LOG',\
'TRANSPORT_TIME'\
]

#print 'select ' + ",".join(FIELDS) + ' from "ETIBLOGRP"."LOG_MESSAGE_CONTROL" where START_TIMESTAMP between to_date(:range_start) and to_date(:range_end) and rownum < :maxrows';
#sql = "select * from "ETIBLOGRP"."LOG_MESSAGE_CONTROL" PARTITION FOR (to_date('2014-09-08', 'YYYY-MM-DD')) where START_TIMESTAMP between to_date('08-SEP-14') and to_date('09-SEP-14')"
date_start = '2014-09-08';
date_end   = '2014-09-09';
con = cx_Oracle.connect('xpta310/passwordzinha_ou_sera_zita@tjptbc87.ge.ptlocal:1527/EAILGPRD')
print con.version
cur = con.cursor()
cur.prepare('select ' + ",".join(FIELDS) + ' from "ETIBLOGRP"."LOG_MESSAGE_CONTROL" PARTITION FOR (to_date(\'' + date_start + '\', \'YYYY-MM-DD\')) where START_TIMESTAMP between to_date(:range_start, \'YYYY-MM-DD\') and to_date(:range_end, \'YYYY-MM-DD\')')
cur.execute(None, {'range_start' : date_start, 'range_end' : date_end})
#res = cur.fetchall()
for row in ResultIter(cur):
  col = 0
  jsondict = {}
  for field in FIELDS:
    jsondict[field] = str(row[col])
    #print field + ":" + str(row[col])
    col += 1
  print json.dumps(jsondict)
#print res
cur.close()
con.close()
