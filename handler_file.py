import cx_Oracle
import json
import datetime
import os

def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

def location_handler(event, context):
    states_list = []
    try:
        #os.environ["LD_LIBRARY_PATH"] = os.getcwd()+'/instantclient_12_2'
        print 'value of LD_LIBRARY_PATH' + os.environ['LD_LIBRARY_PATH']
        con = cx_Oracle.connect('pawn01_lambda', 'T0mAt056_$#', 'PAWN01')
        cur = con.cursor()
    except (Exception, cx_Oracle.Error) as error:
        print error
    else:
        ret_list = []
        records = cur.execute('select * from ALL_ACTIVE_STATES')
        #print str(type(records))
        for record in records:
            #print record[0]
            #print str(type(record[0]))
            states_list.append(record[0])
        for state in states_list:
            tmp_dict_per_state = {}
            tmp_dict_per_state['state'] = state
            print state
            tmp_dict_per_state['locations'] = []
            location_cursor = con.cursor()
            location_sql_statement = "select LOCATION_ID,LOCATION_NAME,CITY,COUNTY,STATE,COUNTRY from ALL_ACTIVE_LOCATIONS where state='{}'".format(state)
            location_records_cursor = location_cursor.execute(location_sql_statement)
            columns = [i[0] for i in location_records_cursor.description]
            #print columns
            #print type(columns)
            for location in location_records_cursor:
                row =  list(location)
                #print row
                location_desc = dict(zip(columns, row))
                #print row
                location_id = location_desc['LOCATION_ID']
                delivery_point_cursor = con.cursor()
                delivery_point_sql_statement = "select DELIVERY_POINT_ID,LOCATION_ID,DP_NAME,DESCRIPTION,LANDMARK,ADDRESS_LINE_1,"\
                                                    "ADDRESS_LINE_2,CITY,COUNTY,POSTAL_CODE,STATE,COUNTRY,LATITUDE,LONGITUDE,PICKUP_START_TIME,"\
                                                    "PICKUP_END_TIME,SALES_TAX FROM DELIVERY_POINTS where location_id={}".format(location_id)
                delivery_point_records_cursor = delivery_point_cursor.execute(delivery_point_sql_statement)
                delivery_columns = [i[0] for i in delivery_point_records_cursor.description]
                location_desc['pickup_points'] = []
                for d_point in delivery_point_records_cursor:
                    d_point_row = list(d_point)
                    d_point_desc = dict(zip(delivery_columns,d_point_row))
                    location_desc['pickup_points'].append(d_point_desc)
                tmp_dict_per_state['locations'].append(location_desc)
                ret_list.append(tmp_dict_per_state)
        #json_list = json.dumps(ret_list)
        return ret_list