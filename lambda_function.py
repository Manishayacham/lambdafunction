import json
import datetime
import time
import os
import dateutil.parser
import logging
import pymysql
import sys

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

rds_host  = 'project2db.c7kki8z8cijl.us-east-1.rds.amazonaws.com'
name = 'chargers'
password = 'chargers'
db_name = 'project2db'

def lambda_handler(event, context):
    
    firstname = event['currentIntent']['slots']['FirstName']
    lastname = event['currentIntent']['slots']['LastName']
    startdate = event['currentIntent']['slots']['PickupDate']
    enddate = event['currentIntent']['slots']['DropoffDate']
    cartype = event['currentIntent']['slots']['Cartype']
    license = event['currentIntent']['slots']['License']
    username=event['currentIntent']['slots']['username']
    
    today = datetime.date.today()
    startdate = datetime.datetime.strptime(startdate , '%Y-%m-%d').date()
    enddate = datetime.datetime.strptime(enddate , '%Y-%m-%d').date()
    
    
    if(startdate < today):
        message = 'Startdate should be greaterthan or equal to cuurent date'
    elif(enddate < startdate):
        message = 'Enddate should be greaterthan startdate'
    else:
        conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute('select count(*),SUBSTRING(expiry_date,1,10) from project2db.userlicense where license= %s and is_blacklisted = %s ', (license, '0'))
            result_set = cur.fetchone()
            for row in result_set:
                if(result_set[0] == 0):
                    message = 'License is not uploaded, please upload license in register upload page/License is blacklisted' 
                else:
                    
                    expirydate = datetime.datetime.strptime(result_set[1], '%Y-%m-%d')
                    
                    if(expirydate.date() < startdate or expirydate.date() < enddate):
                        
                        message = 'Invalid license/license expired, upload valid license'
                    else:
                        message = 'Your Booking has been made suceesfully' 
                        
                    cur.execute('INSERT INTO project2db.booking_details(firstname,car_type,end_date,lastname,license,start_date,user_name) VALUES (%s, %s,%s, %s,%s, %s, %s) ', (firstname,cartype,enddate,lastname,license,startdate,username))
                    conn.commit()
    return dispatch(message)
    
def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response
    
def dispatch(me):
    return close(
        {},
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': me
        }
    )