import json
import ydb
import ydb.iam
import os
import base64
import email
import email.header

ydb_table = os.getenv('YDB_TABLE')
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'),
                    database=os.getenv('YDB_DATABASE'),
                    credentials=ydb.iam.MetadataUrlCredentials(),)
driver.wait(fail_fast=True, timeout=5)
pool = ydb.SessionPool(driver)

def handler(event, context):
    print ('new messages: ' + str(len(event['messages'])))
    for m in event['messages']:
        #message = json.loads(m)
        #print (m)
        message = m
        eventid = message['eventId']
        eventtype = message['eventType'] #
        mail_timestamp = message['mail']['timestamp']
        mail_messageid = message['mail']['messageId']
        mail_ch_from = message['mail']['commonHeaders']['from']
        mail_ch_to = message['mail']['commonHeaders']['to']
        mail_ch_messageid = message['mail']['commonHeaders']['messageId']
        mail_ch_subject = email.header.decode_header(message['mail']['commonHeaders']['subject'])[0][0]
        if isinstance(mail_ch_subject, bytes):
            mail_ch_subject = mail_ch_subject.decode()
        match eventtype:
            case 'Delivery':
                delivery_timestamp = message['delivery']['timestamp']
                delivery_time_ms = message['delivery']['processingTimeMillis']
                delivery_recipients = message['delivery']['recipients']

                sql = 'UPSERT INTO ' + ydb_table + ' (saved_datetime,' \
                                    'eventid,' \
                                    'eventtype,' \
                                    'mail_timestamp,' \
                                    'mail_messageid,' \
                                    'mail_ch_from,' \
                                    'mail_ch_to,' \
                                    'mail_ch_messageid,' \
                                    'mail_ch_subject,' \
                                    'delivery_timestamp,' \
                                    'delivery_time_ms,' \
                                    'delivery_recipients)' \
                        ' VALUES (CurrentUtcDatetime(),\"' + eventid + "\",\"" + eventtype +"\",CAST(\"" + mail_timestamp + "\" AS Timestamp),\""+ mail_messageid + "\",\"" + str(mail_ch_from).replace("\"","'") + "\",\"" + str(mail_ch_to).replace("\"","'") + "\",\"" + mail_ch_messageid + "\",\"" + str(mail_ch_subject).replace("\"","'") + "\"," \
                        'CAST(\"' + delivery_timestamp + "\" AS Timestamp)," + str(delivery_time_ms) + ",\"" + str(delivery_recipients) + '\");'

            case 'Bounce':
                bounce_bounceType = message['bounce']['bounceType']
                bounce_bounceSubType = message['bounce']['bounceSubType']
                bounce_bouncedRecipients = message['bounce']['bouncedRecipients']
                bounce_timestamp  = message['bounce']['timestamp']

                sql = 'UPSERT INTO ' + ydb_table + ' (saved_datetime,' \
                                    'eventid,' \
                                    'eventtype,' \
                                    'mail_timestamp,' \
                                    'mail_messageid,' \
                                    'mail_ch_from,' \
                                    'mail_ch_to,' \
                                    'mail_ch_messageid,' \
                                    'mail_ch_subject,' \
                                    'bounce_bounceType,' \
                                    'bounce_bounceSubType,' \
                                    'bounce_bouncedRecipients,' \
                                    'bounce_timestamp)' \
                        ' VALUES (CurrentUtcDatetime(),\"' + eventid + "\",\"" + eventtype +"\",CAST(\"" + mail_timestamp + "\" AS Timestamp),\""+ mail_messageid + "\",\"" + str(mail_ch_from).replace("\"","'") + "\",\"" + str(mail_ch_to).replace("\"","'") + "\",\"" + mail_ch_messageid + "\",\"" + str(mail_ch_subject).replace("\"","'") + "\"," \
                        '\"' + bounce_bounceType + "\",\"" + bounce_bounceSubType + "\",\"" + str(bounce_bouncedRecipients) + "\",CAST(\"" + str(bounce_timestamp) + '\" AS Timestamp));'
            case _:
             continue

        print (sql)

        def execute_query(session):
            #sql = ""
            return session.transaction().execute(
            sql,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            )
        try:
            result = pool.retry_operation_sync(execute_query)
        except Exception as e:
            print (e)

    return {
        'statusCode': 200,
        'body': 'messages: ' + str(len(event['messages'])),
    }
