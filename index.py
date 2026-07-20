import ydb
import ydb.iam
import os
import email
import email.header
import logging
from datetime import datetime
from pythonjsonlogger import jsonlogger


def to_timestamp_us(value):
    """Parse an ISO-8601 string into microseconds since epoch (YDB Timestamp)."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000)


class YcLoggingFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(YcLoggingFormatter, self).add_fields(log_record, record, message_dict)
        log_record["logger"] = record.name
        log_record["level"] = str.replace(
            str.replace(record.levelname, "WARNING", "WARN"), "CRITICAL", "FATAL"
        )


logHandler = logging.StreamHandler()
logHandler.setFormatter(YcLoggingFormatter("%(message)s %(level)s %(logger)s"))

logger = logging.getLogger("postbox-events")
logger.propagate = False
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

ydb_table = os.getenv("YDB_TABLE")
driver = ydb.Driver(
    endpoint=os.getenv("YDB_ENDPOINT"),
    database=os.getenv("YDB_DATABASE"),
    credentials=ydb.iam.MetadataUrlCredentials(),
)
driver.wait(fail_fast=True, timeout=5)
pool = ydb.QuerySessionPool(driver)


def get_records_data_type():
    struct_type = ydb.StructType()
    struct_type.add_member("eventid", ydb.PrimitiveType.Utf8)
    struct_type.add_member("eventtype", ydb.PrimitiveType.Utf8)
    struct_type.add_member("mail_timestamp", ydb.PrimitiveType.Timestamp)
    struct_type.add_member("mail_messageid", ydb.PrimitiveType.Utf8)
    struct_type.add_member("mail_ch_from", ydb.PrimitiveType.Utf8)
    struct_type.add_member("mail_ch_to", ydb.PrimitiveType.Utf8)
    struct_type.add_member("mail_ch_messageid", ydb.PrimitiveType.Utf8)
    struct_type.add_member("mail_ch_subject", ydb.PrimitiveType.Utf8)
    struct_type.add_member("delivery_timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    struct_type.add_member("delivery_time_ms", ydb.OptionalType(ydb.PrimitiveType.Uint64))
    struct_type.add_member("delivery_recipients", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    struct_type.add_member("bounce_bounceType", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    struct_type.add_member("bounce_bounceSubType", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    struct_type.add_member("bounce_bouncedRecipients", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    struct_type.add_member("bounce_timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    return ydb.ListType(struct_type)


BATCH_SQL = f"""
DECLARE $records AS List<Struct<
    eventid: Utf8,
    eventtype: Utf8,
    mail_timestamp: Timestamp,
    mail_messageid: Utf8,
    mail_ch_from: Utf8,
    mail_ch_to: Utf8,
    mail_ch_messageid: Utf8,
    mail_ch_subject: Utf8,
    delivery_timestamp: Timestamp?,
    delivery_time_ms: Uint64?,
    delivery_recipients: Utf8?,
    bounce_bounceType: Utf8?,
    bounce_bounceSubType: Utf8?,
    bounce_bouncedRecipients: Utf8?,
    bounce_timestamp: Timestamp?
>>;

UPSERT INTO {ydb_table}
SELECT
    CurrentUtcDatetime() AS saved_datetime,
    eventid,
    eventtype,
    mail_timestamp,
    mail_messageid,
    mail_ch_from,
    mail_ch_to,
    mail_ch_messageid,
    mail_ch_subject,
    delivery_timestamp,
    delivery_time_ms,
    delivery_recipients,
    bounce_bounceType,
    bounce_bounceSubType,
    bounce_bouncedRecipients,
    bounce_timestamp
FROM AS_TABLE($records);
"""


def handler(event, context):
    logger.info("new messages received", extra={"count": len(event["messages"])})
    records = []
    for message in event["messages"]:
        eventid = message["eventId"]
        eventtype = message["eventType"]
        mail_timestamp = message["mail"]["timestamp"]
        mail_messageid = message["mail"]["messageId"]
        mail_ch_from = message["mail"]["commonHeaders"]["from"]
        mail_ch_to = message["mail"]["commonHeaders"]["to"]
        mail_ch_messageid = message["mail"]["commonHeaders"]["messageId"]
        mail_ch_subject = email.header.decode_header(
            message["mail"]["commonHeaders"]["subject"]
        )[0][0]
        if isinstance(mail_ch_subject, bytes):
            mail_ch_subject = mail_ch_subject.decode()

        record = {
            "eventid": eventid,
            "eventtype": eventtype,
            "mail_timestamp": to_timestamp_us(mail_timestamp),
            "mail_messageid": mail_messageid,
            "mail_ch_from": str(mail_ch_from),
            "mail_ch_to": str(mail_ch_to),
            "mail_ch_messageid": mail_ch_messageid,
            "mail_ch_subject": str(mail_ch_subject),
            "delivery_timestamp": None,
            "delivery_time_ms": None,
            "delivery_recipients": None,
            "bounce_bounceType": None,
            "bounce_bounceSubType": None,
            "bounce_bouncedRecipients": None,
            "bounce_timestamp": None,
        }

        match eventtype:
            case "Delivery":
                record["delivery_timestamp"] = to_timestamp_us(
                    message["delivery"]["timestamp"]
                )
                record["delivery_time_ms"] = message["delivery"]["processingTimeMillis"]
                record["delivery_recipients"] = str(message["delivery"]["recipients"])
            case "Bounce":
                record["bounce_bounceType"] = message["bounce"]["bounceType"]
                record["bounce_bounceSubType"] = message["bounce"]["bounceSubType"]
                record["bounce_bouncedRecipients"] = str(
                    message["bounce"]["bouncedRecipients"]
                )
                record["bounce_timestamp"] = to_timestamp_us(
                    message["bounce"]["timestamp"]
                )
            case _:
                continue

        records.append(record)

    if records:
        try:
            pool.execute_with_retries(
                BATCH_SQL,
                {"$records": ydb.TypedValue(records, get_records_data_type())},
            )
            logger.info("events stored", extra={"count": len(records)})
        except Exception:
            logger.exception("failed to store events", extra={"count": len(records)})
            # Re-raise so the Data Streams trigger retries the batch.
            raise

    return {
        "statusCode": 200,
        "body": "messages: " + str(len(records)),
    }
