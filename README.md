# Передача и сохранение событий сервиса Yandex Cloud Postbox в базу данных

Работа с событиями сервиса осуществляется благодаря передачи всех событий в Data Stream клиента и сохранения событий в таблице БД YDB. Перенос событий из YDS в таблицу осуществляется функцией из данного репозитория. В качестве параметров функции (переменных окружения) необходимо указать:

**YDB_DATABASE** - путь к базе в формате **/ru-central1/b1pr456f9prz1hjrtkvm/etnm43al12fkgjdjl45wq**

**YDB_TABLE** - название таблицы (ниже используется **postbox_events**)

**YDB_ENDPOINT** - эндпоинт для подключения к базе, указан в свойствах созданной базы, в формате **grpcs://ydb.serverless.yandexcloud.net:2135**

Для включения передачи событий из Postbox необходимо:
- создать поток данных Data Streams
- настроить в Postbox передачу событий в этот поток (настроить конфигурацию)
- создать таблицу для данных в YDB
  
  _CREATE TABLE postbox_events
(
    saved_datetime Datetime NOT NULL,
    eventid String NOT NULL,
    eventtype String,
    mail_timestamp Timestamp,
    mail_messageid String,
    mail_ch_from String,
    mail_ch_to String, 
    mail_ch_messageid String,
    mail_ch_subject String,
    delivery_timestamp Timestamp,
    delivery_time_ms Uint64,
    delivery_recipients String,
    bounce_bounceType String,
    bounce_bounceSubType String,
    bounce_bouncedRecipients String,
    bounce_timestamp Timestamp,
    -- message Json,
    PRIMARY KEY (saved_datetime, eventid)_
  
- создать функцию на основе кода или архива в данном репозитории
- создать триггер для запуска функции при появлении событий в Data Streams и сохранения событий в таблицу. На основе данных возможно построение дашбордов в Datalens


Более полная инструкция по сслылке - https://yandex.cloud/ru/docs/postbox/tutorials/events-from-postbox-to-yds


<img width="1414" alt="image" src="https://github.com/user-attachments/assets/fd5fd6bd-67ab-4353-9de7-f8cc91348b34">

