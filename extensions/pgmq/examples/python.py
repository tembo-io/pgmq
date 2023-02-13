import json
import pprint

from sqlalchemy import create_engine, text

# Connect to the CoreDB postgres
engine = create_engine("postgresql://postgres:postgres@0.0.0.0:5432/postgres")

# create extension
with engine.connect() as con:
    # create extension
    created = con.execute(text( "create extension if not exists pgmq;"))
    con.commit()

QUEUE_NAME = 'myqueue'
VT = 10
NUM_MSGS = 1

with engine.connect() as con:
    # create a queue
    created = con.execute(text( f"select * from pgmq_create('{QUEUE_NAME}');"))
    # list queues
    list_queues = con.execute(text( "select * from pgmq_list_queues()"))
    column_names = list_queues.keys()
    rows = list_queues.fetchall()
    print("### Queues ###")
    for row in rows:
        pprint.pprint(dict(zip(column_names, row)))
    con.commit()

with engine.connect() as con:
    # send a message
    msg = json.dumps({"yolo": 42})
    msg_id = con.execute(text(f"""
        select * 
        from pgmq_send('{QUEUE_NAME}', '{msg}') as msg_id;
    """))
    column_names = msg_id.keys()
    rows = msg_id.fetchall()
    print("### Message ID ###")
    for row in rows:
        pprint.pprint(dict(zip(column_names, row)))
    
with engine.connect() as con:
    # read a message, make it unavailable to be read again for 5 seconds
    read = con.execute(text(f"""
        select *
        from pgmq_read('{QUEUE_NAME}', {VT}, {NUM_MSGS});
    """))
    column_names = read.keys()
    rows = read.fetchall()
    print("### Read Message ###")
    for row in rows:
        pprint.pprint(dict(zip(column_names, row)))

with engine.connect() as con:
    # delete a message
    deleted = con.execute(text(f"select pgmq_delete('{QUEUE_NAME}', 1);"))
    column_names = deleted.keys()
    rows = deleted.fetchall()
    print("### Message Deleted ###")
    for row in rows:
        pprint.pprint(dict(zip(column_names, row)))

