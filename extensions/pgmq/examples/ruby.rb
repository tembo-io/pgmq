#!/usr/bin/env ruby

# NOTES: to run this file, chmod 755 and then ./ruby.rb
# this file demonstrates how to create a message queue, list the queues, post a message, read a
# message, delete a message, and determine if a queue is empty
#
# Many Rails apps will use a queue such as Redis or AWS SQS or RabbitMQ, while using postgres as
# the data store. Using pgmq eliminiates the need to support another service to queue messages for
# a pattern like pub/sub or background jobs. A gem could be introduced with a simple DSL for all
# of these actions rather than using the raw SQL.

require "pg" # requires pg gem be installed: gem install pg
require "json" # load json from ruby standard lib
require "pry" # used for debugging

# define queue name
QUEUE_NAME = "myqueue"
LOCK_TIMEOUT = 1
NUM_MSGS = 1

# Connect to the CoreDB postgres (update port number to match how you are running it locally)
conn = PG.connect(host: "localhost", port: 5434, user: "postgres", password: "postgres")

# Output versions to stdout (for debugging)
$stderr.puts '---',
	RUBY_DESCRIPTION,
	PG.version_string( true ),
	"Server version: #{conn.server_version}",
	"Client version: #{PG.library_version}",
	'---'

# create extension (will be skipped if already exists)
conn.exec( "CREATE EXTENSION if not exists pgmq CASCADE;" )

# create the queue (will create a table pg_ using the queue name)
conn.exec( "select * from pgmq_create('#{QUEUE_NAME}')" )

# list queues
list_queues = conn.exec( "select * from pgmq_list_queues()" )
$stderr.puts '---',
  "### Queues ###"

$stderr.puts list_queues.map { |queue| queue["queue_name"] }

# send a message
msg = "{yolo: 42}".to_json
msg_result = conn.exec( "select * from pgmq_send('#{QUEUE_NAME}', '#{msg}') as msg_id;" )
msg_id = msg_result.first["msg_id"]

$stderr.puts '---',
  "### msg_id ###",
  msg_id

# read a message (making it unavailable for 1 second)
msg_result = conn.exec( "select * from pgmq_read('#{QUEUE_NAME}', #{LOCK_TIMEOUT}, #{NUM_MSGS})" )
msg_row = msg_result.first

$stderr.puts '---',
  "### msg ###",
  "msg_id: #{msg_row['msg_id']}, value: #{JSON.parse(msg_row['message']).to_s}"

# delete a mesage (for a given ID)
msg_result = conn.exec( "select pgmq_delete('#{QUEUE_NAME}', #{msg_id})" )
$stderr.puts '---',
  "### msg delete: #{msg_id} ###",
  msg_result.values.flatten.first.to_s == "t" ? "true" : "false"

# read up to 1000 messages
msg_result = conn.exec( "select * from pgmq_read('#{QUEUE_NAME}', #{LOCK_TIMEOUT}, 1000)" )

if msg_result.any?
  $stderr.puts '---',
    "### msg(s) ###"

  msg_result.each do |result|
    # result["read_ct"] also available
    $stderr.puts "msg_id: #{result['msg_id']}, value: #{JSON.parse(result['message']).to_s}"
  end
else
  $stderr.puts '---',
    "### msg(s) ###",
    "empty"
end
