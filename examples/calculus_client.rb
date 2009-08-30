#!/usr/bin/env ruby
require 'rubygems'
require '../lib/gearman'
#Gearman::Util.debug = true

# Connect to the local server (at the default port 4730)
client = Gearman::Client.new('localhost')
taskset = Gearman::Taskset.new

# Get something to echo
puts '[client] Write a basic arithmetic operation:'
input = gets

operations = input.chomp.scan(/\d+[\+\-\*\/]\d+/).compact
puts "[client] The following operations were found: #{operations.inspect}"

# Setup a task for operation
operations.each do |op|
  # Determining the operation
  case op
    when /\+/
      type, data = 'addition', op.split('+')
    when /\-/
      type, data = 'subtraction', op.split('-')
    when /\*/
      type, data = 'multiplication', op.split('*')
    when /\//
      type, data = 'division', op.split('/')
  end

  task = Gearman::Task.new(type, Marshal.dump(data.map {|v| v.to_i}))
  task.on_complete {|r| puts "[client] #{type} result is: #{r}" }
  task.on_warning {|w| puts "[client] warn: #{w}" }
  task.on_fail { puts "[client] calculation failed" } 

  # Sending the task to the server
  puts "[client] Sending values: #{data.inspect}, to the '#{type}' worker"
  taskset << task
end

client.run(taskset)
