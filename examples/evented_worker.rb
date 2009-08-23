require 'rubygems'
require '../lib/gearman'

Gearman::Util.debug = true

servers = ['localhost:4730']
w = Gearman::Evented::Worker.new(servers)

# Add a handler for a "sleep" function that takes a single argument, the
# number of seconds to sleep before reporting success.
w.add_ability('sleep') do |data,job|
 seconds = data
 (1..seconds.to_i).each do |i|
   sleep 1
   print i
   # Report our progress to the job server every second.
   job.report_status(i, seconds)
 end
 # Report success.
 "SUCCESS"
end
w.work
