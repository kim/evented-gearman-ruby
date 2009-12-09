$:.unshift(File.expand_path(File.dirname(__FILE__) + '/../lib'))
require 'gearman'
require 'test/unit'
require 'mocha'

def start_gearmand(port = 4730, debug = false)
  log = debug ? "-l /tmp/gearmand.log -vvv" : ""
  system "gearmand -d -p #{port} --pid-file=#{gearmand_pidfile(port)} #{log}"
  gearmand_pid(port)
end

def stop_gearmand(port = 4730)
  Process.kill "KILL", gearmand_pid(port)
  File.unlink gearmand_pidfile(port)
end

def teardown_gearmands
  glob = "/tmp/gearmand_*_#{$$}.pid"
  Dir[glob].each do |pidfile|
    Process.kill "KILL", `cat #{pidfile}`.to_i
    File.unlink pidfile
  end
end

def gearmand_pid(port = 4730)
  `cat #{gearmand_pidfile(port)}`.to_i
end

def gearmand_pidfile(port)
  "/tmp/gearmand_#{port}_#{$$}.pid"
end