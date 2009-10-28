require File.dirname(__FILE__) + '/test_helper'

class CrashTest < Test::Unit::TestCase

  def teardown
    system "killall gearmand"
  end

  def test_worker_should_reconnect_if_gearmand_goes_away
    system 'gearmand -d -p 4730'
    worker = Gearman::Worker.new("localhost:4730", :reconnect_sec => 1)
    worker.add_ability("foo") {|data, job| "noop!" }

    response = nil
    EM.run do
      worker.work
      system 'killall gearmand'
      system 'gearmand -d -p 4730'

      task = Gearman::Task.new("foo", "ping")

      task.on_complete {|res| response = res }
      Gearman::Client.new("localhost:4730").run task
    end

    assert_equal "noop!", response
  end

  def test_client_and_worker_should_use_failover_gearmand_if_primary_is_not_available
    system 'gearmand -d -p 4731'
    worker = Gearman::Worker.new(["localhost:4730", "localhost:4731"], :reconnect_sec => 1)
    worker.add_ability("foo") {|data, job| "noop!" }

    response = nil
    EM.run do
      worker.work
      task = Gearman::Task.new("foo", "ping")
      task.on_complete {|res| response = res }
      Gearman::Client.new(["localhost:4730", "localhost:4731"]).run task
    end

    assert_equal "noop!", response
  end

  def test_client_and_worker_should_switch_to_failover_gearmand_if_primary_goes_down
    pidfile = "/tmp/gearmand#{$$}.pid"
    system "gearmand -d -p 4730 --pid-file=#{pidfile}"
    gearmand_pid = `cat #{pidfile}`.to_i
    File.unlink pidfile

    system 'gearmand -d -p 4731'
    worker = Gearman::Worker.new(["localhost:4730", "localhost:4731"], :reconnect_sec => 1)
    worker.add_ability("foo") {|data, job| "noop!" }

    response = nil
    EM.run do
      worker.work

      Process.kill "KILL", gearmand_pid

      task = Gearman::Task.new("foo", "ping")
      task.on_complete {|res| response = res }
      Gearman::Client.new(["localhost:4730", "localhost:4731"]).run task
    end

    assert_equal "noop!", response
  end
end
