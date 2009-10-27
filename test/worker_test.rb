require File.dirname(__FILE__) + '/test_helper'

class WorkerTest < Test::Unit::TestCase

  def test_accepts_multiple_job_servers
    Gearman::Evented::WorkerReactor.expects(:connect).times(2)

    EM.run do
      EM.add_timer(0.1) { EM.stop_event_loop }
      Gearman::Worker.new(["localhost:4730", "localhost:4731"]).work
    end
  end

  def test_accepts_exactly_one_job_server
    Gearman::Evented::WorkerReactor.expects(:connect).with("localhost", "4730", {:abilities => {}}).times(1)

    EM.run do
      EM.add_timer(0.1) { EM.stop_event_loop }
      Gearman::Worker.new("localhost:4730").work
    end
  end

  def test_passes_abilities_to_reactor
    worker = Gearman::Worker.new("localhost:4730")
    worker.add_ability("foo") {|data, job| "noop!"}
    worker.add_ability("bar") {|data, job| "nothing to see here!" }
    worker.remove_ability("bar")

    assert_equal true, worker.has_ability?("foo")
    assert_equal false, worker.has_ability?("bar")

    Gearman::Evented::WorkerReactor.expects(:connect).with do |host, port, opts|
      assert_equal "localhost", host
      assert_equal 4730, port.to_i
      assert_equal 1, opts[:abilities].size
      assert_equal true, opts[:abilities]['foo'][:callback].is_a?(Proc)
      true
    end

    EM.run do
      EM.add_timer(0.1) { EM.stop_event_loop }
      worker.work
    end
  end
end
