require File.dirname(__FILE__) + '/test_helper'

class BasicIntegrationTest < Test::Unit::TestCase

  def setup
    @pid = Process.fork do
      worker = Gearman::Worker.new("localhost:4730")
      worker.add_ability("pingpong") {|data, job| "pong" }
      worker.add_ability("crash") {|data, job| raise Exception.new("BOOM!") }
      worker.add_ability("chunked") do |data, job|
        5.times {|i| job.send_partial("chunk #{i}") }
      end
      worker.work
    end

    @client = Gearman::Client.new("localhost:4730")
  end

  def teardown
    Process.kill 'KILL', @pid
  end

  def test_ping_job
    task = Gearman::Task.new("pingpong", "ping")

    task.on_complete do |response|
      assert_equal "pong", response
    end
    @client.run task
  end

  def test_exception_in_worker
    task = Gearman::Task.new("crash", "doesntmatter")

    warning_given = nil
    failed = false
    task.on_warning {|warning| warning_given = warning }
    task.on_fail { failed = true}
    @client.run task

    assert_not_nil warning_given
    assert_equal true, failed
  end

  def test_chunked_response
    task = Gearman::Task.new("chunked")
    chunks_received = 0
    task.on_data do |data|
      assert_match /^chunk \d/, data
      chunks_received += 1
    end
    @client.run task

    assert_equal 5, chunks_received
  end

  def test_background
    task = Gearman::Task.new('pingpong', 'background', :background => true, :poll_status_interval => 0.1)
    task.on_complete {|d| flunk "on_complete should never be called for a background job!" }
    status_received = false
    task.on_status {|d| status_received = true }
    @client.run task

    assert_equal true, status_received
  end
end