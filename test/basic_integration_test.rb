require File.dirname(__FILE__) + '/test_helper'

class BasicIntegrationTest < Test::Unit::TestCase

  def setup
    @client = Gearman::Client.new("localhost:4730")
    @worker = Gearman::Worker.new("localhost:4730")
  end

  def test_ping_job
    response = nil

    EM.run do
      @worker.add_ability("pingpong") {|data, job| "pong" }
      @worker.work

      task = Gearman::Task.new("pingpong", "ping")

      task.on_complete {|res| response = res }
      @client.run task
    end

    assert_equal "pong", response
  end

  def test_exception_in_worker
    warning_given = nil
    failed = false

    EM.run do
      @worker.add_ability("crash") {|data, job| raise Exception.new("BOOM!") }
      @worker.work

      task = Gearman::Task.new("crash", "doesntmatter")

      task.on_warning {|warning| warning_given = warning }
      task.on_fail { failed = true}
      @client.run task
    end

    assert_not_nil warning_given
    assert_equal true, failed
  end

  def test_chunked_response
    chunks_received = 0

    EM.run do
      @worker.add_ability("chunked") do |data, job|
        5.times {|i| job.send_partial("chunk #{i}") }
      end
      @worker.work

      task = Gearman::Task.new("chunked")
      task.on_data do |data|
        assert_match /^chunk \d/, data
        chunks_received += 1
      end
      @client.run task
    end

    assert_equal 5, chunks_received
  end

  def test_background
    status_received = false

    EM.run do
      @worker.add_ability("fireandforget") {|data, job| "this goes to /dev/null" }
      @worker.work

      task = Gearman::Task.new('fireandforget', 'background', :background => true, :poll_status_interval => 0.1)
      task.on_complete {|d| flunk "on_complete should never be called for a background job!" }
      task.on_status {|d| status_received = true }
      @client.run task
    end

    assert_equal true, status_received
  end
end