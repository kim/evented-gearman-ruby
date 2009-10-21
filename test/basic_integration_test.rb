#!/usr/bin/env ruby

$:.unshift(File.expand_path(File.dirname(__FILE__) + '/../lib'))
require 'gearman'
require 'test/unit'

class BasicIntegrationTest < Test::Unit::TestCase

  def setup
    @pid = Process.fork do
      worker = Gearman::Worker.new("localhost:4730")
      worker.add_ability("pingpong") {|data, job| "pong" }
      worker.add_ability("crash") {|data, job| raise Exception.new("BOOM!") }
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
    assert true, failed
  end
end