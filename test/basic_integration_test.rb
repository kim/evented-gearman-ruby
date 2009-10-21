#!/usr/bin/env ruby

$:.unshift(File.expand_path(File.dirname(__FILE__) + '/../lib'))
require 'gearman'
require 'test/unit'

class BasicIntegrationTest < Test::Unit::TestCase

  def setup
    @pid = Process.fork do
      worker = Gearman::Worker.new("localhost:4730")
      worker.add_ability("pingpong") {|data, job| "pong" }
      worker.work
    end
  end

  def teardown
    Process.kill 'KILL', @pid
  end

  def test_ping_job
    client = Gearman::Client.new("localhost:4730")
    task = Gearman::Task.new("pingpong", "ping")

    task.on_complete do |response|
      assert_equal "pong", response
    end
    client.run task
  end
end