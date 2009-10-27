require File.dirname(__FILE__) + '/test_helper'

class ClientTest < Test::Unit::TestCase

  def test_accepts_multiple_job_servers
  reactor_mock = mock
  reactor_mock.stubs(:callback)
  Gearman::Evented::ClientReactor.expects(:connect).times(2).returns(reactor_mock)
  Gearman::Client.new(["localhost:4730", "localhost:4731"]).run([], 0.1)
  end

  def test_accepts_exactly_one_job_server
    reactor_mock = mock
    reactor_mock.stubs(:callback)
    Gearman::Evented::ClientReactor.expects(:connect).with("localhost", "4730", {}).returns(reactor_mock)
    Gearman::Client.new("localhost:4730").run([], 0.1)
  end
end
