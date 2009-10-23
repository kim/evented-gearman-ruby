require File.dirname(__FILE__) + '/test_helper'

class JobTest < Test::Unit::TestCase

  def setup
    @handle = "H:foo:1"
    @mock_client = mock()
    @job = Gearman::Job.new(@mock_client, @handle)
  end

  def test_supports_report_status
    @mock_client.expects(:send).with(:work_status, [@handle, 1, 5].join("\0"))
    @job.report_status(1, 5)
  end

  def test_supports_send_partial
    @mock_client.expects(:send).with(:work_data, [@handle, "bar"].join("\0"))
    @job.send_partial("bar")
  end

  def test_supports_send_data
    @mock_client.expects(:send).with(:work_data, [@handle, "bar"].join("\0"))
    @job.send_data("bar")
  end

  def test_supports_report_warning
    @mock_client.expects(:send).with(:work_warning, [@handle, "danger"].join("\0"))
    @job.report_warning("danger")
  end
end
