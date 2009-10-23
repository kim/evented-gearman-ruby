require File.dirname(__FILE__) + '/test_helper'

class ProtocolTest < Test::Unit::TestCase

  def test_encode_request
    payload = ["foo", "123", "bar"].join("\0")
    expected = "\0REQ" + [Gearman::Protocol::COMMANDS_NUMERIC[:submit_job], payload.size].pack("NN") + payload
    assert_equal expected, Gearman::Protocol.encode_request(:submit_job, payload)
  end

  def test_decode_response
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_data], 3].pack("NN") + handle + "\0foo"
    packets = Gearman::Protocol.decode_response(response)
    assert_equal 1, packets.size
    assert_equal [:work_data, "H:wrk.acme:1", "foo"], packets.first
  end

  def test_decodes_multiple_response_packets
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_data], 3].pack("NN") + handle + "\0foo"
    response << "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_data], 3].pack("NN") + handle + "\0bar"
    response << "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_data], 3].pack("NN") + handle + "\0baz"

    assert_equal 3, Gearman::Protocol.decode_response(response).size
  end

  def test_response_packet
    packet = [:work_data, handle, "foo"]
    assert_equal packet, Gearman::Protocol.response_packet(*packet)
  end

  def test_decodes_work_complete
    data     = "esta complet"
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_complete], data.size].pack("NN") + [handle, data].join("\0")
    assert_equal [:work_complete, handle, data], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_work_exception
    data     = "{native perl exception object}"
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_exception], data.size].pack("NN") + [handle, data].join("\0")
    assert_equal [:work_exception, handle, data], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_work_warning
    data     = "I warn you, dude"
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_warning], data.size].pack("NN") + [handle, data].join("\0")
    assert_equal [:work_warning, handle, data], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_work_data
    data     = "foo"
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_data], data.size].pack("NN") + [handle, data].join("\0")
    assert_equal [:work_data, handle, data], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_error
    data     = "error"
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:error], data.size].pack("NN") + [handle, data].join("\0")
    assert_equal [:error, handle, data], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_job_assign
    function  = "foo_function"
    arguments = "arguments"

    payload  = [handle, function, arguments].join("\0")
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:job_assign], payload.size].pack("NN") + payload
    assert_equal [:job_assign, handle, function, arguments], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_job_assign_uniq
    function  = "foo_function"
    arguments = "arguments"
    unique_id = "123-657"

    payload  = [handle, function, unique_id, arguments].join("\0")
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:job_assign_uniq], payload.size].pack("NN") + payload
    assert_equal [:job_assign_uniq, handle, function, arguments, unique_id], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_work_fail
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_fail], 0].pack("NN") + handle
    assert_equal [:work_fail, handle], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_job_created
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:job_created], 0].pack("NN") + handle
    assert_equal [:job_created, handle], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_no_job
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:no_job], 0].pack("NN") + handle
    assert_equal [:no_job, handle], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_noop
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:noop], 0].pack("NN") + handle
    assert_equal [:noop, handle], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_work_status
    numerator   = "1"
    denominator = "5"

    payload  = [handle, numerator, denominator].join("\0")
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:work_status], payload.size].pack("NN") + payload

    assert_equal [:work_status, handle, numerator, denominator], Gearman::Protocol.decode_response(response).first
  end

  def test_decodes_status_res
    known       = "1"
    running     = "0"
    numerator   = "1"
    denominator = "4"

    payload = [handle, known, running, numerator, denominator].join("\0")
    response = "\0RES" + [Gearman::Protocol::COMMANDS_NUMERIC[:status_res], payload.size].pack("NN") + payload

    assert_equal [:status_res, handle, known, running, numerator, denominator], Gearman::Protocol.decode_response(response).first
  end

  def test_raises_on_invalid_command
    response = "\0RES" + [6969, 0].pack("NN")
    assert_raises(Gearman::ProtocolError) { Gearman::Protocol.decode_response(response) }
    assert_raises(Gearman::ProtocolError) { Gearman::Protocol.response_packet(*[6969, handle, ''])}
  end

private
  def handle
    "H:wrk.acme:1"
  end
end