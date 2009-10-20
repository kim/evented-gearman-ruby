module Gearman

  class ProtocolError < Exception; end


  class Protocol
    # Map from Integer representations of commands used in the network
    # protocol to more-convenient symbols.
    COMMANDS = {
      1  => :can_do,               # W->J: FUNC
      2  => :cant_do,              # W->J: FUNC
      3  => :reset_abilities,      # W->J: --
      4  => :pre_sleep,            # W->J: --
      #5 =>  (unused),             # -      -
      6  => :noop,                 # J->W: --
      7  => :submit_job,           # C->J: FUNC[0]UNIQ[0]ARGS
      8  => :job_created,          # J->C: HANDLE
      9  => :grab_job,             # W->J: --
      10 => :no_job,               # J->W: --
      11 => :job_assign,           # J->W: HANDLE[0]FUNC[0]ARG
      12 => :work_status,          # W->J/C: HANDLE[0]NUMERATOR[0]DENOMINATOR
      13 => :work_complete,        # W->J/C: HANDLE[0]RES
      14 => :work_fail,            # W->J/C: HANDLE
      15 => :get_status,           # C->J: HANDLE
      16 => :echo_req,             # ?->J: TEXT
      17 => :echo_res,             # J->?: TEXT
      18 => :submit_job_bg,        # C->J: FUNC[0]UNIQ[0]ARGS
      19 => :error,                # J->?: ERRCODE[0]ERR_TEXT
      20 => :status_res,           # C->J: HANDLE[0]KNOWN[0]RUNNING[0]NUM[0]DENOM
      21 => :submit_job_high,      # C->J: FUNC[0]UNIQ[0]ARGS
      22 => :set_client_id,        # W->J: [RANDOM_STRING_NO_WHITESPACE]
      23 => :can_do_timeout,       # W->J: FUNC[0]TIMEOUT
      24 => :all_yours,            # REQ    Worker
      25 => :work_exception,       # W->J: HANDLE[0]ARG
      26 => :option_req,           # C->J: TEXT
      27 => :option_res,           # J->C: TEXT
      28 => :work_data,            # REQ    Worker
      29 => :work_warning,         # W->J/C: HANDLE[0]MSG
      30 => :grab_job_uniq,        # REQ    Worker
      31 => :job_assign_uniq,      # RES    Worker
      32 => :submit_job_high_bg,   # C->J: FUNC[0]UNIQ[0]ARGS
      33 => :submit_job_low,       # C->J: FUNC[0]UNIQ[0]ARGS
      34 => :submit_job_low_bg,    # C->J: FUNC[0]UNIQ[0]ARGS
      35 => :submit_job_sched,     # REQ    Client
      36 => :submit_job_epoch      # REQ    Client
    }

    # Map e.g. 'can_do' => 1
    COMMANDS_NUMERIC = COMMANDS.invert

    # Default job server port.
    DEFAULT_PORT = 4730

    class << self

      def encode_request(type_name, arg = nil)
        type_num = COMMANDS_NUMERIC[type_name.to_sym]
        raise InvalidArgsError, "Invalid type name '#{type_name}'" unless type_num
        arg = '' if not arg
        "\0REQ" + [type_num, arg.size].pack('NN') + arg
      end

      def decode_response(data, packets = [])
        # p data
        magic, type, len = data[0..12].unpack('a4NN')
        raise ProtocolError, "Invalid magic '#{magic}'" unless magic == "\0RES"
        type = COMMANDS[type]
        raise ProtocolError, "Invalid packet type #{type}" unless type

        buf, *more = data[12, data.size].split("\0RES")
        handle, data = buf ? buf.split("\0", 2) : nil
        packets << response_packet(type, handle, data)
        more.each do |packet|
          decode_response("\0RES#{packet}", packets)
        end
        packets
      end

      def response_packet(type, handle, data)
        case type
        when :work_complete,
             :work_exception,
             :work_warning,
             :work_data,
             :error
          [type, handle, data]
        when :job_assign
          func, data = data.split("\0", 3)
          [type, handle, func.to_s, data]
        when :job_assign_uniq
          func, uuid, data = data.split("\0", 3)
          [type, handle, func.to_s, data, uuid]
        when :work_fail,
             :job_created,
             :no_job,
             :noop
          [type, handle]
        when :work_status
          num, den = data.split("\0", 3)
          [type, handle, num, den]
        when :status_res
          known, running, num, den = data.split("\0", 4)
          [type, handle, known, running, num, den]
        else
          raise ProtocolError, "Invalid packet #{type}"
        end
      end
    end
  end
end
