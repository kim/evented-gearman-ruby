module Gearman
  module Evented

    module WorkerReactor
      include Gearman::Evented::Reactor

      def connection_completed
        send :set_client_id, client_id
        super

        @abilities ||= @opts.delete(:abilities) || []
        @abilities.each do |ability, args|
          announce_ability(ability, args[:timeout])
        end

        grab_job
      end

      def announce_ability(name, timeout)
        cmd = timeout ? :can_do_timeout : :can_do
        arg = timeout ? "#{name}\0#{timeout.to_s}" : name
        log "announce_ability #{name} #{timeout}"
        send cmd, arg
      end

      def announce_disability(name)
        send :cant_do, name
      end

      def grab_job
        log "Grab Job"
        send :grab_job_uniq
      end

      def work_fail(handle)
        send :work_fail, handle
      end

      def work_complete(handle, data)
        send :work_complete, "#{handle}\0#{data}"
      end

      def work_warning(handle, message)
        send :work_warning, "#{handle}\0#{message}"
      end

      def receive_data(data)
        Gearman::Protocol.decode_response(data).each do |type, handle, *data|
          dispatch_packet(type, handle, *data)
        end
      end

      def dispatch_packet(type, handle, *data)
        success = true
        timer   = 0
        case type
        when :no_job
          send :pre_sleep
          timer = @opts[:reconnect_sec] || 30
        when :job_assign, :job_assign_uniq
          log "job assign #{handle}, #{data.inspect}"
          handle_job_assign(handle, data[0], data[1])
        when :noop
          log "NOOP"
        when :error
          log "[ERROR]: error from server #{server}: #{data}"
        else
          log "Got unknown #{type}, #{data} from #{server}"
        end

        EM.add_timer(timer) { grab_job }
        succeed [handle, data]
      end

      def handle_job_assign(handle, func, args = '')
        return unless handle
        unless func
          log "ERROR: Ignoring job_assign with no function"
          return
        end

        log "Got job_assign '#{func}' with handle #{handle} and #{args.size rescue 0} byte(s)"

        unless @abilities.has_key?(func)
          log "Ignoring job_assign for unsupported func #{func} with handle #{handle}"
          work_fail handle
          return
        end

        exception = nil
        begin
          ret = @abilities[func][:callback].call(args, Gearman::Job.new(self, handle))
        rescue Exception => e
          exception = e
        end

        if ret && exception.nil?
          ret = ret.to_s
          log "Sending work_complete for #{handle} with #{ret.size} byte(s)"
          work_complete handle, ret
        elsif exception.nil?
          log "Sending work_fail for #{handle} to #{server}"
          work_fail handle
        elsif exception
          log "exception #{exception.message}, sending work_warning, work_fail for #{handle}"
          work_warning handle, exception.message
          work_fail handle
        end
      end

      def client_id
        @client_id ||= `uuidgen`.strip
      end

    end

  end
end
