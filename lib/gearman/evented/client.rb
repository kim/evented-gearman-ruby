module Gearman
  module Evented

    module ClientReactor
      include Gearman::Evented::Reactor

      def connection_completed
        @cbs_job_created ||= []
        @pending_jobs  = []
        @assigned_jobs = {}
        super
      end

      def receive_data(data)
        packets = Gearman::Protocol.decode_response(data)
        log "received #{packets.size} packet(s) at once"
        log "packets: #{packets.inspect}"
        packets.each do |type, handle, *data|
          dispatch_packet(type, handle, *data)
        end
      end

      def dispatch_packet_callback(&callback)
        @dispatch_packet_callback = callback
      end

      def dispatch_packet(type, handle, *data)
        log "Got #{type.to_s}, #{handle}, #{data.inspect} from #{server}"
        if type == :job_created
          job_created(handle)
          if cb = @cbs_job_created.shift
            cb.call(handle)
          end
        else
          dispatch(type, handle, data)
        end
      end

      def submit_job(task, &cb_job_created)
        cmd = "submit_job"
        cmd << "_#{task.priority}" if [ :high, :low ].include?(task.priority)
        cmd << "_bg" if task.background

        log "#{cmd} #{task.name}, #{task.payload} to #{server}"
        send cmd.to_sym, [ task.name, task.hash, task.payload ].join("\0")
        @pending_jobs << task
        @cbs_job_created << cb_job_created if cb_job_created
      end

      def job_created(handle)
        job = @pending_jobs.shift
        raise ProtocolError, "No job waiting for handle! (#{handle})" unless job
        EM.add_periodic_timer(job.poll_status_interval) { get_status(handle) } if job.poll_status_interval
        return if job.background
        @assigned_jobs[handle] = job
      end

      def get_status(handle)
        send :get_status, handle
      end

      def dispatch(type, handle, args)
        return unless type
        task = @assigned_jobs[handle]
        raise ProtocolError, "No task by that name: #{handle}" unless task

        if :work_fail == type && task.should_retry?
          task.dispatch(:on_retry, task.retries_done)
          submit_job(task)
          return
        end

        task.dispatch(type.to_s.sub("work", "on").to_sym, *args)
        @assigned_jobs.delete(handle) if [:work_complete, :work_fail].include?(type)
        EM.stop if @assigned_jobs.empty?
      end
    end

  end
end
