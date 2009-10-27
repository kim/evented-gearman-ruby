module Gearman
  class Client
    attr_accessor :uniq, :jobs

    def initialize(job_servers, opts = {})
      @reactors = []
      @jobs = {}

      @job_servers = Array[*job_servers]

      @uniq = opts.delete(:uniq)
      @opts = opts
    end

    # Run a Task or Taskset
    def run(taskset, timeout = nil)
      EM.run do
        @taskset = Taskset.create(taskset)

        @job_servers.each do |hostport|
          host, port = hostport.split(":")
          reactor = Gearman::Evented::ClientReactor.connect(host, port, @opts)
          reactor.callback { create_job(@taskset.shift, reactor) }
          @reactors << reactor
        end

        EM.add_timer(timeout) { EM.stop_event_loop } if timeout and timeout > 0
      end
    end

    private

      def create_job(task, reactor = nil)
        return unless task
        reactor ||= @reactors[rand(@reactors.size)]
        unless reactor.connected?
          log "create_job: server #{reactor} not connected"
          create_job(task)
          return
        end

        reactor.submit_job(task) {|handle| create_job(@taskset.shift) }
      end

  end
end
