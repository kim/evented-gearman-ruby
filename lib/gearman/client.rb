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
      timeout ||= 0
      use_em_stop = EM.reactor_running?
      EM.run do
        @taskset = Taskset.create(taskset)

        @job_servers.each do |hostport|
          host, port = hostport.split(":")
          reactor = Gearman::Evented::ClientReactor.connect(host, port, @opts)
          reactor.callback { create_job(@taskset.shift, reactor) }
          @reactors << reactor
        end

        if timeout > 0
          if use_em_stop
            EM.add_timer(timeout) { EM.stop }
          else
            sleep timeout
          end
        else
          Thread.new do
            loop do
              sleep 0.1
              live = 0
              @reactors.each {|reactor| live += 1 if reactor.connected? }
              break if live == 0
            end
          end.join
        end
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
