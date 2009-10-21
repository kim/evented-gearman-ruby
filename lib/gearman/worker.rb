module Gearman
  class Worker

    attr_reader :abilities

    def initialize(job_servers, opts = {})
      @reactors = []
      @abilities = {}

      @job_servers = Array[*job_servers]

      @opts = opts
    end

    def add_ability(name, timeout = nil, &f)
      remove_ability(name) if @abilities.has_key?(name)
      @abilities[name] = { :callback => f, :timeout => timeout }
    end

    def remove_ability(name)
      @abilities.delete(name)
    end

    def has_ability?(name)
      @abilities.has_key?(name)
    end

    def work
      EM.run do
        @job_servers.each do |hostport|
          host, port = hostport.split(":")
          opts = { :abilities => @abilities }.merge(@opts)
          Gearman::Evented::WorkerReactor.connect(host, port, opts)
        end
      end
    end

    private
      def log(msg)
        Gearman::Util.log msg
      end
  end
end
