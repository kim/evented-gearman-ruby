module Gearman
  module Evented

    module Reactor
      include EM::Deferrable

      def self.included(mod)
        mod.instance_eval do
          def connect(host, port, opts = {})
            Gearman::Evented::Reactor.connect(host, port, self, opts)
          end
        end
      end

      def self.connect(host, port, reactor, opts = {})
        EM.connect(host, (port || 4730), reactor) do |c|
          c.instance_eval do
            @host = host
            @port = port || 4730
            @opts = opts
          end
        end
      end

      def connected?
        @connected
      end

      def server
        @hostport ||= [ @host, @port ].join(":")
      end

      def connection_completed
        log "connected to #{@host}:#{@port}"
        @connected = true
        @reconnecting = false
        @reconnect = true
        succeed
      end

      def unbind
        log "disconnected from #{@host}:#{@port}"
        @connected = false
        EM.next_tick { reconnect } unless @reconnect
      end

      def disconnect
        log "force disconnect from #{@host}:#{@port}"
        @reconnect = false
        close_connection_after_writing
      end

      def reconnect
        if @reconnecting
          EM.add_timer(@opts[:reconnect_sec] || 30) { reconnect }
          return
        elsif !@reconnect
          return
        else
          @reconnecting = true
          @deferred_status = nil
        end

        log "reconnecting to #{@host}:#{@port}"
        EM.reconnect(@host, @port.to_i, self)
      end

      def send(command, data = nil)
        log "send #{command} #{data.inspect} (#{server})"
        send_data(Gearman::Protocol.encode_request(command, data))
      end

      def log(msg, force = false)
        Gearman::Util.log(msg, force)
      end

      def to_s
        "#{@host}:#{@port}"
      end
    end

  end
end
