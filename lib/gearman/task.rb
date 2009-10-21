module Gearman
  class Task

    attr_reader :name, :payload, :retries_done
    attr_accessor :retries, :priority, :background, :poll_status_interval

    def initialize(name, payload = nil, opts = {})
      @name       = name.to_s
      @payload    = payload || ''
      @priority   = opts.delete(:priority).to_sym rescue nil
      @background = opts.delete(:background) ? true : false

      @retries_done = 0
      @retries      = opts.delete(:retries) || 0

      @poll_status_interval = opts.delete(:poll_status_interval)
      @uniq = opts.has_key?(:uuid) ? opts.delete(:uuid) : `uuidgen`.strip
    end

    ##
    # Set a block of code to be executed when this task completes
    # successfully.  The returned data will be passed to the block.
    def on_complete(&f)
      @on_complete = f
    end

    ##
    # Set a block of code to be executed when this task fails.
    def on_fail(&f)
      @on_fail = f
    end

    ##
    # Set a block of code to be executed when this task is retried after
    # failing.  The number of retries that have been attempted (including the
    # current one) will be passed to the block.
    def on_retry(&f)
      @on_retry = f
    end

    ##
    # Set a block of code to be executed when a remote exception is sent by a worker.
    # The block will receive the message of the exception passed from the worker.
    # The user can return true for retrying or false to mark it as finished
    #
    # NOTE: this is actually deprecated, cf. https://bugs.launchpad.net/gearmand/+bug/405732
    #
    def on_exception(&f)
      @on_exception = f
    end

    ##
    # Set a block of code to be executed when we receive a status update for
    # this task.  The block will receive two arguments, a numerator and
    # denominator describing the task's status.
    def on_status(&f)
      @on_status = f
    end

    ##
    # Set a block of code to be executed when we receive a warning from a worker.
    # It is recommended for workers to send work_warning, followed by work_fail if
    # an exception occurs on their side. Don't expect this behavior from workers NOT
    # using this very library ATM, though. (cf. https://bugs.launchpad.net/gearmand/+bug/405732)
    def on_warning(&f)
      @on_warning = f
    end

    ##
    # Set a block of code to be executed when we receive a (partial) data packet for this task.
    # The data received will be passed as an argument to the block.
    def on_data(&f)
      @on_data = f
    end

    ##
    # Record a failure and check whether we should be retried.
    #
    # @return  true if we should be resubmitted; false otherwise
    def should_retry?
      return false if @retries_done >= @retries
      @retries_done += 1
      true
    end

    def background?
      background
    end

    def dispatch(event, *args)
      callback = instance_variable_get("@#{event}".to_sym)
      callback.call(*args) if callback
    end

    def hash
      @uniq
    end
  end
end
