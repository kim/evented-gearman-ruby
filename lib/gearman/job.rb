module Gearman
  # = Job
  #
  # == Description
  # Interface to allow a worker to report information to a job server.
  class Job
    ##
    # Create a new Job.
    #
    # @param sock    Socket connected to job server
    # @param handle  job server-supplied job handle
    def initialize(client, handle)
      @client = client
      @handle = handle
    end
    ##
    # Report our status to the job server.
    def report_status(numerator, denominator)
      @client.send :work_status, "#{@handle}\0#{numerator}\0#{denominator}"
      self
    end

    ##
    # Send data before job completes
    def send_partial(data)
      @client.send :work_data, "#{@handle}\0#{data}"
      self
    end
    alias :send_data :send_partial

    ##
    # Send a warning explicitly
    def report_warning(warning)
      @client.send :work_warning, "#{@handle}\0#{warning}"
      self
    end
  end
end
