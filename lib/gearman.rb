require 'rubygems'
require 'eventmachine'

module Gearman

  require File.dirname(__FILE__) + '/gearman/evented/reactor'
  require File.dirname(__FILE__) + '/gearman/evented/client'
  require File.dirname(__FILE__) + '/gearman/evented/worker'
  require File.dirname(__FILE__) + '/gearman/client'
  require File.dirname(__FILE__) + '/gearman/task'
  require File.dirname(__FILE__) + '/gearman/taskset'
  require File.dirname(__FILE__) + '/gearman/util'
  require File.dirname(__FILE__) + '/gearman/worker'
  require File.dirname(__FILE__) + '/gearman/job'

  require File.dirname(__FILE__) + '/gearman/protocol'


  class InvalidArgsError < Exception
  end

  class NetworkError < Exception
  end

  def log(msg, force = false)
    Util.log(msg, force)
  end

end
