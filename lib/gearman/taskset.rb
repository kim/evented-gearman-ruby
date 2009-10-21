module Gearman
  class Taskset

    def self.create(task_or_taskset)
      return task_or_taskset if task_or_taskset.kind_of?(Taskset)
      new([task_or_taskset])
    end

    def initialize(tasks = [])
      @tasks = tasks
    end

    def add(task)
      @tasks << task
      self
    end
    alias :<< :add

    def each
      @tasks.each {|task| yield task }
    end

    def pop
      @tasks.pop
    end

    def shift
      @tasks.shift
    end

    def size
      @tasks.size
    end
  end
end
