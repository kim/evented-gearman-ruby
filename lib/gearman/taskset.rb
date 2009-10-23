module Gearman
  class Taskset < ::Array

    def self.create(task_or_taskset)
      [*task_or_taskset]
    end

    alias :add :<<
    alias :add_task :add
  end
end
