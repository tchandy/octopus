module Octopus
  module AssociationShardTracking
    class MismatchedShards < StandardError
      attr_reader :record, :current_shard

      def initialize(record, current_shard)
        @record = record
        @current_shard = current_shard
      end

      def message
        [
          "Association Error: Records are from different shards",
          "Record: #{record.inspect}",
          "Current Shard: #{current_shard.inspect}",
          "Current Record Shard: #{record.current_shard.inspect}",
        ].join(" ")
      end
    end

    def self.extended(base)
      base.send(:include, InstanceMethods)
    end

    module InstanceMethods
      def connection_on_association=(record)
        return unless ::Octopus.enabled?
        return if !self.class.connection.respond_to?(:current_shard) || !self.respond_to?(:current_shard)

        if !record.current_shard.nil? && !current_shard.nil? && record.current_shard.to_s != current_shard.to_s
          raise MismatchedShards.new(record, current_shard)
        end

        record.current_shard = self.class.connection.current_shard = current_shard if should_set_current_shard?
      end
    end


    # def has_many(name, scope = nil, **options, &extension)
    #   puts "has_many called with: name=#{name.inspect}, scope=#{scope.inspect}, options=#{options.inspect}"
    #   if options == {} && scope.is_a?(Hash)
    #     default_octopus_opts(scope)
    #   else
    #     default_octopus_opts(options)
    #   end
    #   # Ensure compatibility with Rails' expected argument count
    #   if scope.nil? || scope.is_a?(Hash)
    #     super(name, **options, &extension)
    #   elsif scope.is_a?(Proc)
    #     if extension
    #       super(name, scope, **options, &extension)
    #     else
    #       p "bpk2"
    #       # super(name, **options, &scope)
    #       super(name, scope, **options, &extension)
    #       # super(name, scope, **options, &extension)
    #       # super(name, **options, &scope.to_proc)
    #       # super(name, **options) { scope.call }
    #       # super(name, scope, **options)
    #     end
    #   else
    #     super(name, scope, **options, &extension)
    #   end
    # end


    def has_many(name, *args, &extension)
      # Extract scope and options carefully
      scope = args[0].is_a?(Proc) ? args.shift : nil
      options = args.last.is_a?(Hash) ? args.pop : {}

      # puts "has_many called with: name=#{name.inspect}, scope=#{scope.inspect}, options=#{options.inspect}"

      # Pass correct arguments to ActiveRecord's has_many
      if scope
        super(name, scope, **options, &extension)
      else
        super(name, **options, &extension)
      end
    end

    def has_and_belongs_to_many(association_id, scope = nil, options = {}, &extension)
      assign_octopus_opts(scope, options)
      super
    end

    def default_octopus_opts(options)
      options[:before_add] = [ :connection_on_association=, options[:before_add] ].compact.flatten
      options[:before_remove] = [ :connection_on_association=, options[:before_remove] ].compact.flatten
    end

    def assign_octopus_opts(scope, options)
      if options == {} && scope.is_a?(Hash)
        default_octopus_opts(scope)
      else
        default_octopus_opts(options)
      end
    end

    if Octopus.atleast_rails51?
      def has_and_belongs_to_many(association_id, scope = nil, **options, &extension)
        assign_octopus_opts(scope, options)
        super
      end
    end
  end
end

ActiveRecord::Base.extend(Octopus::AssociationShardTracking)
