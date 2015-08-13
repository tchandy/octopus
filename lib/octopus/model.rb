require 'active_support/deprecation'

module Octopus
  module Model
    def self.extended(base)
      base.send(:include, Octopus::ShardTracking::Attribute)
      base.send(:include, InstanceMethods)
      base.extend(ClassMethods)
    end

    module SharedMethods
      def clean_table_name
        return unless connection_proxy.should_clean_table_name?

        if self != ActiveRecord::Base && self.respond_to?(:reset_table_name) && !custom_octopus_table_name
          reset_table_name
        end

        reset_column_information
        instance_variable_set(:@quoted_table_name, nil)
      end

      def using(shard)
        if Octopus.enabled?
          clean_table_name
          Octopus::ScopeProxy.new(shard, self)
        else
          self
        end
      end
    end

    module InstanceMethods
      include SharedMethods

      def self.included(base)
        base.send(:alias_method, :equality_without_octopus, :==)
        base.send(:alias_method, :==, :equality_with_octopus)
        base.send(:alias_method, :eql?, :==)
        base.send(:alias_method_chain, :perform_validations, :octopus)
      end

      def set_current_shard
        return unless Octopus.enabled?

        if new_record? || self.class.connection_proxy.block
          shard = self.class.connection_proxy.current_shard
        else
          shard = self.class.connection_proxy.last_current_shard || self.class.connection_proxy.current_shard
        end

        self.current_shard = shard if self.class.allowed_shard?(shard)
      end

      def should_set_current_shard?
        self.respond_to?(:current_shard) && !current_shard.nil?
      end

      def equality_with_octopus(comparison_object)
        equality_without_octopus(comparison_object) && comparison_object.current_shard == current_shard
      end

      def perform_validations_with_octopus(*args)
        if Octopus.enabled? && should_set_current_shard?
          Octopus.using(current_shard) do
            perform_validations_without_octopus(*args)
          end
        else
          perform_validations_without_octopus(*args)
        end
      end
    end

    module ClassMethods
      include SharedMethods

      def self.extended(base)
        base.class_attribute(:replicated)
        base.class_attribute(:sharded)
        base.class_attribute(:allowed_shards)
        base.hijack_methods
      end

      def replicated_model
        self.replicated = true
      end

      def sharded_model
        self.sharded = true
      end

      def allow_shard(*shards)
        self.allowed_shards ||= []
        self.allowed_shards += shards
      end

      def hijack_methods
        around_save :run_on_shard, :unless => lambda { self.class.custom_octopus_connection }
        after_initialize :set_current_shard

        class << self
          attr_accessor :custom_octopus_connection
          attr_accessor :custom_octopus_table_name

          alias_method_chain :connection, :octopus
          alias_method_chain :connection_pool, :octopus
          alias_method_chain :clear_all_connections!, :octopus
          alias_method_chain :clear_active_connections!, :octopus
          alias_method_chain :connected?, :octopus

          alias_method_chain(:set_table_name, :octopus) if Octopus.rails3?

          def table_name=(value = nil)
            self.custom_octopus_table_name = true
            super
          end
        end
      end

=begin
      def connection_proxy
        ActiveRecord::Base.class_variable_defined?(:@@connection_proxy) &&
          ActiveRecord::Base.class_variable_get(:@@connection_proxy) ||
          ActiveRecord::Base.class_variable_set(:@@connection_proxy, Octopus::Proxy.new)
      end
=end
      # A connection proxy implemented in proxy.rb is the heart and soul of octopus gem. This is
      # the class that handles shards, replicas and chooses the appropriate connection pool. The
      # connection_proxy encapsulate the connection pools for the shards and replicas.
      # The original version of this function would store the connection_proxy as a class variable
      # defined on ActiveRecord::Base. Class variables are inherited.
      # In our code base we have three different databases that we connect to, HR, Recruit
      # and Recruit_Analytics
      # Since original connection_proxy was a class variable, NeoSQL, Recruit::SuperRecord and
      # Recruit::Insights::SuperRecord will point to the same proxy. We want the classes that
      # derive from these classes to have different proxies, so that they can point to different
      # shards/replica.
      # So this patch converts the class variable to class instance variable. For all classes
      # who are in ActiveRecord:: namespace, we use the class instance variable created on
      # ActiveRecord::Base
      # In joint_record, after establish connection, we define the connection_proxy instance variable on
      # NeoSQL, Recruit::SuperRecord and Recruit::Instance::SuperRecord. All classses that derive from these
      # will use the proxy of their respective base class.

      def connection_proxy

        if self.name.start_with?('ActiveRecord::')
          ActiveRecord::Base.instance_variable_defined?(:@connection_proxy) &&
              ActiveRecord::Base.instance_variable_get(:@connection_proxy) ||
              ActiveRecord::Base.instance_variable_set(:@connection_proxy, Octopus::Proxy.new(self))
        else
          proxy = self.instance_variable_get(:@connection_proxy)
          if proxy == nil
            super_class = self.superclass
            while (proxy == nil)
              if super_class.name == 'ActiveRecord::Base'
                ActiveRecord::Base.instance_variable_defined?(:@connection_proxy) &&
                    ActiveRecord::Base.instance_variable_get(:@connection_proxy) ||
                    ActiveRecord::Base.instance_variable_set(:@connection_proxy, Octopus::Proxy.new(super_class))
              end
              proxy = super_class.instance_variable_get(:@connection_proxy)
            end
            # Since this method is called everytime, we make any DB call, we just cache the value at each
            # class level, so only the first call traverses the superclass chain. The subsequent calls
            # return cached value.s
            self.instance_variable_set(:@connection_proxy, proxy)
          end
          proxy
        end

      end


      def should_use_normal_connection?
        if !Octopus.enabled?
          true
        elsif custom_octopus_connection
          !connection_proxy.block || !allowed_shard?(connection_proxy.current_shard)
        end
      end

      def allowed_shard?(shard)
        if custom_octopus_connection
          allowed_shards && shard && allowed_shards.include?(shard)
        else
          true
        end
      end

      def connection_with_octopus
        if should_use_normal_connection?
          connection_without_octopus
        else
          connection_proxy.current_model = self
          connection_proxy
        end
      end

      def connection_pool_with_octopus
        if should_use_normal_connection?
          connection_pool_without_octopus
        else
          connection_proxy.connection_pool
        end
      end

      def clear_active_connections_with_octopus!
        if should_use_normal_connection?
          clear_active_connections_without_octopus!
        else
          connection_proxy.clear_active_connections!
        end
      end

      def clear_all_connections_with_octopus!
        if should_use_normal_connection?
          clear_all_connections_without_octopus!
        else
          connection_proxy.clear_all_connections!
        end
      end

      def connected_with_octopus?
        if should_use_normal_connection?
          connected_without_octopus?
        else
          connection_proxy.connected?
        end
      end

      def set_table_name_with_octopus(value = nil, &block)
        self.custom_octopus_table_name = true
        set_table_name_without_octopus(value, &block)
      end

      def octopus_establish_connection(spec = ENV['DATABASE_URL'])
        self.custom_octopus_connection = true if spec
        establish_connection(spec)
      end

      def octopus_set_table_name(value = nil)
        ActiveSupport::Deprecation.warn 'Calling `octopus_set_table_name` is deprecated and will be removed in Octopus 1.0.', caller
        set_table_name(value)
      end
    end
  end
end

ActiveRecord::Base.extend(Octopus::Model)
