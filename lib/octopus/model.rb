require 'active_support/deprecation'

module Octopus::Model
  def self.extended(base)
    base.send(:include, InstanceMethods)
    base.extend(ClassMethods)
    base.hijack_connection()
    base.hijack_initializer()
  end

  module SharedMethods
    def clean_table_name
      return unless self.connection_proxy.should_clean_table_name?

      if self != ActiveRecord::Base && self.respond_to?(:reset_table_name) && !self.custom_octopus_table_name
        self.reset_table_name()
      end

      self.reset_column_information
      self.instance_variable_set(:@quoted_table_name, nil)
    end

    def using(shard)
      if Octopus.enabled?
        clean_table_name
        Octopus::ScopeProxy.new(shard, self)
      else
        self
      end
    end

    def hijack_initializer()
      attr_accessor :current_shard
      before_save :reload_connection

      def set_current_shard
        return unless Octopus.enabled?

        if new_record? || self.class.connection_proxy.block
          self.current_shard = self.class.connection_proxy.current_shard
        else
          self.current_shard = self.class.connection_proxy.last_current_shard || self.class.connection_proxy.current_shard
        end
      end

      after_initialize :set_current_shard
    end

    def hijack_connection()
      def self.should_use_normal_connection?
        !Octopus.enabled? || self.custom_octopus_connection
      end

      def self.connection_proxy
        @@connection_proxy ||= Octopus::Proxy.new
      end

      def self.connection_with_octopus
        if should_use_normal_connection?
          connection_without_octopus
        else
          self.connection_proxy.current_model = self
          self.connection_proxy
        end
      end

      def self.connection_pool_with_octopus
        if should_use_normal_connection?
          connection_pool_without_octopus
        else
          connection_proxy.connection_pool
        end
      end

      class << self
        alias_method_chain :connection, :octopus
        alias_method_chain :connection_pool, :octopus
      end
    end
  end

  module InstanceMethods
    include SharedMethods

    def self.included(base)
      base.send(:alias_method, :equality_without_octopus, :==)
      base.send(:alias_method, :==, :equality_with_octopus)
      base.send(:alias_method, :eql?, :==)
      if !Octopus.rails30? and !Octopus.rails31?
        base.send(:alias_method_chain, :perform_validations, :octopus)
      end
    end

    def should_set_current_shard?
      self.respond_to?(:current_shard) && !self.current_shard.nil?
    end

    def reload_connection_safe(&block)
      return yield unless should_set_current_shard?
      original = self.class.connection_proxy.current_shard
      self.class.connection_proxy.current_shard = self.current_shard
      result = yield
      self.class.connection_proxy.current_shard = original
      result
    end

    def reload_connection()
      return unless should_set_current_shard?
      self.class.connection_proxy.current_shard = self.current_shard
    end

    def equality_with_octopus(comparison_object)
      equality_without_octopus(comparison_object) && comparison_object.current_shard == current_shard
    end

    if !Octopus.rails30? and !Octopus.rails31?
      def perform_validations_with_octopus(*args)
        if Octopus.enabled? and should_set_current_shard?
          Octopus.using(self.current_shard) do
            perform_validations_without_octopus(*args)
          end
        else
          perform_validations_without_octopus(*args)
        end
      end
    end
  end

  module ClassMethods
    include SharedMethods

    def self.extended(base)
      base.class_attribute(:replicated)
      base.class_attribute(:sharded)
      base.hijack_methods
    end

    def replicated_model
      self.replicated = true
    end

    def sharded_model
      self.sharded = true
    end

    def hijack_methods
      class << self
        attr_accessor :custom_octopus_connection
        attr_accessor :custom_octopus_table_name

        alias_method_chain(:set_table_name, :octopus)

        if Octopus.rails32?
          def table_name=(value = nil)
            self.custom_octopus_table_name = true
            super
          end
        end
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
      ActiveSupport::Deprecation.warn "Calling `octopus_set_table_name` is deprecated and will be removed in Octopus 1.0.", caller
      set_table_name(value)
    end
  end
end

ActiveRecord::Base.extend(Octopus::Model)
