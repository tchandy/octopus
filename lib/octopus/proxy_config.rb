module Octopus
  class ProxyConfig
    CURRENT_MODEL_KEY = 'octopus.current_model'.freeze
    CURRENT_SHARD_KEY = 'octopus.current_shard'.freeze
    CURRENT_GROUP_KEY = 'octopus.current_group'.freeze
    CURRENT_SLAVE_GROUP_KEY = 'octopus.current_slave_group'.freeze
    CURRENT_LOAD_BALANCE_OPTIONS_KEY = 'octopus.current_load_balance_options'.freeze
    BLOCK_KEY = 'octopus.block'.freeze
    FULLY_REPLICATED_KEY = 'octopus.fully_replicated'.freeze

    attr_accessor :config, :sharded, :shards, :shards_slave_groups, :slave_groups,
                  :adapters, :replicated, :slaves_load_balancer, :slaves_list, :shards_slave_groups,
                  :slave_groups, :groups, :entire_sharded, :shards_config

    def initialize(config)
      initialize_shards(config)
      initialize_replication(config) if !config.nil? && config['replicated']
    end

    def current_model
      Thread.current[CURRENT_MODEL_KEY]
    end

    def current_model=(model)
      Thread.current[CURRENT_MODEL_KEY] = model.is_a?(ActiveRecord::Base) ? model.class : model
    end

    def current_shard
      Thread.current[CURRENT_SHARD_KEY] ||= Octopus.master_shard
    end

    def current_shard=(shard_symbol)
      if shard_symbol.is_a?(Array)
        self.current_slave_group = nil
        shard_symbol.each { |symbol| fail "Nonexistent Shard Name: #{symbol}" if shards[symbol].nil? }
      elsif shard_symbol.is_a?(Hash)
        hash = shard_symbol
        shard_symbol = hash[:shard]
        slave_group_symbol = hash[:slave_group]
        load_balance_options = hash[:load_balance_options]

        if shard_symbol.nil? && slave_group_symbol.nil?
          fail 'Neither shard or slave group must be specified'
        end

        if shard_symbol.present?
          fail "Nonexistent Shard Name: #{shard_symbol}" if shards[shard_symbol].nil?
        end

        if slave_group_symbol.present?
          if (shards_slave_groups.try(:[], shard_symbol).present? && shards_slave_groups[shard_symbol][slave_group_symbol].nil?) ||
              (shards_slave_groups.try(:[], shard_symbol).nil? && @slave_groups[slave_group_symbol].nil?)
            fail "Nonexistent Slave Group Name: #{slave_group_symbol} in shards config: #{shards_config.inspect}"
          end
        end
        self.current_slave_group = slave_group_symbol
        self.current_load_balance_options = load_balance_options
      else
        fail "Nonexistent Shard Name: #{shard_symbol}" if shards[shard_symbol].nil?
      end

      Thread.current[CURRENT_SHARD_KEY] = shard_symbol
    end

    def current_group
      Thread.current[CURRENT_GROUP_KEY]
    end

    def current_group=(group_symbol)
      # TODO: Error message should include all groups if given more than one bad name.
      [group_symbol].flatten.compact.each do |group|
        fail "Nonexistent Group Name: #{group}" unless has_group?(group)
      end

      Thread.current[CURRENT_GROUP_KEY] = group_symbol
    end

    def current_slave_group
      Thread.current[CURRENT_SLAVE_GROUP_KEY]
    end

    def current_slave_group=(slave_group_symbol)
      Thread.current[CURRENT_SLAVE_GROUP_KEY] = slave_group_symbol
      Thread.current[CURRENT_LOAD_BALANCE_OPTIONS_KEY] = nil if slave_group_symbol.nil?
    end

    def current_load_balance_options
      Thread.current[CURRENT_LOAD_BALANCE_OPTIONS_KEY]
    end

    def current_load_balance_options=(options)
      Thread.current[CURRENT_LOAD_BALANCE_OPTIONS_KEY] = options
    end

    def block
      Thread.current[BLOCK_KEY]
    end

    def block=(block)
      Thread.current[BLOCK_KEY] = block
    end

    def fully_replicated?
      @fully_replicated || Thread.current[FULLY_REPLICATED_KEY]
    end

    # Public: Whether or not a group exists with the given name converted to a
    # string.
    #
    # Returns a boolean.
    def has_group?(group)
      @groups.key?(group.to_s)
    end

    # Public: Retrieves names of all loaded shards.
    #
    # Returns an array of shard names as symbols
    def shard_names
      shards.keys
    end

    def shard_name
      current_shard.is_a?(Array) ? current_shard.first : current_shard
    end

    # Public: Retrieves the defined shards for a given group.
    #
    # Returns an array of shard names as symbols or nil if the group is not
    # defined.
    def shards_for_group(group)
      @groups.fetch(group.to_s, nil)
    end

    def initialize_shards(config)
      @original_config = config

      self.shards = HashWithIndifferentAccess.new
      self.shards_slave_groups = HashWithIndifferentAccess.new
      self.slave_groups = HashWithIndifferentAccess.new
      self.groups = {}
      # self.config = ActiveRecord::Base.connection_pool_without_octopus.spec.config
      self.config = ActiveRecord::Base.connection_pool_without_octopus.db_config.configuration_hash

      unless config.nil?
        self.entire_sharded = config['entire_sharded']
        self.shards_config = config[Octopus.rails_env]
      end

      self.shards_config ||= []

      shards_config.each do |key, value|
        if value.is_a?(String)
          value = resolve_string_connection(value).merge(:octopus_shard => key)
          initialize_adapter(value['adapter'])
          shards[key.to_sym] = connection_pool_for(value, "#{value['adapter']}_connection")
        elsif value.is_a?(Hash) && value.key?('adapter')
          value.merge!(:octopus_shard => key)
          initialize_adapter(value['adapter'])
          shards[key.to_sym] = connection_pool_for(value, "#{value['adapter']}_connection")

          slave_group_configs = value.select do |_k, v|
            structurally_slave_group? v
          end

          if slave_group_configs.present?
            slave_groups = HashWithIndifferentAccess.new
            slave_group_configs.each do |slave_group_name, slave_configs|
              slaves = HashWithIndifferentAccess.new
              slave_configs.each do |slave_name, slave_config|
                shards[slave_name.to_sym] = connection_pool_for(slave_config, "#{value['adapter']}_connection")
                slaves[slave_name.to_sym] = slave_name.to_sym
              end
              slave_groups[slave_group_name.to_sym] = Octopus::SlaveGroup.new(slaves)
            end
            @shards_slave_groups[key.to_sym] = slave_groups
            @sharded = true
          end
        elsif value.is_a?(Hash)
          @groups[key.to_s] = []

          value.each do |k, v|
            fail 'You have duplicated shard names!' if shards.key?(k.to_sym)

            initialize_adapter(v['adapter'])
            config_with_octopus_shard = v.merge(:octopus_shard => k)

            shards[k.to_sym] = connection_pool_for(config_with_octopus_shard, "#{v['adapter']}_connection")
            @groups[key.to_s] << k.to_sym
          end

          if structurally_slave_group? value
            slaves = Hash[@groups[key.to_s].map { |v| [v, v] }]
            @slave_groups[key.to_sym] = Octopus::SlaveGroup.new(slaves)
          end
        end
      end

      shards[:master] ||= ActiveRecord::Base.connection_pool_without_octopus if Octopus.master_shard == :master
    end

    def initialize_replication(config)
      @replicated = true
      if config.key?('fully_replicated')
        @fully_replicated = config['fully_replicated']
      else
        @fully_replicated = true
      end

      @slaves_list = shards.keys.map(&:to_s).sort
      @slaves_list.delete('master')
      @slaves_load_balancer = Octopus.load_balancer.new(@slaves_list)
    end

    def reinitialize_shards
      initialize_shards(@original_config)
    end

    private

    # def connection_pool_for(config, adapter)
    #   if Octopus.rails4?
    #     spec = ActiveRecord::ConnectionAdapters::ConnectionSpecification.new(config.dup, adapter )
    #   else
    #     name = adapter["octopus_shard"]
    #     spec = ActiveRecord::ConnectionAdapters::ConnectionSpecification.new(name, config.dup, adapter)
    #   end
    #
    #   ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec)
    # end

    def connection_pool_for(config, adapter)
      # resolver = ActiveRecord::ConnectionAdapters::ConnectionSpecification::Resolver.new(config)
      # spec = resolver.spec(config)
      # ActiveRecord::Base.connection_handler.establish_connection(spec)
      ActiveRecord::Base.establish_connection(config)
    end

    def resolve_string_connection(spec)
      resolver = ActiveRecord::ConnectionAdapters::ConnectionSpecification::Resolver.new({})
      HashWithIndifferentAccess.new(resolver.spec(spec).config)
    end

    def structurally_slave?(config)
      config.is_a?(Hash) && config.key?('adapter')
    end

    def structurally_slave_group?(config)
      config.is_a?(Hash) && config.values.any? { |v| structurally_slave? v }
    end

    def initialize_adapter(adapter)
      begin
        require "active_record/connection_adapters/#{adapter}_adapter"
      rescue LoadError
        raise "Please install the #{adapter} adapter: `gem install activerecord-#{adapter}-adapter` (#{$ERROR_INFO})"
      end
    end
  end
end
