# frozen_string_literal: true

require 'digest/sha1'
class Redis
  module BloomfilterDriver
    # It loads lua script into redis.
    # BF implementation is done by lua scripting
    # The alghoritm is executed directly on redis
    # Credits for lua code goes to Erik Dubbelboer
    # https://github.com/ErikDubbelboer/redis-lua-scaling-bloom-filter
    class Lua
      attr_accessor :redis

      def initialize(options = {})
        @options = options
        @redis = @options[:redis]
        lua_load
      end

      def insert!(data, expire)
        # Adds a new element to the filter. It will create the filter when it doesn't exist yet.
        add(data, expire)
      end

      def insert(data, expire)
        # Does a Check And Set, this will not add the element if it already exist.
        # `insert` will return `false` if the element is added, or `true` if the element was already in the filter.
        # Since we use a scaling filter adding an element using `insert!` might cause the element to exist in multiple parts of the filter at the same time.
        # `insert` prevents this. Using only `insert` the :count key of the filter will accurately count the number of elements added to the filter.
        # Only using `insert` will also lower the number of false positives by a small amount (less duplicates in the filter means less bits set).
        existed = check_and_set(data, expire)
        !existed.zero?
      end

      def include?(key)
        r = @redis.evalsha(@check_fnc_sha, keys: [@options[:key_name]], argv: [@options[:size], @options[:error_rate], key])
        r == 1
      end

      def clear
        @redis.keys("#{@options[:key_name]}:*").each { |k| @redis.del k }
      end

      protected

      # It loads the script inside Redis
      # Taken from https://github.com/ErikDubbelboer/redis-lua-scaling-bloom-filter
      # This is a scalable implementation of BF. It means the initial size can vary
      def lua_load
        add_fnc = File.read File.expand_path("../../vendor/assets/lua/add.lua", __dir__)
        check_fnc = File.read File.expand_path("../../vendor/assets/lua/check.lua", __dir__)
        cas_fnc = File.read File.expand_path("../../vendor/assets/lua/cas.lua", __dir__)

        @add_fnc_sha   = Digest::SHA1.hexdigest(add_fnc)
        @check_fnc_sha = Digest::SHA1.hexdigest(check_fnc)
        @cas_fnc_sha = Digest::SHA1.hexdigest(cas_fnc)

        loaded = @redis.script(:exists, [@add_fnc_sha, @check_fnc_sha, @cas_fnc_sha]).uniq
        return unless loaded.count != 1 || loaded.first != true
        @add_fnc_sha   = @redis.script(:load, add_fnc)
        @check_fnc_sha = @redis.script(:load, check_fnc)
        @cas_fnc_sha = @redis.script(:load, cas_fnc)
      end

      def add(data, expire)
        @redis.evalsha(@add_fnc_sha, keys: [@options[:key_name]], argv: [@options[:size], @options[:error_rate], data, expire])
      end

      def check_and_set(data, expire)
        @redis.evalsha(@cas_fnc_sha, keys: [@options[:key_name]], argv: [@options[:size], @options[:error_rate], data, expire])
      end
    end
  end
end
