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
      end

      def insert(data, expire = nil)
        evalscript(:add, keys: [@options[:key_name]], argv: [@options[:size], @options[:error_rate], data, expire])
      end

      def include?(key)
        r = evalscript(:check, keys: [@options[:key_name]], argv: [@options[:size], @options[:error_rate], key])
        r == 1
      end

      def clear
        @redis.keys("#{@options[:key_name]}:*").each { |k| @redis.del k }
      end

      protected

      def self.script_cache
        @script_cache ||= Hash.new { |h, k| h[k] = Digest::SHA1.hexdigest(Lua.get_script(k)) }
        @script_cache
      end

      def self.get_script(script)
        File.read File.expand_path("../../vendor/assets/lua/#{script}.lua", __dir__)
      end

      # Optimistically tries to send `evalsha` to the server, if a NOSCRIPT error occurs,
      # loads the script to the redis server and tries again.
      # The scripts are taken from https://github.com/ErikDubbelboer/redis-lua-scaling-bloom-filter
      # This is a scalable implementation of BF. It means the initial size can vary
      def evalscript(script, keys:, argv:)
        begin
          @redis.evalsha(Lua.script_cache[script], keys: keys, argv: argv)
        rescue Redis::CommandError => e
          if e.message =~ /^NOSCRIPT/
            @redis.script(:load, Lua.get_script(script))
            retry
          end
          raise
        end
      end
    end
  end
end
