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

      def insert(data)
        set data, 1
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
        add_fnc = "
          local entries   = ARGV[1]
          local precision = tonumber(ARGV[2])
          local hash      = redis.sha1hex(ARGV[3])
          local countkey  = KEYS[1] .. ':count'
          local count     = redis.call('GET', countkey)
          if not count then
            count = 1
          else
            count = count + 1
          end

          local factor = math.ceil((entries + count) / entries)
          -- 0.69314718055995 = ln(2)
          local index  = math.ceil(math.log(factor) / 0.69314718055995)
          local scale  = math.pow(2, index - 1) * entries
          local key    = KEYS[1] .. ':' .. index

          -- Based on the math from: http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
          -- Combined with: http://www.sciencedirect.com/science/article/pii/S0020019006003127
          -- 0.4804530139182 = ln(2)^2
          local bits = math.floor(-(scale * math.log(precision * math.pow(0.5, index))) / 0.4804530139182)

          -- 0.69314718055995 = ln(2)
          local k = math.floor(0.69314718055995 * bits / scale)

          -- This uses a variation on:
          -- 'Less Hashing, Same Performance: Building a Better Bloom Filter'
          -- https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
          local h = { }
          h[0] = tonumber(string.sub(hash, 1 , 8 ), 16)
          h[1] = tonumber(string.sub(hash, 9 , 16), 16)
          h[2] = tonumber(string.sub(hash, 17, 24), 16)
          h[3] = tonumber(string.sub(hash, 25, 32), 16)

          local found = true
          for i=1, k do
            if redis.call('SETBIT', key, (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % bits, 1) == 0 then
              found = false
            end
          end

          -- We only increment the count key when we actually added the item to the filter.
          -- This doesn't mean count is accurate. Since this is a scaling bloom filter
          -- it is possible the item was already present in one of the filters in a lower index.
          -- If you really want to make sure an items isn't added multile times you
          -- can use cas.lua (Check And Set).
          if found == false then
            -- INCR is a little bit faster than SET.
            redis.call('INCR', countkey)
          end
        "

        check_fnc = "
          local entries   = ARGV[1]
          local precision = ARGV[2]
          local count     = redis.call('GET', KEYS[1] .. ':count')

          if not count then
            return 0
          end

          local factor = math.ceil((entries + count) / entries)
          -- 0.69314718055995 = ln(2)
          local index = math.ceil(math.log(factor) / 0.69314718055995)
          local scale = math.pow(2, index - 1) * entries

          local hash = redis.sha1hex(ARGV[3])

          -- This uses a variation on:
          -- 'Less Hashing, Same Performance: Building a Better Bloom Filter'
          -- https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
          local h = { }
          h[0] = tonumber(string.sub(hash, 1 , 8 ), 16)
          h[1] = tonumber(string.sub(hash, 9 , 16), 16)
          h[2] = tonumber(string.sub(hash, 17, 24), 16)
          h[3] = tonumber(string.sub(hash, 25, 32), 16)

          -- Based on the math from: http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
          -- Combined with: http://www.sciencedirect.com/science/article/pii/S0020019006003127
          -- 0.4804530139182 = ln(2)^2
          local maxbits = math.floor((scale * math.log(precision * math.pow(0.5, index))) / -0.4804530139182)

          -- 0.69314718055995 = ln(2)
          local maxk = math.floor(0.69314718055995 * maxbits / scale)
          local b    = { }

          for i=1, maxk do
            table.insert(b, h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)])
          end

          for n=1, index do
            local key    = KEYS[1] .. ':' .. n
            local found  = true
            local scalen = math.pow(2, n - 1) * entries

            -- 0.4804530139182 = ln(2)^2
            local bits = math.floor((scalen * math.log(precision * math.pow(0.5, n))) / -0.4804530139182)

            -- 0.69314718055995 = ln(2)
            local k = math.floor(0.69314718055995 * bits / scalen)

            for i=1, k do
              if redis.call('GETBIT', key, b[i] % bits) == 0 then
                found = false
                break
              end
            end

            if found then
              return 1
            end
          end

          return 0
        "

        @add_fnc_sha   = Digest::SHA1.hexdigest(add_fnc)
        @check_fnc_sha = Digest::SHA1.hexdigest(check_fnc)

        loaded = @redis.script(:exists, [@add_fnc_sha, @check_fnc_sha]).uniq
        if loaded.count != 1 || loaded.first != true
          @add_fnc_sha   = @redis.script(:load, add_fnc)
          @check_fnc_sha = @redis.script(:load, check_fnc)
        end
      end

      def set(data, val)
        @redis.evalsha(@add_fnc_sha, keys: [@options[:key_name]], argv: [@options[:size], @options[:error_rate], data, val])
      end
    end
  end
end
