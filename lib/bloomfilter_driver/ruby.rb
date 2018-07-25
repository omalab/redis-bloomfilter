# frozen_string_literal: true

require 'digest/sha1'
class Redis
  module BloomfilterDriver
    class Ruby
      # Faster Ruby version.
      # This driver should be used if Redis version < 2.6
      attr_accessor :redis
      def initialize(options = {})
        @options = options
      end

      # Insert a new element
      def insert(data, expire = nil)
        set(data, expire)
      end

      def insert!(data, expire = nil)
        set(data, expire)
      end

      # It checks if a key is part of the set
      def include?(key)
        indexes = []
        indexes_for(key).each { |idx| indexes << idx }
        return false if @redis.getbit(@options[:key_name], indexes.shift).zero?

        result = @redis.pipelined do
          indexes.each { |idx| @redis.getbit(@options[:key_name], idx) }
        end

        !result.include?(0)
      end

      # It deletes a bloomfilter
      def clear
        @redis.del @options[:key_name]
      end

      protected

      # Hashing strategy:
      # https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
      def indexes_for(data)
        sha = Digest::SHA1.hexdigest(data.to_s)
        count_key = @options[:key_name] + ':count'
        count = (@redis.get(count_key) || 0).to_i
        count += 1

        factor = ((@options[:size] + count) / @options[:size].to_f).ceil
        index = (Math.log(factor).round(14) / 0.69314718055995).ceil
        scale = (2**(index - 1)) * @options[:size]
        bits = (-(scale * Math.log(@options[:error_rate] * 0.5**index).round(14)) / 0.4804530139182).floor
        k = (0.69314718055995 * bits / scale).floor
        h = []
        h[0] = sha[0...8].to_i(16)
        h[1] = sha[8...16].to_i(16)
        h[2] = sha[16...24].to_i(16)
        h[3] = sha[24...32].to_i(16)

        idxs = []

        k.times do |i|
          v = (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2.0)]) % bits
          idxs << v
        end
        idxs
      end

      def set(key, expire)
        bits_changed = @redis.pipelined do
          indexes_for(key).each { |i| @redis.setbit @options[:key_name], i, 1 }
        end
        found = !bits_changed.include?(0)
        @redis.expire(@options[:key_name], expire) if !found && expire
      end
    end
  end
end
