# frozen_string_literal: true

class Redis
  class Bloomfilter
    VERSION = '1.1.2'
    def self.version
      "redis-bloomfilter version #{VERSION}"
    end
  end
end
