# the index interface
module Rdb
  class Index
    def create!
      false
    end
  end
end

require 'rdb/indexes/btree'
require 'rdb/indexes/hash'