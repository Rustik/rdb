# the set of attributes
module Rdb
  class Document

    attr_accessor :attributes, :fields, :indexes, :collection_name, :path_to_database#, :last_id

    def initialize attrs, options = {}
      @attributes = attrs
      @fields = options[:fields]
      @indexes = options[:indexes]
      @collection_name = options[:collection_name]
      @path_to_database = options[:path_to_database]
    end

    def store!
      indexes.inject({}) do |result, index_hash|
        field_name, index_class = index_hash.flatten
        index = index_class.new field_name, attributes, 
          collection_name: collection_name, 
          path_to_database: path_to_database

        result[field_name] = index.create!
        result
      end
    end
  end
end