# the collection of documents
module Rdb
  class Collection

    attr_accessor :name, :fields, :indexes, :path_to_database

    PRIMARY_INDEX_NAME = :id
    PRIMARY_INDEX_START_VAL = 0

    # initialize collection with given attributes:
    # @param name
    # @param fields {id: Integer, name: String}
    # @param indexes {id: Rdb::Indexes::Btree, name: Rdb::Indexes::Hash}
    def initialize *args
      @name = args.first[:name]
      @fields = args.first[:fields]
      @indexes = args.first[:indexes]
      @path_to_database = args.first[:path_to_database]
      setup_dirs
    end

    # insert document into collection
    def << attrs
    	primary_index_id = get_last_id(PRIMARY_INDEX_NAME) + 1
      document = Rdb::Document.new attrs.merge(PRIMARY_INDEX_NAME => primary_index_id), {
        fields: fields, 
        indexes: indexes, 
        collection_name: name, 
        path_to_database: path_to_database,
        last_id: primary_index_id}
      raise ::Error, "Not a Document type" unless document.is_a? Rdb::Document
      transaction do 
        document.store!
      end
    end
    alias_method :insert, :<<

    # fetch document from collection
    def [] key, value
      index = indexes[key].new(key, {}, 
          collection_name: name, 
          path_to_database: path_to_database)
      index.search value
    end
    alias_method :select, :[]

    protected

    # for future atomicity
    def transaction
    	yield if block_given?
    end

    def get_last_id index_name
      Rdb::Indexes::Btree.get_last_id(File.join(path_to_database, name, index_name.to_s)) || PRIMARY_INDEX_START_VAL
    end

    def setup_dirs
    	raise ::Error, "Database dir not found" unless Dir.exist? path_to_database
    	paths = []
    	path_to_collection = File.join(path_to_database, name)
      paths << path_to_collection unless Dir.exist? path_to_collection
    	indexes.keys.each do |index_name|
    		path_to_index = File.join(path_to_database, name, index_name.to_s)
      	paths << path_to_index unless Dir.exist? path_to_index
      end

      paths.each { |path| Dir.mkdir(path, 0700) }
    end
  end
end

