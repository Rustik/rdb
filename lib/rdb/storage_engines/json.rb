require 'json'

module Rdb
  module StorageEngines
    class Json < Rdb::StorageEngine

      def initialize data, filename
        @data = data
        @filename = filename
      end

      def content
        @data.to_json
      end

      def store!
        File.open(@filename, 'w') { |file| file.write(content) }
      end

      def read 
        JSON.parse File.read(@filename)
      end
    end
  end
end