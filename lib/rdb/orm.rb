# the ORM mapper, it's not included in main code
#
# Example of usage:
#
=begin

require_relative 'rdb'
require_relative 'rdb/orm'

class Plane
  include Rdb::Orm
  rdb_mount 'planes', {id: Integer, code: String, weight: Integer}, {id: Rdb::Indexes::Btree}, '/home/rustam/rdata'
end

names = %w(rose velvet pink blue charley delta bravo)

300000.times do |x|
  code = "%s%d%s" % [names.sample, rand(1000), names.sample]
  Plane.create(code: code, weight: rand(1000))
end

Plane.find_by(id: 1123)

x = Plane.new
x.code = "superjet3000"
x.weight = 2572
x.save

Plane.find_by(id: x.id)

=end

module Rdb
  module Orm

    def self.included base
      base.include InstanceMethods
      base.extend ClassMethods
    end

    module InstanceMethods
      def save
        result = self.class.create attributes
        result.each do |key, value|
          self.send("#{key}=", value)
        end
      end

      def attributes
        field_names.inject({}) do |result, key|
          result[key] = self.send(key)
          result
        end
      end
    end

    module ClassMethods
      def rdb_mount name, fields, indexes, path_to_database
        @rdb = Rdb.open(
          name: name, 
          fields: fields, 
          indexes: indexes, 
          path_to_database: path_to_database)
        attr_accessor *fields.keys
        define_method(:field_names) { fields.keys }
      end

      # search by key field only
      def find_by condition
        if result = @rdb[*condition.flatten]
          instance = new()
          result.each do |key, value|
            instance.send("#{key}=", value)
          end
          instance
        end
      end

      def create attributes
        @rdb << attributes
      end
    end

  end
end