require 'fileutils'

module Rdb
  module Indexes
    class Btree < Rdb::Index

      attr_accessor :key, :attributes, :collection_name, :min_degree, :path_to_database

      def initialize key, attributes, opts
        @key = key
        @attributes = attributes
        @collection_name = opts[:collection_name]
        @path_to_database = opts[:path_to_database]
        @min_degree = 4
        Rdb.logger.info "Btree#initialize: key %s val %s path %s" % [@key.to_s, @attributes[@key], path_to_index]
      end

      def create!
        insert!
      end

      def insert!
        root_node = Node.new min_degree, path_to_index, attributes, true
        if root_node.full?
          Rdb.logger.debug "Btree#insert! root node full"
          new_root_node = Node.new min_degree, path_to_index, attributes, true
          root_node.move_to! new_root_node
          new_root_node.split_child 0, root_node
          index = 0
          Rdb.logger.debug "Btree#insert! attributes[key] %s" % attributes[key].to_s
          Rdb.logger.debug "Btree#insert! new root node keys %s" % new_root_node.keys.join(' ')
          if new_root_node.keys[0] < attributes[key]
            index += 1
          end
          Rdb.logger.debug "Btree#insert! index for new el %d" % index
          Rdb.logger.debug "Btree#insert! new root node children %s" % new_root_node.children_paths(true).join(' ')
          child_node = new_root_node.children[index]
          child_node.data = attributes
          child_node.insert_non_full attributes[key]
        else
          Rdb.logger.debug "Btree#insert! insert into root node"
          root_node.insert_non_full attributes[key]
        end
        attributes[key]
      end

      def search key
        root_node = Node.new min_degree, path_to_index, attributes, true
        root_node.search key
      end

      def path_to_index
        File.join(path_to_database, collection_name, key.to_s)
      end

      def self::get_last_id path
        return unless Dir.exist? path
        children_and_keys = Dir.children(path)
        children = children_and_keys.select { |fname| File.directory?(File.join(path, fname))}.collect(&:to_i).sort
        if children.size > 0
          get_last_id(File.join(path, children.last.to_s))
        else
          children_and_keys.select { |fname| fname.include? 'data_'}.
            collect { |fname| fname.match(/\d+/)[0].to_i}.compact.max.to_i
        end
      end

      class Node

        attr_accessor :t, :path_to_node, :data, :is_root

        def initialize t, path_to_node, data = nil, is_root = false
          @path_to_node = path_to_node
          @t = t
          @data = data
          @is_root = is_root
          setup_dirs
        end

        def search key
          index = 0
          while index < num_of_keys && key > keys[index]
            index += 1
          end

          if key == keys[index] 
            return Rdb::StorageEngines::Json.new({}, File.join(path_to_node,"data_#{key}.json")).read
          end

          return nil if is_leaf?
          children[index].search key
        end

        def insert key, index
          fname = File.join(path_to_node, "data_#{key}.json")
          Rdb::StorageEngines::Json.new(data, fname).store!
        end

        def insert_non_full key
          Rdb.logger.debug "Btree#insert_non_full key %s" % key.to_s
          Rdb.logger.debug "Btree#insert_non_full all keys: %s" % keys.join(' ')
          index = num_of_keys - 1
          if is_leaf?
            while index >= 0 && keys[index] > key do
              index -= 1
            end
            insert key, index
          else
            while index >= 0 && keys[index] > key do
              index -= 1
            end
            child_path = children_paths[index + 1]
            child_node = Node.new t, child_path, data
            Rdb.logger.debug "Btree#insert_non_full selected child node %s" % child_path
            if child_node.num_of_keys == 2*t - 1
              Rdb.logger.debug "Btree#insert_non_full going to split child node"
              split_child index + 1, child_node
              Rdb.logger.debug "Btree#insert_non_full keys %s index %d" % [keys.join(' '), index]
              if keys[index + 1] < key
                index += 1
              end
              child_path = children_paths(true)[index + 1]
              child_node = Node.new t, child_path, data
            end
            child_node.insert_non_full key
          end
        end

        def split_child index, child_node
          Rdb.logger.debug "Btree#split_child index %d" % index
          Rdb.logger.debug "Btree#split_child child to split path %s" % child_node.path_to_node
          new_child_path = File.join(path_to_node, (index + 1).to_s)
          Rdb.logger.debug "Btree#split_child new child path %s" % new_child_path
          new_child = Node.new t, new_child_path, data
          Rdb.logger.debug "Btree#split_child child node keys %s" % child_node.keys.join(' ')
          child_node.keys.last(t - 1).each do |child_key|
            unless File.join(child_node.path_to_node, "data_%d.json" % child_key) == File.join(new_child.path_to_node, "data_%d.json" % child_key)
              Rdb.logger.debug "Btree#split_child move keys from " + File.join(child_node.path_to_node, "data_%d.json" % child_key) + " to " +
                File.join(new_child.path_to_node, "data_%d.json" % child_key)
              FileUtils.mv File.join(child_node.path_to_node, "data_%d.json" % child_key),
                File.join(new_child.path_to_node, "data_%d.json" % child_key)
            else
                Rdb.logger.debug "Btree#split_child move keys same path, skipped " + File.join(child_node.path_to_node, "data_%d.json" % child_key) + " to " +
                File.join(new_child.path_to_node, "data_%d.json" % child_key)
            end
          end

          Rdb.logger.debug "Btree#split_child going to move children to %s" % new_child_path
          Rdb.logger.debug "Btree#split_child %s" % child_node.children(true).map(&:path_to_node).join(' ')
          Rdb.logger.debug "Btree#split_child last t of them %d" % t
            child_node.children(true).last(t).each do |child|
              child.move_to! new_child
            end

          child_key = child_node.keys[t - 1]
          unless child_node.path_to_node == path_to_node
            Rdb.logger.debug "Btree#split_child finaly move from " + File.join(child_node.path_to_node, "data_%d.json" % child_key) + " to " + 
              File.join(path_to_node, "data_%d.json" % child_key)
            FileUtils.mv File.join(child_node.path_to_node, "data_%d.json" % child_key),
                  File.join(path_to_node, "data_%d.json" % child_key)
          else
            Rdb.logger.debug "Btree#split_child finaly skipped same from " + File.join(child_node.path_to_node, "data_%d.json" % child_key) + " to " + 
              File.join(path_to_node, "data_%d.json" % child_key)
          end

        end

        def children reload = true
          Rdb.logger.debug "Btree#children init"
          return @children if defined?(@children) && !reload
          @children = children_paths(reload).collect { |path| Node.new(t, path)}
        end

        def children_paths reload = false
          return @children_paths if defined?(@children_paths) && !reload
          @children_paths = Dir.children(path_to_node).select { |fname| File.directory?(File.join(path_to_node, fname))}.sort_by(&:to_i).
            collect { |fname| File.join(path_to_node, fname)}
        end

        def next_child_id
          is_leaf? ? 1 : children_paths.last.to_i + 1
        end

        def move_to! node
          next_child_id = (node.is_root || node.is_leaf?) ? 0 : node.children_paths.size
          Rdb.logger.debug "Btree#move_to! next child id children lookup %s " % node.children_paths.join(' ')
          path_to_child_tmp = File.join(node.path_to_node, next_child_id.to_s + ".tmp")
          path_to_child = File.join(node.path_to_node, next_child_id.to_s)
          Rdb.logger.debug "Btree#move_to! of %s to %s" % [path_to_node, path_to_child]
          Dir.mkdir(path_to_child_tmp, 0700) unless Dir.exist? path_to_child_tmp
          Rdb.logger.debug "Btree#move_to! move all children and keys to new dir"
          keys.each do |child_key|
            Rdb.logger.debug "Btree#move_to! move keys from " + File.join(path_to_node, "data_%d.json" % child_key) + " to " +
              File.join(path_to_child_tmp, "data_%d.json" % child_key)
            FileUtils.mv File.join(path_to_node, "data_%d.json" % child_key),
              File.join(path_to_child_tmp, "data_%d.json" % child_key)
          end
          children_paths.each do |child_path|
            Rdb.logger.debug "Btree#move_to! move child from " + child_path + " to " + 
              path_to_child_tmp
            FileUtils.mv child_path,
              path_to_child_tmp
          end
          FileUtils.mv path_to_child_tmp, path_to_child
          Dir.rmdir(path_to_node) rescue nil
          Rdb.logger.debug "Btree#move_to! set path_to_node %s" % path_to_child
          self.path_to_node = path_to_child
        end

        def keys
          Dir.children(path_to_node).select { |fname| fname.include? 'data_'}.
            collect { |fname| fname.match(/\d+/)[0].to_i}.sort
        end

        def full?
          num_of_keys == 2*t - 1
        end

        def num_of_keys
          keys.size
        end

        def is_leaf?
          children_paths(true).size.zero?
        end

        def setup_dirs
          Rdb.logger.debug "Btree#setup_dirs setup dir %s" % path_to_node unless Dir.exist? path_to_node
          Dir.mkdir(path_to_node, 0700) unless Dir.exist? path_to_node
          children_paths
        end
      end
    end
  end
end