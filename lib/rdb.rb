# Top level namespace Rdb
require 'logger'

module Rdb

  ### alias for Collection#open
  def self::open *args
    Rdb::Collection.new *args
  end

  def self::logger
    @logger ||= default_logger
  end

  class << self
    attr_writer :logger
  end  

  private

  def self.default_logger
    logger = Logger.new(STDERR)
    logger.level = Logger::ERROR
    logger
  end  
end

$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'rdb/storage_engine'
require 'rdb/index'
require 'rdb/document'
require 'rdb/collection'
require 'rdb/error'
require 'rdb/version'