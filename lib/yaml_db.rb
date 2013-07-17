require 'rubygems'
require 'yaml'
require 'active_record'
require 'serialization_helper'
require 'active_support/core_ext/kernel/reporting'
require 'rails/railtie'

module YamlDb
  module Helper
    def self.loader
      YamlDb::Load
    end

    def self.dumper
      YamlDb::Dump
    end

    def self.extension
      "yml"
    end
  end


  module Utils
    def self.chunk_records(records)
      yaml = [ records ].to_yaml
      yaml.sub!(/---\s\n|---\n/, '')
      yaml.sub!('- - -', '  - -')
      yaml
    end

  end

  class Dump < SerializationHelper::Dump

    def self.dump_table_columns(io, table)
      io.write("\n")
      io.write({ table => { 'columns' => table_column_names(table) } }.to_yaml)
    end

    def self.dump_table_records(io, table)
      table_record_header(io)
      column_names = {}
      ActiveRecord::Base.connection.columns(table).each do |c|
        column_names[c.name] = c.null
      end
      each_table_page(table) do |records|
        rows = SerializationHelper::Utils.unhash_records(records, column_names)
        io.write(YamlDb::Utils.chunk_records(records))
      end
    end

    def self.table_record_header(io)
      io.write("  records: \n")
    end

  end

  class Load < SerializationHelper::Load
    def self.load_documents(io, truncate = true)
      YAML.load_documents(io) do |ydoc|
        not_exists_tables = []
        ydoc.keys.each do |table_name|
          next if ydoc[table_name].nil?
          if ActiveRecord::Base.connection.table_exists?(table_name)
            load_table(table_name, ydoc[table_name], truncate)
          else
            not_exists_tables << table_name
          end
        end
        if not_exists_tables.any?
          red_color = "\e[31m"
          default_color = "\e[0m"
          #CLEAR   = "\e[0m"
          #BOLD    = "\e[1m"
          #RED     = "\e[31m"
          #GREEN   = "\e[32m"
          #YELLOW  = "\e[33m"
          #BLUE    = "\e[34m"
          puts "#{red_color}Synch data field because tables: #{not_exists_tables.join(",")} are not exists #{default_color}"
        end
      end
    end
  end

  class Railtie < Rails::Railtie
    rake_tasks do
      load File.expand_path('../tasks/yaml_db_tasks.rake', __FILE__)
    end
  end

end
