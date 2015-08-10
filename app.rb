require 'mongoid'

require_relative 'lib/mongoid_postgres_bench/documents/event'
require_relative 'lib/mongoid_postgres_bench/documents/url'
require_relative 'lib/mongoid_postgres_bench/documents/publisher'

Mongoid.load!('config/mongoid.yml', :development) #, database: :click_stats

load "./seed.rb" if ARGV.include? '--seed'

load 'lib/mongoid_postgres_bench/inserting.rb' if ARGV.include? '--inserting'
load 'lib/mongoid_postgres_bench/updating.rb' if ARGV.include? '--updating'
load 'lib/mongoid_postgres_bench/reading.rb' if ARGV.include? '--reading'

