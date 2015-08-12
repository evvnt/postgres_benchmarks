$root = `pwd`.chomp
require 'faker'
load 'lib/postgres_benchmarks/inserting.rb'
# load 'lib/postgres_benchmarks/updating.rb'
load 'lib/postgres_benchmarks/reading.rb'

