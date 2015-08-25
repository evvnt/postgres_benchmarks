require 'faker'
$root = `pwd`.chomp
load 'lib/postgres_benchmarks/operation.rb'
load 'lib/postgres_benchmarks/inserting.rb'
load 'lib/postgres_benchmarks/updating.rb'
load 'lib/postgres_benchmarks/reading.rb'
load 'lib/postgres_benchmarks/querying.rb'

