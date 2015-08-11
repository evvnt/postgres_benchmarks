$root = `pwd`.chomp
load 'lib/postgres_jsonb_bench/inserting.rb'# if ARGV.include? '--inserting'
load 'lib/postgres_jsonb_bench/updating.rb' if ARGV.include? '--updating'
load 'lib/postgres_jsonb_bench/reading.rb' if ARGV.include? '--reading'

