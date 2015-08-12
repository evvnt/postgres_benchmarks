require 'benchmark/ips'
require 'json'
require_relative './database/test'

class Inserting
  attr_accessor :db, :clicks_json, :referrer_json, :clicks_values, :referrer_values
  def initialize(urls=3, publishers=38)
    @urls = urls
    @publishers = publishers
    @times = urls*publishers

    @referrer_json = File.open($root+'/lib/postgres_benchmarks/factories/referrer_data.json').read
    @clicks_json = File.open($root+'/lib/postgres_benchmarks/factories/click_data.json').read

    @db = PostgresBenchmarks::Database::Test.new
  end

  def escaped_referrer_json
    @escaped_referrer_json ||= @referrer_json
  end

  def escaped_clicks_json
    @escaped_clicks_json ||= @clicks_json.gsub('"', '\"')
  end

  def clicks_values
    @clicks_values  ||= (array = JSON.parse(File.open($root+'/lib/postgres_benchmarks/factories/click_data.json').read)
        array.map { |hash| "(#{hash['clicks']}, '#{hash['day_start']}')" }.join(","))
  end

  def referrer_values
    @referrer_values ||= (array = JSON.parse(File.open($root+'/lib/postgres_benchmarks/factories/referrer_data.json').read)
        array.map { |hash| "(#{hash['clicks']}, '#{hash['referrer']}')" }.join(","))
  end

  def run
    # reset_the_db
    Benchmark.ips do |x|
      # Configure the number of seconds used during
      # the warmup phase (default 2) and calculation phase (default 5)
      x.config(:time => @times, :warmup => 2)

      # These parameters can also be configured this way
      # x.time = 5
      # x.warmup = 2

      # Typical mode, runs the block as many times as it can
      x.report("relational_approach") do
        db.conn.exec("INSERT INTO clickable_referrers_relational (hits_counter, referrer_url) VALUES #{referrer_values}")
      end

      # To reduce overhead, the number of iterations is passed in
      # and the block must run the code the specific number of times.
      # Used for when the workload is very small and any overhead
      # introduces incorrectable errors.
      x.report("json_approach") do |times|
        db.conn.exec("INSERT INTO event_publisher_urls_jsonb (clicks_by_referrer) VALUES ('#{escaped_referrer_json}')")
      end

      # To reduce overhead even more, grafts the code given into
      # the loop that performs the iterations internally to reduce
      # overhead. Typically not needed, use the |times| form instead.
      # x.report("addition3", "1 + 2")

      # Really long labels should be formatted correctly
      # x.report("addition-test-long-label") { 1 + 2 }

      # Compare the iterations per second of the various reports!
      x.compare!
    end
  end
end

