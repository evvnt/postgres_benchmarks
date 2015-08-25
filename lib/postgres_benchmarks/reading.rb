require 'benchmark/ips'
require 'json'
require_relative './database/test'
require_relative './data'

class Reading < Operation

  def run
    puts "Setting up the data"
    setup
    puts "Running the benchmark"
    Benchmark.ips do |x|
      # Configure the number of seconds used during
      # the warmup phase (default 2) and calculation phase (default 5)
      x.config(:time => @times, :warmup => 2)

      # These parameters can also be configured this way
      # x.time = 5
      # x.warmup = 2

      # Typical mode, runs the block as many times as it can
      x.report("relational_approach") do
        db.conn.exec(relational_query)
      end

      # To reduce overhead, the number of iterations is passed in
      # and the block must run the code the specific number of times.
      # Used for when the workload is very small and any overhead
      # introduces incorrectable errors.
      x.report("json_approach") do |times|
        db.conn.exec("SELECT clicks_by_referrer FROM event_publisher_urls_jsonb WHERE event_publisher_urls_jsonb.id IN (#{@data.epu_ids.join(", ")})")
      end

      # Compare the iterations per second of the various reports!
      x.compare!
    end
    # PostgresBenchmarks::Database::Test.teardown
  end

  def relational_query
    <<-QUERY
    SELECT * from event_publisher_urls_relational
    INNER JOIN clickable_referrers_relational ON event_publisher_urls_relational.id = clickable_referrers_relational.epu_id
    INNER JOIN clickable_clicks_by_days_relational ON event_publisher_urls_relational.id = clickable_clicks_by_days_relational.epu_id
    WHERE event_publisher_urls_relational.id IN (#{@data.epu_ids.join(", ")});
    QUERY
  end
end

