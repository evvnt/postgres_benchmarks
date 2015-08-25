require 'benchmark/ips'
require 'json'
require_relative './database/test'
require_relative './data'

class Querying < Operation

  def run_average_clicks_by_publisher
    setup
    Benchmark.ips do |x|
      # Configure the number of seconds used during
      # the warmup phase (default 2) and calculation phase (default 5)
      x.config(:time => @times, :warmup => 2)

      # Typical mode, runs the block as many times as it can
      x.report("relational_approach") do
        db.conn.exec(relational_averaging_query)
      end

      # To reduce overhead, the number of iterations is passed in
      # and the block must run the code the specific number of times.
      # Used for when the workload is very small and any overhead
      # introduces incorrectable errors.
      x.report("json_approach") do |times|
        db.conn.exec(json_averaging_query)
      end

      # Compare the iterations per second of the various reports!
      x.compare!
    end
  end

  def averaging_query
    <<-SQL
    SELECT AVG(clickable_referrers_relational.hits_counter), event_publisher_urls_relational.publisher_id
    FROM clickable_referrers_relational
    INNER JOIN event_publisher_urls_relational
      ON event_publisher_urls_relational.id = clickable_referrers_relational.epu_id
    WHERE  event_publisher_urls_relational.publisher_id = 42
    GROUP BY event_publisher_urls_relational.publisher_id;
    SQL
  end

  def averaging_query
    <<-SQL
    SELECT AVG(event_publisher_urls_jsonb.clicks_by_referrer #>> clicks) SET clicks_by_referrer = ('#{@data.cbd_json}')
    SQL
  end
end
