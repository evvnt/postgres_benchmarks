require 'benchmark/ips'
require 'json'
require_relative './database/test'
require_relative './data'

class Inserting

  def run
    Benchmark.ips do |x|
      # Configure the number of seconds used during
      # the warmup phase (default 2) and calculation phase (default 5)
      x.config(:time => @times, :warmup => 2)

      # Typical mode, runs the block as many times as it can
      x.report("relational_approach") do
        db.conn.exec("INSERT INTO clickable_referrers_relational (hits_counter, referrer_url) VALUES #{@data.referrer_values}")
      end

      # To reduce overhead, the number of iterations is passed in
      # and the block must run the code the specific number of times.
      # Used for when the workload is very small and any overhead
      # introduces incorrectable errors.
      x.report("json_approach") do |times|
        db.conn.exec("INSERT INTO event_publisher_urls_jsonb (clicks_by_referrer) VALUES ('#{@data.referrer_json}')")
      end

      # Compare the iterations per second of the various reports!
      x.compare!
    end
  end

  private
  def setup
    @data ||= PostgresBenchmarks::Data.new(@publishers, @urls, @num_referrers)
    @data.generate
  end
end
