require 'benchmark/ips'
require 'json'
require_relative './database/test'
require_relative './data'

class Updating < Operation

  def run
    setup
    binding.pry
    Benchmark.ips do |x|
      # Configure the number of seconds used during
      # the warmup phase (default 2) and calculation phase (default 5)
      x.config(:time => @times, :warmup => 2)

      # Typical mode, runs the block as many times as it can
      x.report("relational_approach") do
        @data.referrers.each do |referrer|
          db.conn.exec("UPDATE clickable_referrers_relational SET hits_counter = #{referrer[:clicks]}, referrer_url = '#{referrer[:referrer]}' WHERE clickable_referrers_relational.id = #{@data.epu_id}")
        end
        
        @data.clicks_by_day.each do |cbd|
          db.conn.exec("UPDATE clickable_clicks_by_days_relational SET hits_counter = #{cbd[:clicks]}, day = '#{cbd[:day]}' WHERE clickable_clicks_by_days_relational.id = #{@data.epu_id}")
        end
      end

      # To reduce overhead, the number of iterations is passed in
      # and the block must run the code the specific number of times.
      # Used for when the workload is very small and any overhead
      # introduces incorrectable errors.
      x.report("json_approach") do |times|
        db.conn.exec("UPDATE event_publisher_urls_jsonb SET clicks_by_referrer = ('#{@data.cbd_json}')")
        db.conn.exec("UPDATE event_publisher_urls_jsonb SET clicks_by_day = ('#{@data.cbd_json}')")
      end

      # Compare the iterations per second of the various reports!
      x.compare!
    end
  end
end
