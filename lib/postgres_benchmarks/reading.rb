require 'benchmark/ips'
require 'json'
require_relative './database/test'

class Reading
  attr_accessor :db, :clicks_json, :referrer_json, :clicks_values, :referrer_values
  def initialize(num_referrers=50, times=10)
    # @publishers = publishers
    @times = times
    @num_referrers = num_referrers

    @db = PostgresBenchmarks::Database::Test.new
  end

  def clicks_values
    @clicks_values  ||= (JSON.parse(File.open($root+'/lib/postgres_benchmarks/factories/click_data.json').read).
                         map { |hash| "(#{hash['clicks']}, '#{hash['day_start']}')" }.join(","))
  end

  def run
    setup
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
        db.conn.exec("SELECT clicks_by_referrer FROM event_publisher_urls_jsonb WHERE event_publisher_urls_jsonb.id = #{@id}")
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

  private

  def setup
    # id comes back as: [{"id" => "1"}]
    binding.pry
    @id = db.conn.exec("INSERT INTO event_publisher_urls_relational (event_id, url_id) VALUES (1,1) RETURNING id;").first["id"]
    referrers =  random_referrers

    # Insert the generated data into the different db formats
    db.conn.exec("INSERT INTO clickable_referrers_relational (epu_id, hits_counter, referrer_url) VALUES #{as_values(referrers)}")
    db.conn.exec("INSERT INTO event_publisher_urls_jsonb (clicks_by_referrer) VALUES ('#{referrers.to_json}')")
  end


  def as_values(arr)
    @referrer_values ||= arr.map { |hash| "(#{@id}, #{hash[:clicks]}, '#{hash[:referrer]}')" }.join(",")
  end

  def gen_clicks
    rand(0..100)
  end

  def random_referrers
    Array.new(@num_referrers) { {id: @id, clicks: gen_clicks, referrer: Faker::Internet.url} }
  end

  def relational_query
    <<-QUERY
    SELECT * from event_publisher_urls_relational
    INNER JOIN clickable_referrers_relational ON event_publisher_urls_relational.id = clickable_referrers_relational.epu_id
    INNER JOIN clickable_clicks_by_days_relational ON event_publisher_urls_relational.id = clickable_clicks_by_days_relational.epu_id
    WHERE event_publisher_urls_relational.id = #{@id}; 
    QUERY
  end
end

