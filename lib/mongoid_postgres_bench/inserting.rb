require 'benchmark/ips'

class Inserting
  def initialize(urls=3, publishers=38)
    @event = Event.create
    @urls = urls
    @publishers = publishers
    @times = urls*publishers
    generate_data
  end

  def generate_urls
    @times.times do
      @urls << Faker::Internet.url
    end
  end

  def generate_events
    @times.times do
      @urls << Faker::Internet.url
    end
  end

  def run
    Benchmark.ips do |x|
      # Configure the number of seconds used during
      # the warmup phase (default 2) and calculation phase (default 5)
      x.config(:time => @times, :warmup => 2)

      # These parameters can also be configured this way
      # x.time = 5
      # x.warmup = 2

      # Typical mode, runs the block as many times as it can
      x.report("mongo") do
        
      end

      # To reduce overhead, the number of iterations is passed in
      # and the block must run the code the specific number of times.
      # Used for when the workload is very small and any overhead
      # introduces incorrectable errors.
      x.report("addition2") do |times|
      end

      # To reduce overhead even more, grafts the code given into
      # the loop that performs the iterations internally to reduce
      # overhead. Typically not needed, use the |times| form instead.
      x.report("addition3", "1 + 2")

      # Really long labels should be formatted correctly
      x.report("addition-test-long-label") { 1 + 2 }

      # Compare the iterations per second of the various reports!
      x.compare!
    end
  end
  
end

