require 'faker'

module PostgresBenchmarks
  class Data
    attr_accessor :epu_id, :referrers
    def initialize(events:, publishers:, urls:, num_referrers:, num_days:, times:)
      @urls=urls
      @publishers=publishers
      @num_referrers = num_referrers
      @num_days = num_days
      @db = PostgresBenchmarks::Database::Test.new
    end
    
    def generate
      # id comes back as: [{"id" => "1"}]

      @event_ids = @db.conn.exec("INSERT INTO events_relational (title, summary, description) VALUES #{event_values} RETURNING id;").map{ |r| r["id"] }

      @event_ids.each_with_index do |id|
        @epu_ids = @db.conn.exec("INSERT INTO event_publisher_urls_relational (event_id, url_id) VALUES (#{id},) RETURNING id;")
      end

      # Insert the generated data into the different db formats
      @db.conn.exec("INSERT INTO clickable_referrers_relational (epu_id, hits_counter, referrer_url) VALUES #{referrer_values}")

      @db.conn.exec("INSERT INTO clickable_clicks_by_days_relational (epu_id, hits_counter, day) VALUES #{cbd_values}")

      @db.conn.exec("INSERT INTO event_publisher_urls_jsonb (clicks_by_referrer) VALUES ('#{referrer_json}')")
    end

    def referrers
      @referrers ||= random_referrers
    end

    def clicks_by_day
      @clicks_by_day ||= random_cbds
    end

    def referrer_json
      referrers.to_json
    end

    def referrer_values(with_id=true)
      referrers_as_values(with_id)
    end

    def cbd_json
      clicks_by_day.to_json
    end

    def cbd_values(with_id=true)
      cbds_as_values(with_id)
    end

    def event_values(num_events)
      1.upto(num_events).map { |num| "('#{Faker::Lorem.words(5, true).join(" ")}', '#{Faker::Lorem.words(20, true).join(" ")}', '#{Faker::Lorem.words(50, true).join(" ")}')"}.join(", ")
    end

    private

    def referrers_as_values(with_id=true)
      if with_id
        @referrer_values = referrers.map { |hash| "(#{@epu_id}, #{hash[:clicks]}, '#{hash[:referrer]}')" }.join(",")
      else
        @referrer_values = referrers.map { |hash| "(#{hash[:clicks]}, '#{hash[:referrer]}')" }.join(",")
      end
    end

    def cbds_as_values(with_id=true)
      if with_id
        @cbd_values = clicks_by_day.map { |hash| "(#{@epu_id}, #{hash[:clicks]}, '#{hash[:day]}')" }.join(",")
      else
        @cbd_values = clicks_by_day.map { |hash| "(#{hash[:clicks]}, '#{hash[:day]}')" }.join(",")
      end
    end

    def gen_clicks
      rand(0..100)
    end

    def random_referrers
      Array.new(@num_referrers) { {epu_id: @epu_id, clicks: gen_clicks, referrer: Faker::Internet.url} }
    end

    def random_cbds
      Array.new(@num_days) { {epu_id: @epu_id, clicks: gen_clicks, day: random_day} }
    end

    def random_day
      today = Date.today
      @dates = (today - 60..today).to_a
      @dates = @dates.shuffle
      @dates.pop
    end
  end
end