module PostgresBenchmarks
  class Data
    attr_accessor :epu_id, :referrers
    def initialize(events:, publishers:, urls:, num_referrers:, num_days:, times:)
      @events = events
      @urls=urls
      @publishers=publishers
      @num_referrers = num_referrers
      @num_days = num_days
      @db = PostgresBenchmarks::Database::Test.new
    end
    
    def generate
      begin
        @event_ids = @db.conn.exec("INSERT INTO events_relational (title, summary, description) VALUES #{event_values} RETURNING id;").map{ |r| r["id"] }

        @publisher_ids = @db.conn.exec("INSERT INTO publishers_relational (name) VALUES #{publisher_values} RETURNING id;").map{ |r| r["id"] }

        @event_url_ids = []
        @event_ids.each do |id|
          0.upto(@urls-1) do |index|
            @event_url_ids |= @db.conn.exec("INSERT INTO urls_relational (event_id, order_id, value) VALUES (#{id}, #{index}, '#{url_value}') RETURNING id, event_id;").values.product(@publisher_ids)
          end
        end
        @event_url_ids.map!(&:flatten)

        @epu_ids = @db.conn.exec("INSERT INTO event_publisher_urls_relational (event_id, url_id, publisher_id, hits_counter) VALUES #{epu_values} RETURNING id;").values.flatten
        # Insert the generated data into the different db formats
        @db.conn.exec("INSERT INTO clickable_referrers_relational (epu_id, hits_counter, referrer_url) VALUES #{referrer_values}")

        @db.conn.exec("INSERT INTO clickable_clicks_by_days_relational (epu_id, hits_counter, day) VALUES #{cbd_values}")

        # This method needs to fully populate the row with events, publishers and urls along with their generated click stats - this is unfinished
        # @db.conn.exec("INSERT INTO event_publisher_urls_jsonb (event_id, publisher_id, url_id, clicks_by_referrer, clicks_by_day) VALUES ('#{all_json_data}')")
      ensure
        @db.conn.close
      end
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

    def event_url_publisher_values
      event_url_publisher_as_values
    end

    def event_values
      1.upto(@events).map { "('#{Faker::Lorem.words(5, true).join(" ")}', '#{Faker::Lorem.sentence}', '#{Faker::Lorem.paragraph(5)}')"}.join(", ")
    end

    def url_value
      Faker::Internet.url
    end

    def publisher_values
      1.upto(@publishers).map { "('#{Faker::Lorem.words(2, true).join(" ")}')"}.join(", ")
    end

    def epu_values
      # creates a publisher association for every event and url combitnation
      @event_url_ids.product(@publisher_ids).map { |f| f.flatten }.map { |a| "(#{a[0]}, #{a[1]}, #{a[2]}, #{gen_clicks})"}.join(", ")
    end

    # finish this method to populate the jsonb table
    # def all_json_data
    #   data = @db.conn.exec("SELECT") { || "(#{event_id}, #{publisher_id}, #{url_id}, #{hits_counter}, #{clicks_by_referrer}, #{clicks_by_day})" }.join(", ")
    # end

    private

    def referrers_as_values(with_id=true)
      if with_id
        @referrer_values = @epu_ids.map { |epu_id| referrers.map { |hash| "(#{epu_id}, #{hash[:clicks]}, '#{hash[:referrer]}')" }.join(",") }.join(", ")
      else
        @referrer_values = referrers.map { |hash| "(#{hash[:clicks]}, '#{hash[:referrer]}')" }.join(",")
      end
    end

    def cbds_as_values(with_id=true)
      if with_id
        @cbd_values = @epu_ids.map { |epu_id| clicks_by_day.map { |hash| "(#{epu_id}, #{hash[:clicks]}, '#{hash[:day]}')" }.join(",") }.join(", ")
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