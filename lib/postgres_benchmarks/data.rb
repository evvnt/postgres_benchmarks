module PostgresBenchmarks
  class Data
    attr_accessor :epu_ids, :referrers, :day, :db, :events_total_clicks
    def initialize(events:, publishers:, urls:, num_referrers:, num_days:, times:)
      @events = events
      @urls = urls
      @publishers = publishers
      @num_referrers = num_referrers
      @num_days = num_days
      @db = PostgresBenchmarks::Database::Test.new
      @day
    end

    def generate
      begin
        @account_ids = @db.conn.exec("INSERT INTO accounts (email) VALUES #{account_values} RETURNING id;").map{ |r| r["id"] }

        @event_ids = @db.conn.exec("INSERT INTO events_relational (title, summary, description, account_id) VALUES #{event_values} RETURNING id;").map{ |r| r["id"] }

        @publisher_ids = @db.conn.exec("INSERT INTO publishers_relational (name) VALUES #{publisher_values} RETURNING id;").map{ |r| r["id"] }

        @event_url_ids = []
        @event_ids.each do |id|
          0.upto(@urls-1) do |index|
            @event_url_ids |= @db.conn.exec("INSERT INTO urls_relational (event_id, order_id, value) VALUES (#{id}, #{index}, '#{url_value}') RETURNING event_id, order_id ;").values.product(@publisher_ids)
          end
        end
        @event_url_ids.map!(&:flatten)

        @epu_ids = @db.conn.exec("INSERT INTO event_publisher_urls_relational (event_id, url_id, publisher_id, hits_counter) VALUES #{epu_values} RETURNING id;").values.flatten
        # Insert the generated data into the different db formats
        @db.conn.exec("INSERT INTO clickable_referrers_relational (epu_id, hits_counter) VALUES #{referrer_values}")

        @db.conn.exec("INSERT INTO clickable_clicks_by_days_relational (epu_id, hits_counter, day) VALUES #{cbd_values}")

        @db.conn.exec("UPDATE urls_relational SET total_clicks = #{events_total_clicks};")

        # JSONB DATA TYPES BELOW

        # This method needs to fully populate the row with events, publishers and urls along with their generated click stats - this is unfinished
        @db.conn.exec("INSERT INTO event_publisher_urls_jsonb (event_id, url_id, clicks_data) VALUES #{all_json_data}")
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

    def events_total_clicks
      events_total_clicks ||= referrers.inject(0) { |memo, val| memo += val[:clicks] }

      def account_values
        1.upto(@events).map { "('#{::Faker::Internet.email}')"}.join(", ")
      end

      def event_values
        1.upto(@events).map do |id|
          "('#{::Faker::Lorem.words(5, true).join(" ")}', '#{::Faker::Lorem.sentence}', '#{::Faker::Lorem.paragraph(5)}', '#{id}')"
        end.join(', ')
      end

      def url_value
        ::Faker::Internet.url
      end

      def publisher_values
        1.upto(@publishers).map { "('#{::Faker::Lorem.words(2, true).join(" ")}')"}.join(", ")
      end

      def epu_values
        # creates a publisher association for every event and url combitnation
        @event_url_ids.product(@publisher_ids).map { |f| f.flatten }.map { |a| "(#{a[0]}, #{a[1]}, #{a[2]}, #{gen_clicks})"}.join(", ")
      end

      # finish this method to populate the jsonb table
      def all_json_data


        result = db.conn.exec("SELECT event_id, publisher_id, url_id, clickable_clicks_by_days_relational.day, clickable_clicks_by_days_relational.hits_counter as day_hits
                            FROM event_publisher_urls_relational
                            INNER JOIN clickable_clicks_by_days_relational ON clickable_clicks_by_days_relational.epu_id = event_publisher_urls_relational.id")
        godzilla = []
        #  {
        # 'total_clicks' => 10,
        # 'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 },
        # 'clicks_by_day' => [{ 'clicks' => 10, 'day_start' => '2014-01-26T04:00:00+00:00' }, { 'clicks' => 40, 'day_start' => '2014-01-27T04:00:00+00:00' }]
        #  }


        # total_clicks is sum of clicks by day for records with same event_id  ---- [10, 50, 33]
        event_url_grouped = result.group_by {|e| "#{e['event_id']}, #{e['url_id']}" }
        total_clicks = event_url_grouped.keys.map {|key| event_url_grouped[key].inject(0) {|sum, object| sum += object["day_hits"].to_i } }

        # clicks_by_publisher  ---- [{'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 }}, {'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 }}]
        # event_url_grouped = result.group_by {|e| "#{e['event_id']}, #{e['url_id']}" }
        publisher_clicks_grouped = event_url_grouped.keys.map {|key| event_url_grouped[key].group_by { |e| e['publisher_id'] } }
        clicks_by_publisher = publisher_clicks_grouped.map { |event| event.each_with_object({}) { |(publisher_id,data), h| h[publisher_id] = data.inject(0) { |memo, hits| memo += hits['day_hits'].to_i} } }


        # clicks_by_day  ---- [{'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 }}, {'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 }}]
        # sum up the hits on a URL and event by day
        # event_url_grouped = result.group_by {|e| "#{e['event_id']}, #{e['url_id']}" }
        clicks_by_day = event_url_grouped.keys.map { |key| event_url_grouped[key].group_by { |data| data['day'] }.each_with_object({}) { |(k,v), h| h[k] = v.inject(0) { |memo, hits| memo += hits['day_hits'].to_i } } }

        count = result.count

        #
        0.upto(event_url_grouped.count-1) do |index|
          godzilla << {'total_clicks' => total_clicks[index], 'clicks_by_publisher' => clicks_by_publisher[index], 'clicks_by_day' => clicks_by_day[index]}
        end


        # event_url_grouped.keys.map { |ele| ele.split(", ") }
        godzilla.map.with_index { |click_data, idx| "(#{event_url_grouped.keys[idx].split(", ")[0]}, #{event_url_grouped.keys[idx].split(", ")[1]}, '#{click_data.to_json}')" }.join(", ")

        # data = @db.conn.exec(json_select_sql_query) { || "(#{event_id}, #{publisher_id}, #{url_id}, #{hits_counter}, #{clicks_by_referrer}, #{clicks_by_day})" }.join(", ")
      end

      private

      def referrers_as_values(with_id=true)
        if with_id
          @referrer_values = @epu_ids.map { |epu_id| referrers.map { |hash| "(#{epu_id}, #{hash[:clicks]})" }.join(",") }.join(", ")
        else
          @referrer_values = referrers.map { |hash| "(#{hash[:clicks]})" }.join(",")
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
        Array.new(@num_referrers) { {epu_id: @epu_id, clicks: gen_clicks} }
      end

      def random_cbds
        Array.new(@num_days) { {epu_id: @epu_id, clicks: gen_clicks, day: random_day} }
      end

      def random_day
        @day ||= Date.parse("2012-06-06")
        @day = @day.next_day
      end

      def json_select_sql_query
        <<-SQL
      SELECT event_id, publisher_id, url_id, clickable_clicks_by_days_relational.day, clickable_clicks_by_days_relational.hits_counter as day_hits
      FROM event_publisher_urls_relational
      INNER JOIN clickable_clicks_by_days_relational ON clickable_clicks_by_days_relational.epu_id = event_publisher_urls_relational.id
        SQL
        # res = db.conn.exec("SELECT event_id, publisher_id, url_id, clickable_clicks_by_days_relational.day, clickable_clicks_by_days_relational.hits_counter as day_hits
        # FROM event_publisher_urls_relational
        # INNER JOIN clickable_clicks_by_days_relational ON clickable_clicks_by_days_relational.epu_id = event_publisher_urls_relational.id")
      end
    end
  end
end
