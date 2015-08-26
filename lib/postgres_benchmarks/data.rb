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

        puts 'getting url values to save'
        values_to_save = @event_ids.map do |id|
          print '.1'
           0.upto(@urls-1).map { |index|
             print '.2'
            "(#{id}, #{index}, '#{url_value}')"
          }.join(', ')
        end.join(', ')
        puts 'finished'

        puts 'storing urls'
        @event_url_ids = @db.conn.exec("INSERT INTO urls_relational (event_id, order_id, value) VALUES #{values_to_save} RETURNING event_id, id").values.product(@publisher_ids)
        @event_url_ids.map!(&:flatten)
        puts 'finished'

        @epu_ids = @db.conn.exec("INSERT INTO event_publisher_urls_relational (event_id, url_id, publisher_id, hits_counter) VALUES #{epu_values} RETURNING id;").values.flatten
        # Insert the generated data into the different db formats
        @db.conn.exec("INSERT INTO clickable_referrers_relational (epu_id, hits_counter) VALUES #{referrer_values}")

        @db.conn.exec("INSERT INTO clickable_clicks_by_days_relational (epu_id, hits_counter, day) VALUES #{cbd_values}")

        @db.conn.exec("UPDATE urls_relational SET total_clicks = #{events_total_clicks};")

        # JSONB DATA TYPES BELOW
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
      puts "getting event total clicks\n"
      events_total_clicks ||= referrers.inject(0) { |memo, val|
        print '.'
        memo += val[:clicks]
      }
      puts "finished\n"
      events_total_clicks
    end

    def account_values
      puts 'getting account value'
      values = 1.upto(@events).map {
        print '.'
        "('#{::Faker::Internet.email}')"
      }.join(", ")
      puts 'finished'
      values
    end

    def event_values
      puts "getting event values\n"
      values = 1.upto(@events).map do |id|
        print '.'
        "('#{::Faker::Lorem.words(5, true).join(" ")}', '#{::Faker::Lorem.sentence}', '#{::Faker::Lorem.paragraph(5)}', '#{id}')"
      end.join(', ')
      puts "finished\n"
      values
    end

    def url_value
      ::Faker::Internet.url
    end

    def publisher_values
      puts "getting publisher values\n"
      values = 1.upto(@publishers).map { "('#{::Faker::Lorem.words(2, true).join(" ")}')"}.join(", ")
      puts "finished\n"
      values
    end

    def epu_values
      # creates a publisher association for every event and url combitnation
      puts "associating publisher with event and url combo\n"
      values = @event_url_ids.map { |a|
        print '.'
        "(#{a[0]}, #{a[1]}, #{a[2]}, #{gen_clicks})"
      }.join(", ")
      puts "finished\n"
      values
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
      puts "grouping by event_url\n"
      event_url_grouped = result.group_by {|e|
        print '.1'
        "#{e['event_id']}, #{e['url_id']}"
      }
      puts "finsihed\n"

      puts "getting total clicks\n"
      total_clicks = event_url_grouped.keys.map {|key|
        print '.1'
        event_url_grouped[key].inject(0) {|sum, object|
          print '.2'
          sum += object["day_hits"].to_i
        }
      }
      puts "finished\n"

      # clicks_by_publisher  ---- [{'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 }}, {'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 }}]
      # event_url_grouped = result.group_by {|e| "#{e['event_id']}, #{e['url_id']}" }
      puts "grouping by publisher clicks"
      publisher_clicks_grouped = event_url_grouped.keys.map {|key|
        print '.1'
        event_url_grouped[key].group_by { |e|
          print '.2'
          e['publisher_id']
        }
      }
      puts "finished\n"

      puts "getting clicks by publisher\n"
      clicks_by_publisher = publisher_clicks_grouped.map { |event|
        print '.1'
        event.each_with_object({}) { |(publisher_id,data), h|
          print '.2'
          h[publisher_id] = data.inject(0) {
            |memo, hits| memo += hits['day_hits'].to_i
            print '.3'
          }
        }
      }
      puts "finished\n"


      # clicks_by_day  ---- [{'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 }}, {'clicks_by_publisher' => { ’12’ => 8, ‘1’ => 2 }}]
      # sum up the hits on a URL and event by day
      # event_url_grouped = result.group_by {|e| "#{e['event_id']}, #{e['url_id']}" }
      puts "making clicks by day\n"
      clicks_by_day = event_url_grouped.keys.map { |key|
        print '.1'
        event_url_grouped[key].group_by { |data|
          print '.2'
          data['day']
        }.each_with_object({}) { |(k,v), h|
          print '.3'
          h[k] = v.inject(0) { |memo, hits|
            print '.4'
            memo += hits['day_hits'].to_i
          }
        }
      }
      puts "finished\n"

      count = result.count

      #
      puts "creating godzilla array\n"
      0.upto(event_url_grouped.count-1) do |index|
        print '.1'
        godzilla << {'total_clicks' => total_clicks[index], 'clicks_by_publisher' => clicks_by_publisher[index], 'clicks_by_day' => clicks_by_day[index]}
      end
      puts "finished\n"


      # event_url_grouped.keys.map { |ele| ele.split(", ") }
      puts "making string to input into database.\n"
      return_value = godzilla.map.with_index { |click_data, idx|
        print '.'
        "(#{event_url_grouped.keys[idx].split(", ")[0]}, #{event_url_grouped.keys[idx].split(", ")[1]}, '#{click_data.to_json}')"
      }.join(", ")
      puts "finished\n"

      return_value
      # data = @db.conn.exec(json_select_sql_query) { || "(#{event_id}, #{publisher_id}, #{url_id}, #{hits_counter}, #{clicks_by_referrer}, #{clicks_by_day})" }.join(", ")
    end

    private

    def referrers_as_values(with_id=true)
      puts "collecting referring values\n"
      if with_id
        @referrer_values = @epu_ids.map { |epu_id|
          print '.1'
          referrers.map { |hash|
            print '.2s'
            "(#{epu_id}, #{hash[:clicks]})"
          }.join(",")
        }.join(", ")
      else
        @referrer_values = referrers.map { |hash| "(#{hash[:clicks]})" }.join(",")
      end
      puts "finished\n"
      @referrer_values
    end

    def cbds_as_values(with_id=true)
      puts "collecting cdb values\n"
      if with_id
        @cbd_values = @epu_ids.map { |epu_id|
          print '.1'
          clicks_by_day.map { |hash|
            print '.2'
            "(#{epu_id}, #{hash[:clicks]}, '#{hash[:day]}')"
          }.join(",")
        }.join(", ")
      else
        @cbd_values = clicks_by_day.map { |hash|
          print '.1'
          "(#{hash[:clicks]}, '#{hash[:day]}')"
        }.join(",")
      end
      puts "finished\n"
      @cbd_values
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
