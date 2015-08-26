require 'benchmark/ips'
require 'json'
require_relative './database/test'
require_relative './data'

class Querying < Operation

  # Queries:
  #   with_clicks
  #   total_clicks_for_category_and_publisher
  #   total_clicks_by_account
  #   average_clicks_by_category

  def run_with_clicks
    setup
    Benchmark.ips do |x|
      x.config(time: @times, warmup: 2)

      x.report("relational_approach") do
        db.conn.exec(relational_with_clicks_query)
      end

      x.report("json_approach") do |times|
        db.conn.exec(jsonb_with_clicks_query)
      end

      x.compare!
    end
  end


  def run_average_clicks_by_publisher
    setup
    Benchmark.ips do |x|
      x.config(time: @times, warmup: 2)

      x.report("relational_approach") do
        db.conn.exec(relational_average_clicks_for_publisher_query)
      end

      x.report("json_approach") do |times|
        db.conn.exec(jsonb_average_clicks_for_publisher_query)
      end

      x.compare!
    end
  end

  private

  ## Relational Queries
  def relational_with_clicks_query
    <<-SQL
    SELECT urls_relational.*
    FROM urls_relational
    WHERE urls_relational.total_clicks > 0
    SQL
  end

  def relational_average_clicks_for_publisher_query
    <<-SQL
    SELECT AVG(clickable_referrers_relational.hits_counter), event_publisher_urls_relational.publisher_id
    FROM clickable_referrers_relational
    INNER JOIN event_publisher_urls_relational
      ON event_publisher_urls_relational.id = clickable_referrers_relational.epu_id
    WHERE  event_publisher_urls_relational.publisher_id = 12
    GROUP BY event_publisher_urls_relational.publisher_id;
    SQL
  end

  ## JSONB Queries
  def jsonb_with_clicks_query
    <<-SQL
    SELECT event_publisher_urls_jsonb.*
    FROM urls
    WHERE (event_publisher_urls_jsonb.clicks_data ->> 'total_clicks')::integer > 0
    SQL
  end

  def jsonb_average_clicks_for_publisher_query
    <<-SQL
    SELECT event_id, AVG((clicks_data #>> '{clicks_by_publisher, 12}')::integer)
    FROM event_publisher_urls_jsonb;
    SQL
  end
end
