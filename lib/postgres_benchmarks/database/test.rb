require 'pg'

module PostgresBenchmarks

  module Database
    class Test
      attr_accessor :conn
      def initialize(dbname: 'test')
        @conn = PG::Connection.connect(dbname: dbname, host: 'localhost')
      end

      class << self
        def setup
          begin

            # get_table_names
            # table_names= db.conn.exec("select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE'").entries
            temp_conn = PG::Connection.connect(dbname: 'postgres', host: 'localhost')
            temp_conn.exec('DROP DATABASE IF EXISTS test;')
            temp_conn.exec('CREATE DATABASE test;')
          ensure
            temp_conn.close
          end

          begin
            temp_conn = PG::Connection.connect(dbname: 'test', host: 'localhost')

            temp_conn.exec( "CREATE TABLE accounts (id serial NOT NULL, email character varying(255));" )
            temp_conn.exec( "CREATE INDEX index_api_clients_on_id ON accounts USING btree (id);" )

            temp_conn.exec( "CREATE TABLE events_relational (id serial NOT NULL, account_id integer, title character varying(255), description text, summary text);" )
            temp_conn.exec( "CREATE INDEX index_events_relational_on_id ON events_relational USING btree (id);" )

            temp_conn.exec( "CREATE TABLE publishers_relational (id serial NOT NULL, name character varying(255));" )
            temp_conn.exec( "CREATE INDEX index_publishers_relational_on_id ON publishers_relational USING btree (id);" )

            temp_conn.exec( "CREATE TABLE urls_relational (id serial NOT NULL, value character varying(255), order_id integer, event_id integer, total_clicks integer);" )
            temp_conn.exec( "CREATE INDEX index_urls_relational_on_id ON urls_relational USING btree (id);" )

            temp_conn.exec( "CREATE INDEX index_urls_relational_on_event_id ON urls_relational USING btree (id);" )

            temp_conn.exec( "CREATE TABLE event_publisher_urls_relational (id serial NOT NULL, event_id integer, publisher_id integer, url_id integer, hits_counter integer);" )
            temp_conn.exec( "CREATE INDEX index_event_publisher_urls_relational_on_id ON event_publisher_urls_relational USING btree (id);" )

            temp_conn.exec( "CREATE TABLE clickable_referrers_relational (id serial NOT NULL, hits_counter integer, epu_id integer);" )
            temp_conn.exec( "CREATE INDEX index_clickable_referrers_relational_on_id ON clickable_referrers_relational USING btree (id);" )

            temp_conn.exec( "CREATE TABLE clickable_clicks_by_days_relational (id serial NOT NULL, hits_counter integer, day date, epu_id integer);" )
            temp_conn.exec( "CREATE INDEX index_clickable_clicks_by_days_relational_on_id ON clickable_clicks_by_days_relational USING btree (id);" )


            temp_conn.exec( "CREATE TABLE event_publisher_urls_jsonb (id serial NOT NULL, event_id integer, url_id integer, clicks_data jsonb );" )
            temp_conn.exec( "CREATE INDEX index_event_publisher_urls_jsonb_on_click_data_clicks_by_day ON event_publisher_urls_jsonb ((clicks_data -> 'clicks_by_day'));" )
            temp_conn.exec( "CREATE INDEX index_event_publisher_urls_jsonb_on_click_data_clicks_by_publisher ON event_publisher_urls_jsonb ((clicks_data -> 'clicks_by_publisher'));" )
            temp_conn.exec( "CREATE INDEX index_event_publisher_urls_jsonb_on_click_data_total_clicks ON event_publisher_urls_jsonb ((clicks_data -> 'total_clicks'));" )
          ensure
            temp_conn.close
          end
        end

        def teardown
          begin
            temp_conn = PG::Connection.connect(dbname: 'postgres', host: 'localhost')
            temp_conn.exec('DROP DATABASE test;')
          ensure
            temp_conn.close
          end
        end
      end
    end
  end
end
