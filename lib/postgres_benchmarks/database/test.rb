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
            temp_conn.exec('DROP DATABASE test;')
            temp_conn.exec('CREATE DATABASE test;')
          ensure
            temp_conn.close
          end

          begin
            temp_conn = PG::Connection.connect(dbname: 'test', host: 'localhost')

            temp_conn.exec( "CREATE TABLE event_publisher_urls_relational (id serial NOT NULL, event_id integer, publisher_id integer, url_id integer, hits_counter integer);" )

            temp_conn.exec( "CREATE TABLE clickable_referrers_relational (id serial NOT NULL, hits_counter integer, referrer_url text);" )

            temp_conn.exec( "CREATE TABLE clickable_clicks_by_days_relational (id serial NOT NULL, hits_counter integer, day date);" )
            
            temp_conn.exec( "CREATE TABLE event_publisher_urls_jsonb (id serial NOT NULL, event_id integer, publisher_id integer, url_id integer, hits_counter integer, clicks_by_referrer jsonb, clicks_by_day jsonb );" )
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
