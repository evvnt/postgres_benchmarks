require 'pg'


module PostgresBenchmarks

  module Database
    class Evvnt
      attr_accessor :conn
      def initialize(args)
        @conn = PG::Connection.connect(dbname: 'evvnt-dev', host: 'localhost')
      end

      def something
        conn.query(' select urls.order_id as index, event_id, urls.value, urls.real_url, urls.bitly_referrers_json, urls.id
                     from urls
                     group by event_id, urls.id, urls.bitly_referrers_json
                     order by event_id asc, urls.id asc;')
      end
    end
  end
end