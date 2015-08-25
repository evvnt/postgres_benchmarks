require 'benchmark/ips'
require 'json'
require_relative './database/test'
require_relative './data'

class Operation
  attr_accessor :db, :data
  def initialize(events: 10, publishers: 10, urls: 10, num_referrers: 50, num_days: 20, times: 50)
    @events = events
    @urls = urls
    @publishers = publishers
    @num_referrers = num_referrers
    @num_days = num_days
    @times = times

    @db = PostgresBenchmarks::Database::Test.new
  end

  def run
    raise NotImplementedError
  end

  protected
  def setup
    @data ||= PostgresBenchmarks::Data.new(events: @events,
                                           publishers: @publishers,
                                           urls: @urls,
                                           num_referrers: @num_referrers,
                                           num_days: @num_days,
                                           times: @times)
    @data.generate
  end


  def relational_query
    raise NotImplementedError
  end
end
