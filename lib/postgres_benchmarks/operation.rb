require 'benchmark/ips'
require 'json'
require_relative './database/test'
require_relative './data'

class Operation
  attr_accessor :db, :data
  def initialize(publishers: 10, urls: 10, num_referrers: 50, num_days: 20, times: 50)
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

  private
  def setup
    @data ||= PostgresBenchmarks::Data.new(publishers: @publishers,
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

