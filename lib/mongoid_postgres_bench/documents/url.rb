class Url
  include Mongoid::Document

  attr_accessor :bitly_referrers_json

  belongs_to :event, inverse_of: :url

  field :index, type: Integer
  field :value, type: String
  field :bitly_referrers_json, type: Array
  field :bitly_clicks_by_day_json, type: Array
end