class Event
  include Mongoid::Document

  has_many :urls, inverse_of: :event
  has_many :publishers
  # field :clicks_by_referrer, type: Set, default:Set.new
  # field :clicks_by_day, type: Set, default: Set.new
  # field :id, type: Integer
end