class Publisher
  include Mongoid::Document

  belongs_to :event

  field :id, type: Integer
end