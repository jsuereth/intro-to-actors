package data

// TODO - Ammenities?
case class Location(address: String, city: String, country: String)
case class Hotel(id: String, name: String, description: String, location: Location)