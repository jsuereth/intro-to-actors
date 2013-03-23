package data

// TODO - Ammenities?
case class Location(address: String, city: String, country: String)
case class Hotel(id: String, name: String, description: String, location: Location)
object Hotel {
  // Helper method to auto-generate IDs
  def apply(name: String, description: String, location: Location): Hotel = {
      val id: String = {
        val md = java.security.MessageDigest.getInstance("SHA-512")
        md.update(name.getBytes)
        md.update(description.getBytes)
        md.update(location.address.getBytes)
        md.update(location.city.getBytes)
        md.update(location.country.getBytes)
        hashing digestToHexString md.digest
    }
    new Hotel(id, name, description, location)
  }
}

case class IdList(id: String, ids: Seq[String])