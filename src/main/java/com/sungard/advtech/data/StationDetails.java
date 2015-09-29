package com.sungard.advtech.data;

public class StationDetails implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    float lat;
    float lon;
    String name;
    public float getLat() {
        return lat;
    }
    public void setLat(float lat) {
        this.lat = lat;
    }
    public float getLon() {
        return lon;
    }
    public void setLon(float lon) {
        this.lon = lon;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public static StationDetails of(String name, float lat, float lon) {
        StationDetails sd = new StationDetails();
        sd.name = name;
        sd.lat = lat;
        sd.lon = lon;
        return sd;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Float.floatToIntBits(lat);
        result = prime * result + Float.floatToIntBits(lon);
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StationDetails other = (StationDetails) obj;
        if (Float.floatToIntBits(lat) != Float.floatToIntBits(other.lat))
            return false;
        if (Float.floatToIntBits(lon) != Float.floatToIntBits(other.lon))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }
    @Override
    public String toString() {
        return String.format("%s: %+09.4f|%+09.4f", name, lat, lon);
    }
}
