package com.sungard.advtech.data;

public class WeatherAtStation extends StationDetails implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    int year;
    int month;
    int day;
    float meanTemp;
    public int getYear() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }
    public int getMonth() {
        return month;
    }
    public void setMonth(int month) {
        this.month = month;
    }
    public int getDay() {
        return day;
    }
    public void setDay(int day) {
        this.day = day;
    }
    public float getMeanTemp() {
        return meanTemp;
    }
    public void setMeanTemp(float meanTemp) {
        this.meanTemp = meanTemp;
    }
    public static WeatherAtStation of(int year, int month, int day, float meanTemp, StationDetails stationDetails) {
        WeatherAtStation ws = new WeatherAtStation();
        ws.year = year;
        ws.month = month;
        ws.day = day;
        ws.meanTemp = meanTemp;
        ws.lat = stationDetails.lat;
        ws.lon = stationDetails.lon;
        ws.name = stationDetails.name;
        return ws;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + day;
        result = prime * result + Float.floatToIntBits(meanTemp);
        result = prime * result + month;
        result = prime * result + year;
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        WeatherAtStation other = (WeatherAtStation) obj;
        if (day != other.day)
            return false;
        if (Float.floatToIntBits(meanTemp) != Float
                .floatToIntBits(other.meanTemp))
            return false;
        if (month != other.month)
            return false;
        if (year != other.year)
            return false;
        return true;
    }
    @Override
    public String toString() {
        return String.format("%04d-%02d-%02d: %.2f @ %s", year, month, day, meanTemp, super.toString());
    }
}
