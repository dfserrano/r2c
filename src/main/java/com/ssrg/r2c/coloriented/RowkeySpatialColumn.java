package com.ssrg.r2c.coloriented;

public class RowkeySpatialColumn extends RowkeyColumn {

	private Column latitude;
	private Column longitude;

	public RowkeySpatialColumn(Column column, Column lat, Column lng) {
		super(column, RowkeyColumn.Encoding.GEOHASH);
		this.name = "lat-lng";
		latitude = lat;
		longitude = lng;
	}

	public Column getLatitude() {
		return latitude;
	}

	public Column getLongitude() {
		return longitude;
	}

}
