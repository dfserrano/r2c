package com.ssrg.r2c;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

import com.ssrg.r2c.coloriented.RecordValue;
import com.ssrg.r2c.migration.HBase;
import com.ssrg.r2c.util.Utils;

public class TestHBase {

	public static void main(String[] args) throws IOException {
		
		HBase hbase = new HBase();
		
		// client
		System.out.println("--------- Get client firstname ------------");
		hbase.get("client", Bytes.toBytes(1), "client", "firstname");
		
		System.out.println("--------- Get client ------------");
		hbase.get("client", Bytes.toBytes(1), "client");
		
		System.out.println("--------- Get client named 'Die*' ------------");
		hbase.scanPrefix("client_firstname", "Die", "client");
		
		System.out.println("--------- Get addresses near 53.5227 113.516 ------------");
		hbase.scanGeohash("addressinfo_lat", "addressinfo", 53.5227, 113.516, 7);
		
		System.out.println("--------- Get a checkin ------------");
		int idClient = 1;
		int idRestaurant = 3;
		long time = 1406201300000l;
		
		byte[] idC = Bytes.toBytes(idClient);
		byte[] idR = Bytes.toBytes(idRestaurant);
		byte[] timestamp = Arrays.copyOfRange(Bytes.toBytes(time), 0, 5);
		int totalBytes = idC.length + idR.length + timestamp.length;

		byte[] byteval = new byte[totalBytes];
		int offset = 0;
		offset = Bytes.putBytes(byteval, offset, idC, 0,
				idC.length);
		offset = Bytes.putBytes(byteval, offset, idR, 0,
				idR.length);
		offset = Bytes.putBytes(byteval, offset, timestamp, 0, timestamp.length);
		
		byte[] clientID = Bytes.toBytes("clientID");
		byte[] lastTimestamp = Arrays.copyOfRange(Bytes.toBytes(time), 5, 8);
		byte[] prefix = new byte[clientID.length + lastTimestamp.length];
		offset = 0;
		offset = Bytes.putBytes(prefix, offset, lastTimestamp, 0,
				lastTimestamp.length);
		offset = Bytes.putBytes(prefix, offset, clientID, 0,
				clientID.length);
		

		hbase.scanPrefix("checkin", byteval, "checkin", prefix);

		hbase.close();
	}
}
