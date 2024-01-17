package com.ekkelenkamp.netatmo2wow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class NetatmoTokenFiles {

	private File accessTokenFile;
	private File refreshTokenFile;
	
	private static final String FILE_POSTFIX = ".token";

	public NetatmoTokenFiles(String tokenLocation) {
		accessTokenFile = getTokenFileLocation(tokenLocation, NetatmoTokenType.ACCESS);
		refreshTokenFile = getTokenFileLocation(tokenLocation, NetatmoTokenType.REFRESH);
	}
	
	private File getTokenFileLocation(String tokenLocation, NetatmoTokenType type) {
		if(tokenLocation == null || tokenLocation.isEmpty())
			throw new IllegalArgumentException("TokenLocation is empty");
		
		tokenLocation = tokenLocation.endsWith("/") ? tokenLocation : tokenLocation + "/";
		String path = tokenLocation + type + FILE_POSTFIX;
		File file = new File(path);
		
		if(!file.isAbsolute())
			throw new IllegalArgumentException("File path '" + path + "' is not absolute");
		if(!file.exists())
			throw new IllegalArgumentException("File '" + path + "' does not exist");
		if(!file.canRead() || !file.canWrite())
			throw new IllegalArgumentException("File '" + path + "' cannot be read or written");
		
		return file;
	}
	
	public String readToken(NetatmoTokenType type) {
		String token = null;
		try(Scanner reader = new Scanner(getFile(type)))
		{
			token = reader.nextLine();
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
		return token;
	}
	
	public void writeToken(NetatmoTokenType type, String token) {
		try(FileWriter writer = new FileWriter(getFile(type)))
		{
			writer.write(token);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private File getFile(NetatmoTokenType type) {
		return type.equals(NetatmoTokenType.ACCESS) ? accessTokenFile : refreshTokenFile;
	}
}
