package com.chunkserver;

import com.interfaces.ChunkServerInterface;

/* Added */
import java.io.FileNotFoundException;
import java.nio.file.FileSystems;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.io.File;

/* Names of added functions -
 * updateCounter, createTextFile, createBinFile 
 * */

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Shahram Ghandeharizadeh (Modified by Suveena Thanawala)
 *
 */

public class ChunkServer implements ChunkServerInterface {
	/* File name will need to be changed to be compatible with grader's configuration */
	final static String filePath = "/Users/Suveena/Desktop/TinyFS"; // or C:\\newfile.txt
	public static long counter;

	/**
	 * Initialize the chunk server
	 */
	public ChunkServer() {
		// Create an ArrayList for writing to initial file
		ArrayList<String> initFileList = new ArrayList<String>();
		initFileList.add("0");
		Path initFilePath = Paths.get(filePath + "/initFile.txt");
		// Write/open file -- from StackOverflow
		try {
			Files.write(initFilePath, initFileList, Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* Update counter - separate function for readability
	 * Read initial file's contents & increment counter
	 * */
	public void updateCounter() {
		Scanner sc = null;
		try {
			sc = new Scanner(new File(filePath + "/initFile.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		counter = sc.nextLong() + 1;
	}
	
	/* Modified from Stack Overflow
	 * https://stackoverflow.com/questions/2885173/how-do-i-create-a-file-and-write-to-it-in-java
	 */
	public void createBinFile(String chunkHandle) {
		Path nextPath = Paths.get(chunkHandle);
		byte data[] = new byte[ChunkSize];
		// Create/write to file -- from StackOverflow
		try {
			Files.write(nextPath, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/* Copying from constructor (Modified from Stack Overflow)
	 * https://stackoverflow.com/questions/2885173/how-do-i-create-a-file-and-write-to-it-in-java
	 */
	public void createTextFile(ArrayList<String> initFileList) {
		Path initFilePath = Paths.get(filePath + "/initFile.txt"); // Write the initialization file
		// Create/write -- from StackOverflow
		try {
			Files.write(initFilePath, initFileList, Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Each chunk corresponds to a file. Return the chunk handle of the last chunk
	 * in the file (chunk handle is file path)
	 */
	public String initializeChunk() {
		updateCounter();
		String chunkHandle = filePath + "/" + counter + ".bin";
		
		// "files" keeps track of all files
		ArrayList<String> files = new ArrayList<String>();
		files.add(Long.toString(counter));
		
		createBinFile(chunkHandle);
		createTextFile(files);

		return chunkHandle;
	}

	/**
	 * Write the byte array to the chunk at the specified offset The byte array size
	 * should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {

		// Return false if byte array size > ChunkSize (4KB) -- Not sure if this is right?
		if (payload.length > ChunkSize) {
			return false;
		}
		
	    Path filePath = FileSystems.getDefault().getPath("", ChunkHandle);
    		byte[] fileData;

    		// Attempt to read bytes in given file
	    try {
			fileData = Files.readAllBytes(filePath);
		} catch (IOException e) {
			return false;
		}
	    
	    // Write from offset
	    int index = offset;
	    while (index < payload.length) {
	    		fileData[index] = payload[index];
	    		index++;
	    }

	    // Write the data to file with path @ chunk handle
		Path nextPath = Paths.get(ChunkHandle); 
		try {
			Files.write(nextPath, fileData);
		} catch (IOException e1) {
			return false;
		}
		
		return true;
	}

	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		byte[] dataToReturn = new byte[NumberOfBytes];
		Path filePath = FileSystems.getDefault().getPath("", ChunkHandle);
	    	byte[] fileData = null;
	    	
	    	// Attempt to read file
		try {
			fileData = Files.readAllBytes(filePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Read from offset
	    int index = offset;
	    while (index < NumberOfBytes) {
	    		dataToReturn[index] = fileData[index];
	    		index++;
	    }
	    
	    	return dataToReturn;
	}

}
