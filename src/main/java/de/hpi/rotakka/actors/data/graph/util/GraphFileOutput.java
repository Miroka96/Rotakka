package de.hpi.rotakka.actors.data.graph.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class GraphFileOutput {
    final private String slaveIdentifier;
    final private int shardNumber;

    final private String directoryPath;
    final private String verticesPath;
    final private String edgesPath;

    final FileOutputStream vertices;
    final FileOutputStream edges;

    GraphFileOutput(String slaveIdentifier, int shardNumber) {
        this.slaveIdentifier = slaveIdentifier;
        this.shardNumber = shardNumber;

        directoryPath = slaveIdentifier + File.pathSeparator + shardNumber;
        verticesPath = directoryPath + File.pathSeparator + "vertices.json";
        edgesPath = directoryPath + File.pathSeparator + "edges.json";

        File directory = new File(directoryPath);
        try {
            boolean ignore;
            ignore = directory.mkdirs();
            ignore = new File(verticesPath).createNewFile();
            ignore = new File(edgesPath).createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            assert false : "Failed to create vertices or edges shard storage files for shard " + shardNumber +
                    " of slave " + slaveIdentifier + " could not be found";
        }

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(verticesPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            assert false : "Recently created shard storage files for vertices of shard " +
                    shardNumber + " of slave " + slaveIdentifier + " could not be found";
        }
        vertices = fos;

        try {
            fos = new FileOutputStream(edgesPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            assert false : "Recently created shard storage files for edges of shard " +
                    shardNumber + " of slave " + slaveIdentifier + " could not be found";
        }
        edges = fos;
    }

    void close() {
        try {
            vertices.close();
            edges.close();
        } catch (IOException e) {
            e.printStackTrace();
            assert false : "Could not close vertices or edges file of shard storage of shard " +
                    shardNumber + " of slave " + slaveIdentifier;
        }
    }

    void delete() {
        new File(verticesPath).delete();
        new File(edgesPath).delete();
        File shardDirectory = new File(directoryPath);
        assert shardDirectory.isDirectory() : directoryPath + " should be the relative path of a directory  - vertices and edges will be stored in there";
        if (shardDirectory.list().length == 0) {
            shardDirectory.delete();

            File slaveDirectory = new File(slaveIdentifier);
            assert shardDirectory.isDirectory() : directoryPath + " should be the relative path of a directory - shard directories will be stored in there";
            if (slaveDirectory.list().length == 0) {
                slaveDirectory.delete();
            }
        }
    }
}
