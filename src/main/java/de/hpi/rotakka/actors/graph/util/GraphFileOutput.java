package de.hpi.rotakka.actors.graph.util;

import akka.actor.ActorRef;
import akka.event.LoggingAdapter;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.hpi.rotakka.actors.graph.GraphStoreMaster;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Objects;

public class GraphFileOutput {
    final private String slaveIdentifier;
    final private int shardNumber;

    final private String slavePath;
    final private String shardPath;
    final private String verticesPath;
    final private String edgesPath;

    final private FileOutputStream vertices;
    final private FileOutputStream edges;

    private ObjectMapper mapper;

    @NotNull
    private static String actorRefToIdentifier(@NotNull ActorRef actor) {
        // an ActorRef looks like: Actor[akka://default/user/actorName#12312134]
        // the last number can also be negative
        String slaveIdentifier = actor.toString();
        int lastSlashIndex = slaveIdentifier.lastIndexOf('/');
        slaveIdentifier = slaveIdentifier.substring(lastSlashIndex + 1, slaveIdentifier.length() - 1);
        int hashIndex = slaveIdentifier.lastIndexOf('#');
        slaveIdentifier = slaveIdentifier.substring(0, hashIndex) + "-" + slaveIdentifier.substring(hashIndex);
        return slaveIdentifier;
    }

    private final LoggingAdapter log;

    public GraphFileOutput(String prefix, ActorRef slave, int shardNumber, LoggingAdapter log) {
        this(prefix, actorRefToIdentifier(slave), shardNumber, log);
    }

    public GraphFileOutput(String prefix, String slaveIdentifier, int shardNumber, LoggingAdapter log) {
        this.slaveIdentifier = slaveIdentifier;
        this.shardNumber = shardNumber;
        this.log = log;

        slavePath = prefix + File.separator + slaveIdentifier;
        shardPath = slavePath + File.separator + shardNumber;
        verticesPath = shardPath + File.separator + "vertices.json";
        edgesPath = shardPath + File.separator + "edges.json";

        try {
            File directory = new File(shardPath);
            if (directory.mkdirs()) {
                log.debug("Created storage directory " + directory.getAbsolutePath());
            } else {
                log.info(directory.getAbsolutePath() + " already existed");
            }

            File vertices = new File(verticesPath);
            if (vertices.createNewFile()) {
                log.debug("Created vertex storage file " + vertices.getAbsolutePath());
            } else {
                log.info(vertices.getAbsolutePath() + " already existed");
            }

            File edges = new File(edgesPath);
            if (edges.createNewFile()) {
                log.debug("Created edges storage file " + edges.getAbsolutePath());
            } else {
                log.info(edges.getAbsolutePath() + " already existed");
            }
        } catch (IOException e) {
            log.error(e, "Failed to create vertices or edges shard storage files for shard " + shardNumber +
                    " of slave " + slaveIdentifier + " could not be found");
        }

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(verticesPath);
            log.debug("Opened " + new File(verticesPath).getAbsolutePath());
        } catch (FileNotFoundException e) {
            log.error(e, "Recently created shard storage files for vertices of shard " +
                    shardNumber + " of slave " + slaveIdentifier + " could not be found");
        }
        vertices = fos;

        try {
            fos = new FileOutputStream(edgesPath);
            log.debug("Opened " + new File(edgesPath).getAbsolutePath());
        } catch (FileNotFoundException e) {
            log.error(e, "Recently created shard storage files for edges of shard " +
                    shardNumber + " of slave " + slaveIdentifier + " could not be found");
        }
        edges = fos;

        JsonFactory jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        mapper = new ObjectMapper(jsonFactory);
    }

    public void close() {
        try {
            vertices.close();
            log.debug("Closed " + new File(verticesPath).getAbsolutePath());
            edges.close();
            log.debug("Closed " + new File(edgesPath).getAbsolutePath());

        } catch (IOException e) {
            log.error(e, "Could not close vertices or edges file of shard storage of shard " +
                    shardNumber + " of slave " + slaveIdentifier);
        }
    }

    public void delete() {
        File vertices = new File(verticesPath);
        if (vertices.delete()) {
            log.debug("Deleted " + vertices.getAbsolutePath());
        } else {
            log.warning(vertices.getAbsolutePath() + " did not exist");
        }

        File edges = new File(edgesPath);
        if (edges.delete()) {
            log.debug("Deleted " + edges.getAbsolutePath());
        } else {
            log.warning(edges.getAbsolutePath() + " did not exist");
        }

        File shardDirectory = new File(shardPath);
        assert shardDirectory.isDirectory() : shardPath + " should be the relative path of a directory  - vertices and edges will be stored in there";
        if (Objects.requireNonNull(shardDirectory.list()).length == 0) {
            if (shardDirectory.delete()) {
                log.debug("Deleted " + shardDirectory.getAbsolutePath());

                File slaveDirectory = new File(slavePath);
                assert slaveDirectory.isDirectory() : slavePath + " should be the relative path of a directory - shard directories will be stored in there";
                if (Objects.requireNonNull(slaveDirectory.list()).length == 0) {
                    if (slaveDirectory.delete()) {
                        log.debug("Deleted " + slaveDirectory.getAbsolutePath());
                    } else {
                        log.info("Could not delete " + slaveDirectory.getAbsolutePath());
                    }
                }
            } else {
                log.warning("Could not delete " + shardDirectory.getAbsolutePath());
            }
        }
    }

    public void add(@NotNull GraphStoreMaster.Vertex vertex) {
        try {
            mapper.writeValue(vertices, vertex);
            vertices.write('\n');
            vertices.flush();
        } catch (IOException e) {
            log.error(e, "Could not store vertex to shard storage file of shard " +
                    shardNumber + " of slave " + slaveIdentifier);
        }

    }

    public void add(@NotNull GraphStoreMaster.Edge edge) {
        try {
            mapper.writeValue(edges, edge);
            edges.write('\n');
            edges.flush();
        } catch (IOException e) {
            log.error(e, "Could not store edge to shard storage file of shard " +
                    shardNumber + " of slave " + slaveIdentifier);
        }
    }

    public void add(@NotNull GraphStoreMaster.SubGraph subGraph) {
        if (subGraph.getEdges() != null) {
            for (GraphStoreMaster.Edge e : subGraph.getEdges()) {
                add(e);
            }
        }
        if (subGraph.getVertices() != null) {
            for (GraphStoreMaster.Vertex v : subGraph.getVertices()) {
                add(v);
            }
        }
    }
}
