package de.hpi.rotakka.actors.data.graph;

import de.hpi.rotakka.actors.AbstractLoggingActor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;

abstract class AbstractGraphStore extends AbstractLoggingActor {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Vertex implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
        HashMap<String, Object> properties;
    }

    static Vertex merge(Vertex into, Vertex from) {
        into.properties.putAll(from.properties);
        return into;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Edge implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
        String from;
        String to;
        HashMap<String, Object> properties;
    }

    static Edge merge(Edge into, Edge from) {
        into.properties.putAll(from.properties);
        into.from = from.from;
        into.to = from.to;
        return into;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class SubGraph implements Serializable {
        public static final long serialVersionUID = 1L;
        Vertex[] vertices;
        Edge[] edges;
    }

    abstract void add(Vertex vertex);

    abstract void add(Edge edge);

    void add(SubGraph subGraph) {
        if (subGraph.vertices != null) {
            for (Vertex vertex : subGraph.vertices) {
                this.add(vertex);
            }
        }
        if (subGraph.edges != null) {
            for (Edge edge : subGraph.edges) {
                this.add(edge);
            }
        }

    }

}
