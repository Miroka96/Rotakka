package de.hpi.rotakka.actors.data.graph;

import de.hpi.rotakka.actors.AbstractLoggingActor;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

abstract class AbstractGraphStore extends AbstractLoggingActor {

    @Data
    @AllArgsConstructor
    public static final class Vertex implements Serializable {
        public static final long serialVersionUID = 1L;

    }

    @Data
    @AllArgsConstructor
    public static final class Edge implements Serializable {
        public static final long serialVersionUID = 1L;

    }

    @Data
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
