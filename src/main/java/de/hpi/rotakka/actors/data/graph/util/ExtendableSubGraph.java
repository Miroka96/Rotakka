package de.hpi.rotakka.actors.data.graph.util;

import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;

import java.util.ArrayList;

public class ExtendableSubGraph {
    public ArrayList<GraphStoreMaster.Vertex> vertices = new ArrayList<>();
    public ArrayList<GraphStoreMaster.Edge> edges = new ArrayList<>();

    public GraphStoreMaster.SubGraph toSubGraph() {
        GraphStoreMaster.Vertex[] newVertices = vertices.toArray(new GraphStoreMaster.Vertex[0]);
        GraphStoreMaster.Edge[] newEdges = edges.toArray(new GraphStoreMaster.Edge[0]);
        return new GraphStoreMaster.SubGraph(newVertices, newEdges);
    }
}
