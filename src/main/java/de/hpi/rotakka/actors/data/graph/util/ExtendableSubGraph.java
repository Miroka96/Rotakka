package de.hpi.rotakka.actors.data.graph.util;

import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;

public class ExtendableSubGraph {
    public ArrayList<GraphStoreMaster.Vertex> vertices = new ArrayList<>();
    public ArrayList<GraphStoreMaster.Edge> edges = new ArrayList<>();

    @Nullable
    public GraphStoreMaster.SubGraph toSubGraph() {
        GraphStoreMaster.Vertex[] vs = null;
        GraphStoreMaster.Edge[] es = null;
        if (vertices.size() > 0) {
            vs = vertices.toArray(new GraphStoreMaster.Vertex[0]);
        }
        if (edges.size() > 0) {
            es = edges.toArray(new GraphStoreMaster.Edge[0]);
        }
        if (vs == null && es == null) return null;
        return new GraphStoreMaster.SubGraph(vs, es);
    }
}
