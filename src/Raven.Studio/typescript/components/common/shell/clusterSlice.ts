﻿import { createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";

export interface ClusterNode {
    nodeTag: string;
    serverUrl: string;
}

interface ClusterState {
    localNodeTag: string;
    nodes: EntityState<ClusterNode>;
}

const clusterNodesAdapter = createEntityAdapter<ClusterNode>({
    selectId: (node) => node.nodeTag,
});

const nodesSelectors = clusterNodesAdapter.getSelectors();

const selectAllNodes = (store: RootState) => nodesSelectors.selectAll(store.cluster.nodes);
const selectAllNodeTags = (store: RootState) => nodesSelectors.selectIds(store.cluster.nodes) as string[];
const selectNodeByTag = (nodeTag: string) => (store: RootState) =>
    nodesSelectors.selectById(store.cluster.nodes, nodeTag);

const selectLocalNode = (store: RootState) =>
    nodesSelectors.selectById(store.cluster.nodes, store.cluster.localNodeTag);
const selectLocalNodeTag = (store: RootState) => store.cluster.localNodeTag;

const initialState: ClusterState = {
    localNodeTag: "A",
    nodes: clusterNodesAdapter.getInitialState(),
};

export const clusterSlice = createSlice({
    initialState,
    name: "cluster",
    reducers: {
        nodesLoaded: (state, action: PayloadAction<ClusterNode[]>) => {
            const bootstrapped = !action.payload.some((x) => x.nodeTag === "?");
            clusterNodesAdapter.setAll(state.nodes, bootstrapped ? action.payload : []);
        },
        localNodeTagLoaded: (state, action: PayloadAction<string>) => {
            state.localNodeTag = action.payload;
        },
    },
});

export const clusterActions = {
    nodesLoaded: clusterSlice.actions.nodesLoaded,
    localNodeTagLoaded: clusterSlice.actions.localNodeTagLoaded,
};

export const clusterSelectors = {
    allNodes: selectAllNodes,
    allNodeTags: selectAllNodeTags,
    nodeByTag: selectNodeByTag,
    localNode: selectLocalNode,
    localNodeTag: selectLocalNodeTag,
};