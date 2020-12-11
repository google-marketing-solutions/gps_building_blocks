# Lint as: python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Visualize features as a graph.

This module visualizes relationships among features (of an ML model) as a graph.
The input can be any similarity matrix X (N by N) where each row/column
corresponds to a feature, and the value of X_ij corresonds to a similarity score
between feature i and feature j. An edge will be added between feature i and
feature j as long as the value of X_ij is bigger than the specified threshold.

"""
from typing import Dict, List, Optional, Tuple


from absl import logging
import matplotlib.cm
import networkx as nx
import numpy as np
import pandas as pd
from plotly import graph_objects as go

CMAP_TYPE = matplotlib.colors.LinearSegmentedColormap


def cluster_to_sim(clusters: Dict[str, int]) -> pd.DataFrame:
  """Convert a clustering result to a similarity matrix.

  Returns an N (number of features) by N matrix X where both rows and columns
  correspond to the features, and X_ij = 1 if feature i and feature j are in
  the same cluster, otherwise X_ij = 0.

  Args:
    clusters (Dict): {feature_name: cluster_ID} of length N (number of
    features).

  Returns:
    An N by N similarity matrix.
  """
  features = clusters.keys()
  sim_matrix = pd.DataFrame([[0] * len(features)] * len(features))
  sim_matrix.columns = features
  sim_matrix.index = features
  for f1 in sim_matrix.columns:
    for f2 in sim_matrix.index:
      if clusters[f1] == clusters[f2]:
        sim_matrix.loc[f1, f2] = 1
  return sim_matrix


def customized_discrete_colorscale(
    pl_entries: int = 10,
    cmap: CMAP_TYPE = matplotlib.cm.get_cmap('hsv')
) -> List[Tuple[np.float64, str]]:
  """Converts a matplotlib colormap to a discrete color scale.

  Args:
    pl_entries: number of discrete values to assign colors to.
    cmap: a matplotlib cmap object. If None, defaults
      to matplotlib.cm.get_cmap('hsv').

  Returns:
    A discrete color scale as a list of [location on scale, color] (e.g.[(0.1,
    'rgb(255, 0, 0)'), (0.5, 'rgb(255, 147, 0)')])
  """

  if not isinstance(cmap, CMAP_TYPE):
    raise ValueError('Please provide a matplotlib colormap object for cmap.')
  h = 1.0/(pl_entries)
  pl_colorscale = []
  c_order = h * np.arange(pl_entries+1)
  for i in range(pl_entries):
    cs = list(map(np.uint8, np.array(cmap(c_order[i])[:3]) * 255))
    pl_colorscale.append(
        (c_order[i], 'rgb({},{},{})'.format(*cs)))
    # To have clear boundaries between colors in the colorbar
    if i < (pl_entries):
      pl_colorscale.append((c_order[i + 1],
                            'rgb({},{},{})'.format(*cs)))
  return pl_colorscale


def plot_graph(graph: nx.Graph,
               title: str,
               edge_colors: Optional[List[str]] = None,
               edge_widths: Optional[List[float]] = None,
               color_map: str = 'hsv') -> go.Figure:
  """Visualize a graph.

  Hover over each node to see the variable name and the number of edges, i.e.
  number of nodes connected to it.
  Node size is proportional to the number of edges.
  Nodes are colored based on the number of edges.
  Edge colors and edge widths can be customized.

  Args:
    graph: a graph to be visualized.
    title: the title of the graph.
    edge_colors: a list of colors for the edges.
      When None, use 'red' for all edges.
    edge_widths: a list of widths for the edges.
      When None, use 0.5 for all edges.
    color_map: the type of colormap (passed through to matplotlib.cm.get_cmap).

  Returns:
    fig: a plotly figure object.

  """
  pos = nx.spring_layout(graph)
  nx.set_node_attributes(graph, pos, 'pos')

  # Plot the edges
  if edge_colors is None:
    edge_colors = ['red'] * len(graph.edges())
  if edge_widths is None:
    edge_widths = [0.5] * len(graph.edges())
  edge_trace = []
  for edge, c, w in zip(graph.edges(), edge_colors, edge_widths):
    x0, y0 = graph.nodes[edge[0]]['pos']
    x1, y1 = graph.nodes[edge[1]]['pos']
    edge_trace.append(
        go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            line=dict(width=w, color=c),
            hoverinfo='none',
            mode='lines'))

  # Plot the nodes
  node_x = []
  node_y = []
  for node in graph.nodes():
    x, y = graph.nodes[node]['pos']
    node_x.append(x)
    node_y.append(y)

  node_adjacencies = []
  node_text = []
  for node, adjacencies in graph.adjacency():
    node_adjacencies.append(len(adjacencies))
    node_text.append('{}, {} edges'.format(node, len(adjacencies)))
  max_degree = max(node_adjacencies)
  try:
    cmap = matplotlib.cm.get_cmap(color_map)
  except ValueError:
    logging.error('Color map %s is not found!', color_map)
    return None
  custom_colorscale = customized_discrete_colorscale(max_degree, cmap)

  node_trace = go.Scatter(
      x=node_x, y=node_y,
      mode='markers',
      hoverinfo='text',
      marker=dict(
          showscale=True,
          colorscale=custom_colorscale,
          reversescale=True,
          color=node_adjacencies,
          size=[5*a for a in node_adjacencies],
          colorbar=dict(
              thickness=15,
              title='Number of edges',
              xanchor='left',
              titleside='right',
              nticks=max_degree,
          ),
          line_width=1,
          line_color='black'),
      text=node_text)

  # Generate the plot
  fig = go.Figure(
      data=edge_trace + [node_trace],
      layout=go.Layout(
          title=title,
          titlefont_size=16,
          showlegend=False,
          hovermode='closest',
          autosize=False,
          width=600,
          height=600,
      ))
  fig.update_layout(xaxis=dict(showgrid=False,
                               zeroline=False,
                               showticklabels=False),
                    yaxis=dict(showgrid=False,
                               zeroline=False,
                               showticklabels=False))
  return fig


def feature_graph_visualization(sim_matrix: pd.DataFrame,
                                threshold: float = 0.5,
                                color_map: str = 'hsv') -> go.Figure:
  """Visualize features as a graph.

  Hover over each node to see the variable name and the number of edges, i.e.
  number of nodes connected to it with a similarity value above the threshold.
  Node size is proportional to the number of edges.
  Nodes are colored based on the number of edges.
  Edges are color-coded based on the sign of similarity (red for positive, blue
   for negative).

  Args:
    sim_matrix: An N * N dataframe containing pairwise
      similarity values among features, where N is the number of features.
    threshold: Default to 0.5. The threshold for the absolute
      value of similarity.
      An edge is only visualized if the nodes it connects have a similarity
      with an absolute value above this threshold.
    color_map: the type of colormap.

  Returns:
    fig: a plotly figure object containing information to visualize the input
      graph (that can be plotted to output using fig.show()).
  """
  # TODO(): add a check for matrix being positive semidefinite
  if sum(sim_matrix.columns != sim_matrix.index) != 0:
    raise ValueError(
        'Columns and rows of the similarity matrix are not in the same order!')
  features = sim_matrix.columns
  inds = np.argwhere(abs(np.tril(np.array(sim_matrix), -1)) > threshold)
  linked_features = [(features[i1], features[i2]) for i1, i2 in inds]

  # Create graph
  feature_graph = nx.Graph()
  feature_graph.add_edges_from(linked_features)
  pos = nx.spring_layout(feature_graph)
  nx.set_node_attributes(feature_graph, pos, 'pos')
  title = f'Feature cluster (similarity threshold = {threshold})'
  edge_colors = ['red' if sim_matrix.loc[edge_start, edge_end] > 0
                 else 'blue' for edge_start, edge_end in feature_graph.edges()]
  edge_widths = [abs(sim_matrix.loc[edge_start, edge_end]) * 5
                 for edge_start, edge_end in feature_graph.edges()]
  # Plot the graph
  fig = plot_graph(feature_graph, title, edge_colors, edge_widths, color_map)

  return fig


def initialize_colab_import() -> None:
  """Initialize colab import by plotting a random geometric graph.

  This is called within an adhoc_import context to touch all required
  dependencies for this module when imported into a colab notebook.
  """
  graph = nx.random_geometric_graph(10, 1)
  plot_graph(graph, 'Random Geometric Graph')
