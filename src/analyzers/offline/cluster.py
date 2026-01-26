"""Cluster Analyzer for Matrix Watcher.

Groups anomalies by temporal proximity to find multi-source events.
"""

import logging
from collections import defaultdict
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class ClusterAnalyzer:
    """Analyzer for clustering anomalies by temporal proximity.
    
    Groups anomalies within ±time_window seconds, builds a graph
    with anomalies as nodes and temporal proximity as edges,
    then identifies connected components as clusters.
    
    Example:
        analyzer = ClusterAnalyzer(time_window=3.0)
        clusters = analyzer.find_clusters(anomalies_df)
        significant = analyzer.get_multi_source_clusters(clusters)
    """
    
    def __init__(
        self,
        time_window: float = 3.0,
        min_cluster_size: int = 2,
        multi_source_threshold: int = 3
    ):
        """Initialize analyzer.
        
        Args:
            time_window: Time window in seconds for grouping (±window)
            min_cluster_size: Minimum anomalies to form a cluster
            multi_source_threshold: Minimum sources for significant cluster
        """
        self.time_window = time_window
        self.min_cluster_size = min_cluster_size
        self.multi_source_threshold = multi_source_threshold
    
    def find_clusters(
        self, 
        anomalies: pd.DataFrame,
        timestamp_col: str = "timestamp",
        source_col: str = "source"
    ) -> list[dict[str, Any]]:
        """Find clusters of temporally proximate anomalies.
        
        Args:
            anomalies: DataFrame with anomaly records
            timestamp_col: Name of timestamp column
            source_col: Name of source column
            
        Returns:
            List of cluster dictionaries
        """
        if anomalies.empty:
            return []
        
        # Sort by timestamp
        sorted_df = anomalies.sort_values(timestamp_col).reset_index(drop=True)
        n = len(sorted_df)
        
        # Build adjacency list (graph)
        adjacency = defaultdict(set)
        
        for i in range(n):
            t_i = sorted_df.loc[i, timestamp_col]
            for j in range(i + 1, n):
                t_j = sorted_df.loc[j, timestamp_col]
                
                # Early exit if beyond window
                if t_j - t_i > self.time_window:
                    break
                
                # Within window - add edge
                adjacency[i].add(j)
                adjacency[j].add(i)
        
        # Find connected components using BFS
        visited = set()
        clusters = []
        
        for start in range(n):
            if start in visited:
                continue
            
            # BFS to find component
            component = []
            queue = [start]
            
            while queue:
                node = queue.pop(0)
                if node in visited:
                    continue
                
                visited.add(node)
                component.append(node)
                
                for neighbor in adjacency[node]:
                    if neighbor not in visited:
                        queue.append(neighbor)
            
            if len(component) >= self.min_cluster_size:
                cluster_data = self._build_cluster_info(
                    sorted_df, component, timestamp_col, source_col
                )
                clusters.append(cluster_data)
        
        # Sort by significance (sources, then size)
        clusters.sort(
            key=lambda c: (c["unique_sources"], c["anomaly_count"]),
            reverse=True
        )
        
        return clusters
    
    def _build_cluster_info(
        self,
        df: pd.DataFrame,
        indices: list[int],
        timestamp_col: str,
        source_col: str
    ) -> dict[str, Any]:
        """Build cluster information dictionary.
        
        Args:
            df: DataFrame with anomalies
            indices: Row indices in cluster
            timestamp_col: Timestamp column name
            source_col: Source column name
            
        Returns:
            Cluster information dictionary
        """
        cluster_df = df.iloc[indices]
        
        timestamps = cluster_df[timestamp_col].tolist()
        sources = cluster_df[source_col].unique().tolist()
        
        return {
            "anomaly_count": len(indices),
            "unique_sources": len(sources),
            "sources": sources,
            "start_time": min(timestamps),
            "end_time": max(timestamps),
            "time_span": max(timestamps) - min(timestamps),
            "is_multi_source": len(sources) >= self.multi_source_threshold,
            "anomalies": cluster_df.to_dict('records'),
            "indices": indices
        }
    
    def get_multi_source_clusters(
        self, 
        clusters: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Filter to only multi-source clusters.
        
        Args:
            clusters: List of cluster dictionaries
            
        Returns:
            Filtered list with only multi-source clusters
        """
        return [c for c in clusters if c["is_multi_source"]]
    
    def rank_clusters(
        self, 
        clusters: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Rank clusters by significance.
        
        Ranking criteria:
        1. Number of unique sources (more = more significant)
        2. Number of anomalies
        3. Time span (shorter = more concentrated)
        
        Args:
            clusters: List of cluster dictionaries
            
        Returns:
            Sorted list with rank added
        """
        # Calculate score for each cluster
        for cluster in clusters:
            # Higher sources and count = higher score
            # Shorter span = higher score (more concentrated)
            span_factor = 1 / (cluster["time_span"] + 0.1)
            cluster["score"] = (
                cluster["unique_sources"] * 10 +
                cluster["anomaly_count"] * 2 +
                span_factor
            )
        
        # Sort by score
        ranked = sorted(clusters, key=lambda c: c["score"], reverse=True)
        
        # Add rank
        for i, cluster in enumerate(ranked):
            cluster["rank"] = i + 1
        
        return ranked
    
    def get_cluster_summary(
        self, 
        clusters: list[dict[str, Any]]
    ) -> pd.DataFrame:
        """Get summary DataFrame of clusters.
        
        Args:
            clusters: List of cluster dictionaries
            
        Returns:
            Summary DataFrame
        """
        if not clusters:
            return pd.DataFrame()
        
        summary_data = []
        for c in clusters:
            summary_data.append({
                "rank": c.get("rank", 0),
                "anomaly_count": c["anomaly_count"],
                "unique_sources": c["unique_sources"],
                "sources": ", ".join(c["sources"]),
                "start_time": c["start_time"],
                "end_time": c["end_time"],
                "time_span": round(c["time_span"], 2),
                "is_multi_source": c["is_multi_source"],
                "score": round(c.get("score", 0), 2)
            })
        
        return pd.DataFrame(summary_data)
    
    def analyze(
        self, 
        anomalies: pd.DataFrame,
        timestamp_col: str = "timestamp",
        source_col: str = "source"
    ) -> dict[str, Any]:
        """Run full cluster analysis.
        
        Args:
            anomalies: DataFrame with anomaly records
            timestamp_col: Timestamp column name
            source_col: Source column name
            
        Returns:
            Analysis results dictionary
        """
        clusters = self.find_clusters(anomalies, timestamp_col, source_col)
        ranked = self.rank_clusters(clusters)
        multi_source = self.get_multi_source_clusters(ranked)
        summary = self.get_cluster_summary(ranked)
        
        return {
            "clusters": ranked,
            "multi_source_clusters": multi_source,
            "summary": summary,
            "total_clusters": len(clusters),
            "multi_source_count": len(multi_source),
            "total_anomalies": len(anomalies),
            "time_window": self.time_window
        }
