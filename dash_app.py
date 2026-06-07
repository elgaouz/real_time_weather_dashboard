"""Real-time weather dashboard: Kafka live (3 cities), Morocco time, 10s refresh."""
from __future__ import annotations

import os
from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dash_table, dcc, html

from hive_data import HIVE_DATABASE, HIVE_HOST, HIVE_PORT, HIVE_TABLE, latest_per_city, load_weather_df
from realtime_kafka import get_snapshot, start_realtime_consumer
from time_utils import now_morocco_str

# Chart + table refresh interval (default 10 seconds).
REFRESH_MS = int(os.getenv("DASH_REFRESH_MS", "10000"))
PORT = int(os.getenv("DASH_PORT", "8050"))
USE_KAFKA = os.getenv("DASH_USE_KAFKA", "true").lower() in ("1", "true", "yes")

# Fixed display order (OpenWeather returns “Marrakesh” for Marrakech query).
CITY_ORDER = ["Rabat", "Casablanca", "Marrakesh"]

app = Dash(__name__)

app.layout = html.Div(
    className="container",
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "20px", "fontFamily": "Segoe UI, sans-serif"},
    children=[
        html.H1(
            "Real-Time Dashboard for Weather Monitoring",
            style={"textAlign": "center", "color": "#1a5fb4", "marginBottom": "8px"},
        ),
        html.P(
            "{ A simple place for knowing your city's weather }",
            style={"textAlign": "center", "color": "#2ec27e", "marginTop": "0"},
        ),
        html.Div(id="refresh-time", style={"textAlign": "center", "margin": "16px 0", "fontSize": "14px"}),
        html.Div(
            style={"display": "flex", "flexWrap": "wrap", "gap": "24px", "justifyContent": "center"},
            children=[
                dcc.Graph(id="bar-chart", style={"flex": "1 1 520px", "minHeight": "400px"}),
                html.Div(
                    style={"flex": "1 1 480px", "minWidth": "320px"},
                    children=[
                        dash_table.DataTable(
                            id="data-table",
                            columns=[
                                {"name": "CityName", "id": "CityName"},
                                {"name": "CreationDate", "id": "CreationDate"},
                                {"name": "CreationTime", "id": "CreationTime"},
                                {"name": "Humidity", "id": "Humidity"},
                                {"name": "Temperature", "id": "Temperature"},
                            ],
                            page_size=3,
                            page_action="none",
                            style_cell={"textAlign": "left", "padding": "8px", "fontSize": "13px"},
                            style_header={"backgroundColor": "#3584e4", "color": "white", "fontWeight": "bold"},
                            style_table={"overflowX": "auto"},
                        ),
                    ],
                ),
            ],
        ),
        dcc.Interval(id="interval", interval=REFRESH_MS, n_intervals=0),
    ],
)


def canonical_city(name: str) -> str:
    n = (name or "").strip().lower()
    if n == "rabat":
        return "Rabat"
    if "casablanca" in n:
        return "Casablanca"
    if "marrake" in n:
        return "Marrakesh"
    return (name or "").strip() or "Unknown"


def three_city_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Latest row per canonical city, exactly 3 rows in fixed order."""
    by_canon: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        c = canonical_city(str(r.get("CityName", "")))
        if c in CITY_ORDER:
            by_canon[c] = r
    out: List[Dict[str, Any]] = []
    for want in CITY_ORDER:
        if want not in by_canon:
            continue
        row = {k: v for k, v in by_canon[want].items() if k != "DashboardReceivedAt"}
        row["CityName"] = want
        ct = str(row.get("CreationTime", ""))
        if ct and not row.get("CreationDate"):
            row["CreationDate"] = ct[:10]
        out.append(row)
    return out


def _figure_from_cities(cities: list, temps: list, hums: list, title_suffix: str) -> go.Figure:
    fig = go.Figure(
        data=[
            go.Bar(name="Temperature", x=cities, y=temps, marker_color="#3584e4"),
            go.Bar(name="Humidity", x=cities, y=hums, marker_color="#ff7800"),
        ]
    )
    fig.update_layout(
        title=f"City's Temperature and Humidity ({title_suffix})",
        barmode="group",
        xaxis_title="City",
        yaxis_title="Value",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


@app.callback(
    Output("refresh-time", "children"),
    Output("bar-chart", "figure"),
    Output("data-table", "data"),
    Input("interval", "n_intervals"),
)
def update_dashboard(_n: int):
    wall_morocco = now_morocco_str()

    if USE_KAFKA:
        start_realtime_consumer()
        snap = get_snapshot()
        latest = snap["latest_rows"]
        table_src = snap["table_rows"]
        err = snap["consumer_error"]

        rows_for_three: List[Dict[str, Any]] = latest if latest else table_src
        three = three_city_rows(rows_for_three)

        if three:
            cities = [str(r["CityName"]) for r in three]
            temps = [float(r["Temperature"]) for r in three]
            hums = [float(r["Humidity"]) for r in three]
            fig = _figure_from_cities(cities, temps, hums, "live from Kafka — Morocco time")
            refresh = (
                f"Current refresh (Morocco): {wall_morocco}  |  "
                f"Next update in ~{REFRESH_MS // 1000}s  |  "
                f"Live: Kafka (OpenWeather pipeline)  |  "
                f"Messages: {snap['messages_seen']}  |  "
                f"Last Kafka event (Morocco): {snap['last_event_local'] or '—'}"
            )
            if err:
                refresh += f"  |  Kafka note: {err}"
            return refresh, fig, three

        fig = go.Figure()
        fig.update_layout(title="Waiting for live Kafka rows…", template="plotly_white")
        refresh = (
            f"Current refresh (Morocco): {wall_morocco}  |  "
            f"Live: Kafka (warming up…)  |  Messages: {snap['messages_seen']}"
        )
        if err:
            refresh += f"  |  Kafka note: {err}"
        return refresh, fig, []

    # Fallback: Hive / CSV — still show 3 canonical cities only
    refresh = (
        f"Current refresh (Morocco): {wall_morocco}  |  "
        f"Next update in ~{REFRESH_MS // 1000}s  |  "
        f"Source: Hive {HIVE_HOST}:{HIVE_PORT}/{HIVE_DATABASE}.{HIVE_TABLE}"
        + (" |  (Kafka off or no messages — start producer)" if USE_KAFKA else "")
    )
    df = load_weather_df(prefer_hive=True, limit=500)
    df_chart = latest_per_city(df)
    if df_chart.empty:
        fig = go.Figure()
        fig.update_layout(title="No data yet", template="plotly_white")
        return refresh, fig, []

    records = df_chart.to_dict("records")
    three = three_city_rows(records)
    if not three:
        fig = go.Figure()
        fig.update_layout(title="No rows for Rabat / Casablanca / Marrakesh", template="plotly_white")
        return refresh, fig, []

    cities = [str(r["CityName"]) for r in three]
    temps = [float(r["Temperature"]) for r in three]
    hums = [float(r["Humidity"]) for r in three]
    fig = _figure_from_cities(cities, temps, hums, "Hive / batch — Morocco display")
    for r in three:
        for col in ["CreationDate", "CreationTime", "CityName"]:
            if col in r and r[col] is not None:
                r[col] = str(r[col])
    return refresh, fig, three


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
