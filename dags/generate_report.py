"""
Gold Layer HTML Report Generator
Generates a stunning, interactive HTML dashboard with all Gold analytics visualizations.
"""
import os
from datetime import datetime


# =====================================================================
# SQL Queries (mirrors gold_aggregations.sql)
# =====================================================================
QUERY_FLEET_FLOW = """
WITH pickups AS (
    SELECT pickup_location_id AS location_id, COUNT(*) AS total_departures
    FROM silver.fact_taxi_trips GROUP BY pickup_location_id
),
dropoffs AS (
    SELECT dropoff_location_id AS location_id, COUNT(*) AS total_arrivals
    FROM silver.fact_taxi_trips GROUP BY dropoff_location_id
)
SELECT z.borough, z.zone_name,
    COALESCE(p.total_departures, 0) AS departures,
    COALESCE(d.total_arrivals, 0) AS arrivals,
    COALESCE(d.total_arrivals, 0) - COALESCE(p.total_departures, 0) AS net_fleet_flow
FROM silver.dim_zone z
LEFT JOIN pickups p ON z.location_id = p.location_id
LEFT JOIN dropoffs d ON z.location_id = d.location_id
ORDER BY net_fleet_flow ASC LIMIT 15;
"""

QUERY_CONGESTION_FEE = """
SELECT z.borough, t.taxi_type_name,
    COUNT(f.trip_id) AS total_trips,
    SUM(f.total_amount) AS gross_revenue,
    SUM(f.cbd_congestion_fee) AS total_congestion_fees_collected,
    ROUND((SUM(f.cbd_congestion_fee) / NULLIF(SUM(f.total_amount), 0)) * 100, 2) AS congestion_fee_percentage
FROM silver.fact_taxi_trips f
JOIN silver.dim_zone z ON f.pickup_location_id = z.location_id
JOIN silver.dim_taxi_type t ON f.taxi_type_id = t.taxi_type_id
WHERE f.cbd_congestion_fee > 0
GROUP BY z.borough, t.taxi_type_name
ORDER BY total_congestion_fees_collected DESC;
"""

QUERY_TRAFFIC_SPEED = """
SELECT dt.hour, dt.is_weekend, COUNT(f.trip_id) AS trip_volume,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(f.trip_duration_seconds / 60.0), 2) AS avg_duration_minutes,
    ROUND(AVG(f.trip_distance / NULLIF(f.trip_duration_seconds / 3600.0, 0)), 2) AS avg_speed_mph
FROM silver.fact_taxi_trips f
JOIN silver.dim_time dt ON f.pickup_time_id = dt.time_id
WHERE f.trip_distance > 0 AND f.trip_duration_seconds > 60
GROUP BY dt.hour, dt.is_weekend
ORDER BY dt.is_weekend, dt.hour;
"""

QUERY_AIRPORT = """
SELECT z.zone_name AS airport_destination, t.taxi_type_name,
    COUNT(f.trip_id) AS total_dropoffs,
    ROUND(AVG(f.total_amount), 2) AS average_fare,
    ROUND(AVG(f.trip_distance), 2) AS average_distance_miles
FROM silver.fact_taxi_trips f
JOIN silver.dim_zone z ON f.dropoff_location_id = z.location_id
JOIN silver.dim_taxi_type t ON f.taxi_type_id = t.taxi_type_id
WHERE z.zone_name ILIKE '%Airport%'
GROUP BY z.zone_name, t.taxi_type_name
ORDER BY total_dropoffs DESC;
"""

QUERY_TIPPING = """
WITH trip_buckets AS (
    SELECT trip_id, tip_amount, fare_amount,
        CASE
            WHEN trip_distance <= 2.0 THEN '1. Micro (0-2 mi)'
            WHEN trip_distance <= 5.0 THEN '2. Short (2-5 mi)'
            WHEN trip_distance <= 10.0 THEN '3. Medium (5-10 mi)'
            ELSE '4. Long (10+ mi)'
        END AS distance_bracket
    FROM silver.fact_taxi_trips f
    JOIN silver.dim_payment_type p ON f.payment_type_id = p.payment_type_id
    WHERE p.payment_description = 'Credit card' AND f.fare_amount > 0
)
SELECT distance_bracket, COUNT(*) AS total_credit_card_trips,
    ROUND(AVG(tip_amount), 2) AS average_tip_usd,
    ROUND(AVG(tip_amount / fare_amount) * 100, 2) AS average_tip_percentage
FROM trip_buckets GROUP BY distance_bracket ORDER BY distance_bracket;
"""

QUERY_SUMMARY_STATS = """
SELECT
    (SELECT COUNT(*) FROM silver.fact_taxi_trips) AS total_trips,
    (SELECT COUNT(*) FROM silver.fact_taxi_trips WHERE taxi_type_id = 1) AS yellow_trips,
    (SELECT COUNT(*) FROM silver.fact_taxi_trips WHERE taxi_type_id = 2) AS green_trips,
    (SELECT ROUND(AVG(trip_distance)::NUMERIC, 2) FROM silver.fact_taxi_trips WHERE trip_distance > 0) AS avg_distance,
    (SELECT ROUND(AVG(trip_duration_seconds / 60.0)::NUMERIC, 2) FROM silver.fact_taxi_trips WHERE trip_duration_seconds > 60) AS avg_duration_min;
"""


def generate_gold_report(**kwargs):
    """
    Connects to PostgreSQL, runs all Gold analytics queries,
    and generates an interactive HTML dashboard with Plotly visualizations.
    """
    import polars as pl
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()

    # ------------------------------------------------------------------
    # Execute all queries
    # ------------------------------------------------------------------
    df_fleet    = pl.read_database(QUERY_FLEET_FLOW, connection=engine)
    df_congest  = pl.read_database(QUERY_CONGESTION_FEE, connection=engine)
    df_speed    = pl.read_database(QUERY_TRAFFIC_SPEED, connection=engine)
    df_airport  = pl.read_database(QUERY_AIRPORT, connection=engine)
    df_tipping  = pl.read_database(QUERY_TIPPING, connection=engine)
    df_summary  = pl.read_database(QUERY_SUMMARY_STATS, connection=engine)

    run_date = kwargs.get('data_interval_start', datetime.now()).strftime('%B %Y')

    # ------------------------------------------------------------------
    # Summary stats
    # ------------------------------------------------------------------
    total_trips   = f"{df_summary['total_trips'][0]:,}"
    yellow_trips  = f"{df_summary['yellow_trips'][0]:,}"
    green_trips   = f"{df_summary['green_trips'][0]:,}"
    avg_distance  = f"{df_summary['avg_distance'][0]:.2f} mi"
    avg_duration  = f"{df_summary['avg_duration_min'][0]:.1f} min"

    # ------------------------------------------------------------------
    # COLOR PALETTE (Light Minimalist)
    # ------------------------------------------------------------------
    YELLOW   = '#D4AC0D'
    GREEN    = '#27AE60'
    BLUE     = '#2980B9'
    RED      = '#C0392B'
    PURPLE   = '#8E44AD'
    ORANGE   = '#D35400'
    BG_DARK  = '#F8F9FA'  # Renamed variable contents, kept variable name for compatibility
    BG_CARD  = '#FFFFFF'
    BG_CHART = '#FFFFFF'
    TEXT     = '#212529'
    GRID     = '#E9ECEF'
    ACCENT   = '#343A40'

    chart_layout = dict(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor=BG_CHART,
        font=dict(family='Inter, Helvetica, sans-serif', color=TEXT, size=12),
        margin=dict(l=60, r=30, t=50, b=60),
        xaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
        yaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
    )

    # ------------------------------------------------------------------
    # CHART 1: Fleet Flow (Horizontal Bar)
    # ------------------------------------------------------------------
    colors_fleet = [RED if v < 0 else GREEN for v in df_fleet['net_fleet_flow'].to_list()]
    fig1 = go.Figure(go.Bar(
        y=df_fleet['zone_name'].to_list(),
        x=df_fleet['net_fleet_flow'].to_list(),
        orientation='h',
        marker=dict(color=colors_fleet, line=dict(width=0)),
        text=df_fleet['net_fleet_flow'].to_list(),
        textposition='outside',
        textfont=dict(size=11, color=TEXT),
    ))
    fig1.update_layout(**chart_layout, title='Net Fleet Flow by Zone (Top 15 Deficit)',
                       height=500)
    fig1.update_yaxes(autorange='reversed', gridcolor=GRID)

    # ------------------------------------------------------------------
    # CHART 2: Congestion Fee Revenue (Grouped Bar)
    # ------------------------------------------------------------------
    fig2 = go.Figure()
    for taxi_type, color in [('Yellow', YELLOW), ('Green', GREEN)]:
        subset = df_congest.filter(pl.col('taxi_type_name') == taxi_type)
        if len(subset) > 0:
            fig2.add_trace(go.Bar(
                x=subset['borough'].to_list(),
                y=[float(v) for v in subset['total_congestion_fees_collected'].to_list()],
                name=taxi_type,
                marker=dict(color=color, line=dict(width=0)),
                text=[f"{float(v):.1f}%" for v in subset['congestion_fee_percentage'].to_list()],
                textposition='outside',
                textfont=dict(size=11, color=TEXT),
            ))
    fig2.update_layout(**chart_layout, barmode='group',
                       title='CBD Congestion Fee Revenue by Borough',
                       yaxis_title='Total Fees Collected ($)',
                       height=450)

    # ------------------------------------------------------------------
    # CHART 3: Traffic Speed (Line - Weekday vs Weekend)
    # ------------------------------------------------------------------
    fig3 = go.Figure()
    for is_wknd, label, color, dash in [(False, 'Weekday', BLUE, 'solid'), (True, 'Weekend', ORANGE, 'dash')]:
        subset = df_speed.filter(pl.col('is_weekend') == is_wknd)
        if len(subset) > 0:
            fig3.add_trace(go.Scatter(
                x=subset['hour'].to_list(),
                y=[float(v) for v in subset['avg_speed_mph'].to_list()],
                mode='lines+markers',
                name=label,
                line=dict(color=color, width=2, dash=dash),
                marker=dict(size=6),
            ))
    fig3.update_layout(**chart_layout,
                       title='Average Network Speed (MPH) by Hour',
                       xaxis_title='Hour of Day', yaxis_title='Speed (MPH)',
                       height=420)
    fig3.update_xaxes(dtick=1, gridcolor=GRID)

    # ------------------------------------------------------------------
    # CHART 4: Trip Volume Heatmap-style bar (Weekday vs Weekend)
    # ------------------------------------------------------------------
    fig4 = go.Figure()
    for is_wknd, label, color in [(False, 'Weekday', BLUE), (True, 'Weekend', ORANGE)]:
        subset = df_speed.filter(pl.col('is_weekend') == is_wknd)
        if len(subset) > 0:
            fig4.add_trace(go.Bar(
                x=subset['hour'].to_list(),
                y=subset['trip_volume'].to_list(),
                name=label,
                marker=dict(color=color, opacity=0.85, line=dict(width=0)),
            ))
    fig4.update_layout(**chart_layout, barmode='group',
                       title='Trip Volume by Hour of Day',
                       xaxis_title='Hour of Day', yaxis_title='Number of Trips',
                       height=420)
    fig4.update_xaxes(dtick=1, gridcolor=GRID)

    # ------------------------------------------------------------------
    # CHART 5: Airport Market Share (Stacked Bar)
    # ------------------------------------------------------------------
    fig5 = go.Figure()
    for taxi_type, color in [('Yellow', YELLOW), ('Green', GREEN)]:
        subset = df_airport.filter(pl.col('taxi_type_name') == taxi_type)
        if len(subset) > 0:
            fig5.add_trace(go.Bar(
                x=subset['airport_destination'].to_list(),
                y=subset['total_dropoffs'].to_list(),
                name=taxi_type,
                marker=dict(color=color, line=dict(width=0)),
            ))
    fig5.update_layout(**chart_layout, barmode='stack',
                       title='Airport Corridor: Taxi Dropoff Market Share',
                       yaxis_title='Total Dropoffs',
                       height=420)

    # ------------------------------------------------------------------
    # CHART 6: Tipping Elasticity (Dual Axis)
    # ------------------------------------------------------------------
    fig6 = make_subplots(specs=[[{"secondary_y": True}]])
    fig6.add_trace(go.Bar(
        x=df_tipping['distance_bracket'].to_list(),
        y=[float(v) for v in df_tipping['average_tip_usd'].to_list()],
        name='Avg Tip ($)',
        marker=dict(color=PURPLE, opacity=0.85, line=dict(width=0)),
    ), secondary_y=False)
    fig6.add_trace(go.Scatter(
        x=df_tipping['distance_bracket'].to_list(),
        y=[float(v) for v in df_tipping['average_tip_percentage'].to_list()],
        name='Tip % of Fare',
        mode='lines+markers',
        line=dict(color=ORANGE, width=2),
        marker=dict(size=8, symbol='diamond'),
    ), secondary_y=True)
    fig6.update_layout(**chart_layout,
                       title='Tipping Elasticity by Trip Distance',
                       height=420)
    fig6.update_yaxes(title_text='Average Tip ($)', secondary_y=False, gridcolor=GRID)
    fig6.update_yaxes(title_text='Tip as % of Fare', secondary_y=True, gridcolor=GRID)

    # ------------------------------------------------------------------
    # Helper: Polars DataFrame -> HTML table
    # ------------------------------------------------------------------
    def df_to_html_table(df, max_rows=20):
        rows = df.head(max_rows)
        header = ''.join(f'<th>{c}</th>' for c in rows.columns)
        body = ''
        for i in range(len(rows)):
            cells = ''.join(f'<td>{rows[c][i]}</td>' for c in rows.columns)
            body += f'<tr>{cells}</tr>'
        return f'<table class="data-table"><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>'

    # ------------------------------------------------------------------
    # Build the HTML report
    # ------------------------------------------------------------------
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NYC Taxi Gold Analytics Report — {run_date}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Inter', Helvetica, sans-serif;
            background: {BG_DARK};
            color: {TEXT};
            min-height: 100vh;
        }}
        .header {{
            background: #FFFFFF;
            border-bottom: 1px solid #E9ECEF;
            padding: 40px 0 35px;
            text-align: center;
        }}
        .header h1 {{
            font-size: 2rem;
            font-weight: 600;
            color: #212529;
            margin-bottom: 8px;
            letter-spacing: -0.5px;
        }}
        .header p {{
            color: #6C757D;
            font-size: 0.95rem;
            font-weight: 400;
        }}
        .container {{ max-width: 1280px; margin: 0 auto; padding: 40px 20px; }}

        /* KPI Cards */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        .kpi-card {{
            background: {BG_CARD};
            border: 1px solid #E9ECEF;
            border-radius: 6px;
            padding: 24px 20px;
            text-align: left;
            min-height: 118px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            overflow: hidden;
        }}
        .kpi-card .value {{
            font-size: clamp(1.2rem, 2vw, 1.75rem);
            font-weight: 600;
            color: #212529;
            margin-bottom: 4px;
            line-height: 1.1;
            overflow-wrap: anywhere;
            word-break: break-word;
            max-width: 100%;
        }}
        .kpi-card .label {{
            font-size: 0.75rem;
            color: #6C757D;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 500;
            line-height: 1.3;
            overflow-wrap: anywhere;
        }}

        /* Section */
        .section {{
            background: {BG_CARD};
            border: 1px solid #E9ECEF;
            border-radius: 6px;
            padding: 30px;
            margin-bottom: 30px;
        }}
        .section h2 {{
            font-size: 1.15rem;
            font-weight: 600;
            margin-bottom: 8px;
            color: #212529;
        }}
        .section .subtitle {{
            font-size: 0.9rem;
            color: #6C757D;
            margin-bottom: 24px;
            line-height: 1.5;
        }}
        .chart-container {{ width: 100%; margin-bottom: 20px; }}

        /* Tables */
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
            font-size: 0.85rem;
        }}
        .data-table thead {{ background: #F8F9FA; border-bottom: 2px solid #DEE2E6; border-top: 1px solid #DEE2E6; }}
        .data-table th {{
            padding: 12px 14px;
            text-align: left;
            font-weight: 600;
            color: #495057;
        }}
        .data-table td {{
            padding: 10px 14px;
            border-bottom: 1px solid #E9ECEF;
            color: #212529;
        }}
        .data-table tbody tr:nth-child(even) {{ background: #F8F9FA; }}

        /* Two column grid */
        .grid-2 {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
        }}
        @media (max-width: 900px) {{
            .grid-2 {{ grid-template-columns: 1fr; }}
        }}

        .footer {{
            text-align: center;
            padding: 30px;
            color: #6C757D;
            font-size: 0.85rem;
            border-top: 1px solid #E9ECEF;
            margin-top: 20px;
        }}
        .footer a {{ color: {BLUE}; text-decoration: none; }}

        .badge {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.7rem;
            font-weight: 600;
            letter-spacing: 0.5px;
            margin-left: 8px;
            vertical-align: middle;
            border: 1px solid;
        }}
        .badge-yellow {{ background: #FFF9E6; color: #B7950B; border-color: #F7DC6F; }}
        .badge-green {{ background: #E8F8F5; color: #117A65; border-color: #A3E4D7; }}
    </style>
</head>
<body>

<div class="header">
    <h1>NYC Taxi Analytics Dashboard</h1>
    <p>Gold Layer Report &nbsp; &nbsp; Data Period: {run_date} &nbsp; &nbsp; Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div>

<div class="container">

    <!-- KPI Cards -->
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="value">{total_trips}</div>
            <div class="label">Total Trips</div>
        </div>
        <div class="kpi-card">
            <div class="value" style="color: {YELLOW};">{yellow_trips}</div>
            <div class="label">Yellow Taxi Trips</div>
        </div>
        <div class="kpi-card">
            <div class="value" style="color: {GREEN};">{green_trips}</div>
            <div class="label">Green Taxi Trips</div>
        </div>
        <div class="kpi-card">
            <div class="value">{avg_distance}</div>
            <div class="label">Avg Trip Distance</div>
        </div>
        <div class="kpi-card">
            <div class="value">{avg_duration}</div>
            <div class="label">Avg Trip Duration</div>
        </div>
    </div>

    <!-- Section 1: Fleet Rebalancing -->
    <div class="section">
        <h2>1. Fleet Rebalancing: Net Spatial Flow</h2>
        <p class="subtitle">Zones where vehicles are "draining" (more pickups than dropoffs) vs. "accumulating" — critical for dispatching rebalancing.</p>
        <div class="chart-container" id="chart-fleet"></div>
        {df_to_html_table(df_fleet)}
    </div>

    <!-- Section 2: Congestion Fee Impact -->
    <div class="section">
        <h2>2. Policy Impact: CBD Congestion Fee Revenue</h2>
        <p class="subtitle">Financial impact of the MTA Congestion Relief Zone fee across boroughs. Percentages show the fee's share of gross revenue.</p>
        <div class="chart-container" id="chart-congestion"></div>
        {df_to_html_table(df_congest)}
    </div>

    <!-- Section 3 & 4: Speed + Volume (Side by side) -->
    <div class="grid-2">
        <div class="section">
            <h2>3. Traffic Velocity</h2>
            <p class="subtitle">Average network speed (MPH) by hour — weekday vs. weekend. Identifies severe bottlenecks.</p>
            <div class="chart-container" id="chart-speed"></div>
        </div>
        <div class="section">
            <h2>3b. Trip Volume by Hour</h2>
            <p class="subtitle">Demand distribution across the day. Peak hours drive pricing and scheduling strategy.</p>
            <div class="chart-container" id="chart-volume"></div>
        </div>
    </div>
    {df_to_html_table(df_speed)}

    <!-- Section 4: Airport Corridors -->
    <div class="section">
        <h2>4. Market Share: Airport Corridors</h2>
        <p class="subtitle">Yellow vs. Green taxi dominance on lucrative airport routes.<span class="badge badge-yellow">YELLOW</span><span class="badge badge-green">GREEN</span></p>
        <div class="chart-container" id="chart-airport"></div>
        {df_to_html_table(df_airport)}
    </div>

    <!-- Section 5: Tipping -->
    <div class="section">
        <h2>5. Consumer Behavior: Tipping Elasticity</h2>
        <p class="subtitle">Tip amount and tip-as-percentage-of-fare across distance brackets (credit card trips only).</p>
        <div class="chart-container" id="chart-tipping"></div>
        {df_to_html_table(df_tipping)}
    </div>

</div>

<div class="footer">
    Janidu Kodithuwakku &nbsp &nbsp; Generated by the <a href="#">NYC Taxi Medallion Pipeline</a> &nbsp &nbsp; Airflow DAG: <code>dynamic_nyc_taxi_medallion</code>
</div>

<script>
    var config = {{ responsive: true, displayModeBar: false }};

    Plotly.newPlot('chart-fleet', {fig1.to_json()}.data, {fig1.to_json()}.layout, config);
    Plotly.newPlot('chart-congestion', {fig2.to_json()}.data, {fig2.to_json()}.layout, config);
    Plotly.newPlot('chart-speed', {fig3.to_json()}.data, {fig3.to_json()}.layout, config);
    Plotly.newPlot('chart-volume', {fig4.to_json()}.data, {fig4.to_json()}.layout, config);
    Plotly.newPlot('chart-airport', {fig5.to_json()}.data, {fig5.to_json()}.layout, config);
    Plotly.newPlot('chart-tipping', {fig6.to_json()}.data, {fig6.to_json()}.layout, config);
</script>

</body>
</html>"""

    # ------------------------------------------------------------------
    # Write the report to the data directory (accessible from host)
    # ------------------------------------------------------------------
    report_dir = '/opt/airflow/data'
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, 'gold_analytics_report.html')

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"Report generated successfully at: {report_path}")
    print(f"Access it on your Mac at: data/gold_analytics_report.html")
