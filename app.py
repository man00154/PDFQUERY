import pandas as pd
import plotly.express as px
import streamlit as st

from excel_agent import ExcelAIAgent, safe_numeric

st.set_page_config(page_title="Excel AI Agent Dashboard", layout="wide")

@st.cache_resource
def get_agent():
    return ExcelAIAgent()

agent = get_agent()

st.title("Excel AI Agent Dashboard")
st.caption("Query multiple Excel files, sheets, rows, columns, metrics, and charts.")

with st.sidebar:
    st.header("Data Navigator")
    files = agent.list_files()
    selected_file = st.selectbox("Select file", options=[""] + files)

    selected_sheet = ""
    if selected_file:
        selected_sheet = st.selectbox("Select sheet", options=[""] + agent.list_sheets(selected_file))

    st.markdown("---")
    st.subheader("Quick Query")
    user_query = st.text_area(
        "Ask query",
        placeholder="Example: sum of Usage in KW where Customer Name = ZSCALER SOFTECH INDIA PVT LTD in sheet Customer Details1 in file Customer and Capacity Tracker Airoli 15Mar26.xlsx",
        height=140
    )
    run_query_btn = st.button("Run Query")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Explorer",
    "Aggregation",
    "Search",
    "Charts",
    "AI Query"
])

with tab1:
    st.subheader("Sheet Explorer")
    if selected_file and selected_sheet:
        df = agent.get_sheet(selected_file, selected_sheet)
        st.write(f"Rows: {len(df)} | Columns: {len(df.columns)}")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Select a file and sheet from the sidebar.")

with tab2:
    st.subheader("Aggregation Engine")

    col1, col2, col3, col4 = st.columns(4)

    agg_file = col1.selectbox("File", options=[""] + files, key="agg_file")
    agg_sheet = col2.selectbox("Sheet", options=[""] + (agent.list_sheets(agg_file) if agg_file else []), key="agg_sheet")

    agg_columns = []
    if agg_file and agg_sheet:
        agg_columns = list(agent.get_sheet(agg_file, agg_sheet).columns)

    agg_col = col3.selectbox("Column", options=[""] + agg_columns, key="agg_col")
    agg_op = col4.selectbox("Operation", ["sum", "mean", "min", "max", "count", "median", "std"], key="agg_op")

    filter_col = st.selectbox("Optional filter column", options=[""] + agg_columns, key="agg_filter_col")
    filter_val = st.text_input("Optional filter value", key="agg_filter_val")

    if st.button("Compute Aggregation"):
        filters = {}
        if filter_col and filter_val:
            filters[filter_col] = filter_val

        if agg_file and agg_sheet and agg_col:
            out = agent.aggregate(agg_file, agg_sheet, agg_col, agg_op, filters)
            c1, c2, c3 = st.columns(3)
            c1.metric("Operation", out["operation"])
            c2.metric("Column", out["column"])
            c3.metric("Result", out["result"])
            st.json(out)
        else:
            st.warning("Select file, sheet and column.")

with tab3:
    st.subheader("Search Rows")
    s1, s2, s3 = st.columns(3)

    srch_file = s1.selectbox("File", options=[""] + files, key="search_file")
    srch_sheet = s2.selectbox("Sheet", options=[""] + (agent.list_sheets(srch_file) if srch_file else []), key="search_sheet")
    keyword = s3.text_input("Keyword / customer / value", key="search_kw")

    search_cols = []
    if srch_file and srch_sheet:
        search_cols = list(agent.get_sheet(srch_file, srch_sheet).columns)

    selected_cols = st.multiselect("Search in specific columns", options=search_cols)

    if st.button("Search"):
        if keyword:
            res = agent.search_rows(
                keyword=keyword,
                file_name=srch_file or None,
                sheet_name=srch_sheet or None,
                columns=selected_cols or None
            )
            st.success(f"Found {len(res)} rows")
            st.dataframe(res, use_container_width=True)
        else:
            st.warning("Enter a keyword.")

with tab4:
    st.subheader("Charts & Dashboard Cards")

    c1, c2, c3, c4 = st.columns(4)
    chart_file = c1.selectbox("File", options=[""] + files, key="chart_file")
    chart_sheet = c2.selectbox("Sheet", options=[""] + (agent.list_sheets(chart_file) if chart_file else []), key="chart_sheet")

    chart_columns = []
    if chart_file and chart_sheet:
        chart_df = agent.get_sheet(chart_file, chart_sheet)
        chart_columns = list(chart_df.columns)
    else:
        chart_df = pd.DataFrame()

    group_col = c3.selectbox("Group by", options=[""] + chart_columns, key="group_col")
    value_col = c4.selectbox("Value column", options=[""] + chart_columns, key="value_col")

    op = st.selectbox("Aggregation", ["sum", "mean", "min", "max", "count"], key="chart_op")

    if st.button("Generate Chart"):
        if chart_file and chart_sheet and group_col and value_col:
            grp = agent.group_aggregate(chart_file, chart_sheet, group_col, value_col, op)

            metric_total = grp.iloc[:, 1].sum() if not grp.empty else 0
            metric_max = grp.iloc[:, 1].max() if not grp.empty else 0
            metric_min = grp.iloc[:, 1].min() if not grp.empty else 0
            metric_count = len(grp)

            a, b, c, d = st.columns(4)
            a.metric("Total", round(float(metric_total), 2) if pd.notna(metric_total) else 0)
            b.metric("Max", round(float(metric_max), 2) if pd.notna(metric_max) else 0)
            c.metric("Min", round(float(metric_min), 2) if pd.notna(metric_min) else 0)
            d.metric("Groups", metric_count)

            fig_bar = px.bar(grp, x=group_col, y=grp.columns[1], title=f"{op.upper()} of {value_col} by {group_col}")
            fig_pie = px.pie(grp, names=group_col, values=grp.columns[1], title=f"Distribution of {value_col} by {group_col}")

            st.plotly_chart(fig_bar, use_container_width=True)
            st.plotly_chart(fig_pie, use_container_width=True)
            st.dataframe(grp, use_container_width=True)
        else:
            st.warning("Select file, sheet, group by and value column.")

with tab5:
    st.subheader("AI Query Runner")

    examples = [
        "sum of Usage in KW in sheet Customer Details1 in file Customer and Capacity Tracker Airoli 15Mar26.xlsx",
        "max of Capacity in Use where Customer Name = CISCO SYSTEMS INDIA PTY LTD in sheet Customer Details1 in file Customer and Capacity Tracker Airoli 15Mar26.xlsx",
        "find ZSCALER SOFTECH INDIA PVT LTD",
        "sum of Sold in sheet NEW SUMMARY in file Customer and Capacity Tracker Bangalore 01 15Feb26.xlsx",
        "group by FLOOR and sum of Usage in KW in sheet Customer Details1 in file Customer and Capacity Tracker Airoli 15Mar26.xlsx",
    ]
    st.write("Example queries:")
    for ex in examples:
        st.code(ex)

    if run_query_btn and user_query:
        out = agent.run_query(user_query)

        if out["type"] == "metric":
            data = out["data"]
            st.success(out["message"])
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("File", data["file"])
            c2.metric("Sheet", data["sheet"])
            c3.metric("Operation", data["operation"])
            c4.metric("Result", data["result"])
            st.json(out["parsed"])

        elif out["type"] == "table":
            st.success(out["message"])
            st.dataframe(out["data"], use_container_width=True)
            st.json(out["parsed"])

        elif out["type"] == "chart_table":
            st.success(out["message"])
            df = out["data"]
            if not df.empty:
                fig = px.bar(df, x=df.columns[0], y=df.columns[1], title="Query Result")
                st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)
            st.json(out["parsed"])

        elif out["type"] == "error":
            st.error(out["message"])
            st.json(out["parsed"])
        else:
            st.info(out["message"])
            st.json(out["parsed"])
