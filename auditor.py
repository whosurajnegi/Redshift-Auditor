import streamlit as st
import pandas as pd
import psycopg2

# Function to establish a connection to Redshift using psycopg2
def connect_to_redshift(jdbc_url, user, password):
    try:
        jdbc_url = jdbc_url.replace("jdbc:redshift://", "")
        host, port_db = jdbc_url.split(":")
        port, dbname = port_db.split("/")
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None

# Function to read key columns from Excel
def read_key_columns_from_excel(file):
    df = pd.read_excel(file)
    return df

# UI Layout
st.title("Redshift QA vs Prod Data Comparison Tool")

if "page" not in st.session_state:
    st.session_state.page = "input"

if st.session_state.page == "input":
    col1, col2 = st.columns(2)

    with col1:
        st.header("Redshift QA")
        qa_jdbc = st.text_input("JDBC URL (QA)")
        qa_user = st.text_input("User ID (QA)")
        qa_password = st.text_input("Password (QA)", type="password")

    with col2:
        st.header("Redshift Prod")
        prod_jdbc = st.text_input("JDBC URL (Prod)")
        prod_user = st.text_input("User ID (Prod)")
        prod_password = st.text_input("Password (Prod)", type="password")

    excel_file = st.file_uploader("Upload Excel File with Table and Key Columns", type=["xlsx"])

    if st.button("Next"):
        if not (qa_jdbc and qa_user and qa_password and prod_jdbc and prod_user and prod_password):
            st.error("Please fill in all required fields.")
        elif not excel_file:
            st.error("Please upload the Excel file with table and key columns.")
        else:
            st.session_state.qa_conn = connect_to_redshift(qa_jdbc, qa_user, qa_password)
            st.session_state.prod_conn = connect_to_redshift(prod_jdbc, prod_user, prod_password)
            if not st.session_state.qa_conn or not st.session_state.prod_conn:
                st.error("Failed to connect to one or both Redshift databases.")
            else:
                st.session_state.page = "comparison"
                st.session_state.table_key_data = read_key_columns_from_excel(excel_file)
                st.rerun()

elif st.session_state.page == "comparison":
    st.header("Enter Database and Table Details")
    db_name = st.text_input("Redshift Database Name")
    tables = st.session_state.table_key_data['Table Name'].tolist()

    if st.button("Compare Tables"):
        if not (db_name and tables):
            st.error("Please fill in all required fields.")
        else:
            st.session_state.db_name = db_name
            st.session_state.page = "results"
            st.rerun()

elif st.session_state.page == "results":
    st.title("Comparison Results")
    qa_conn = st.session_state.qa_conn
    prod_conn = st.session_state.prod_conn
    db_name = st.session_state.db_name
    tables = st.session_state.table_key_data['Table Name'].tolist()
    
    if qa_conn and prod_conn:
        for table in tables:
            st.subheader(f"Comparing Table: {table}")

            # Fetch key columns for the table
            key_columns = st.session_state.table_key_data[st.session_state.table_key_data["Table Name"] == table]["Key Columns"].values
            if len(key_columns) == 0:
                st.error(f"No key columns found for {table}. Skipping comparison.")
                continue
            key_columns = key_columns[0].split(",")  # Convert comma-separated keys to a list, stripping any spaces
            key_columns = [key.strip() for key in key_columns]  # Remove leading/trailing spaces
            key_columns_str = ", ".join(key_columns)

            try:
                # Get record count
                qa_count = pd.read_sql(f"SELECT COUNT(*) FROM {db_name}.{table}", qa_conn).iloc[0, 0]
                prod_count = pd.read_sql(f"SELECT COUNT(*) FROM {db_name}.{table}", prod_conn).iloc[0, 0]

                st.write(f"QA Count: {qa_count}, Prod Count: {prod_count}")
                
                if qa_count != prod_count:
                    st.warning("Record count mismatch detected.")
                else:
                    st.success("Record count matches.")

                # Fetch table data based on primary keys
                qa_data = pd.read_sql(f"SELECT {key_columns_str} FROM {db_name}.{table}", qa_conn)
                prod_data = pd.read_sql(f"SELECT {key_columns_str} FROM {db_name}.{table}", prod_conn)

                # Primary Key-Based Data Mismatch Identification (QA vs Prod and Prod vs QA)
                st.subheader("Primary Key-Based Data Mismatch Identification")
                # Merge data from QA and Prod on the primary key columns
                qa_prod_data = pd.merge(qa_data, prod_data, how='outer', on=key_columns, indicator=True)

                # Identify mismatches between QA and Prod
                mismatches_qa_vs_prod = qa_prod_data[qa_prod_data['_merge'] == 'left_only']
                mismatches_prod_vs_qa = qa_prod_data[qa_prod_data['_merge'] == 'right_only']

                if not mismatches_qa_vs_prod.empty or not mismatches_prod_vs_qa.empty:
                    st.error("⚠️ Mismatches found based on primary keys!")

                    if not mismatches_qa_vs_prod.empty:
                        st.subheader("Mismatches in QA but not in Prod:")
                        st.dataframe(mismatches_qa_vs_prod.drop(columns=['_merge']))
                    
                    if not mismatches_prod_vs_qa.empty:
                        st.subheader("Mismatches in Prod but not in QA:")
                        st.dataframe(mismatches_prod_vs_qa.drop(columns=['_merge']))

                    # Option to download the mismatch reports
                    mismatches_qa_vs_prod_csv = mismatches_qa_vs_prod.drop(columns=['_merge']).to_csv(index=False).encode('utf-8')
                    mismatches_prod_vs_qa_csv = mismatches_prod_vs_qa.drop(columns=['_merge']).to_csv(index=False).encode('utf-8')

                    st.download_button(
                        "Download Mismatch Report (QA vs Prod)",
                        mismatches_qa_vs_prod_csv,
                        f"{table}_qa_vs_prod_mismatch.csv",
                        "text/csv"
                    )
                    st.download_button(
                        "Download Mismatch Report (Prod vs QA)",
                        mismatches_prod_vs_qa_csv,
                        f"{table}_prod_vs_qa_mismatch.csv",
                        "text/csv"
                    )

                # Nullability Check (Null Value Comparison)
                st.subheader("Nullability Check (Null Value Comparison)")
                null_check = pd.DataFrame({
                    "QA Nulls": qa_data.isnull().sum(),
                    "Prod Nulls": prod_data.isnull().sum()
                })
                st.dataframe(null_check)

                # Aggregation Validation (Sum and Average Check)
                st.subheader("Aggregation Validation (Sum and Average Check)")
                numeric_columns = qa_data.select_dtypes(include=['number']).columns
                agg_check = pd.DataFrame(columns=["QA Sum", "Prod Sum", "QA Avg", "Prod Avg"])

                for col in numeric_columns:
                    agg_check.loc[col] = [
                        qa_data[col].sum(), prod_data[col].sum(),
                        qa_data[col].mean(), prod_data[col].mean()
                    ]

                st.dataframe(agg_check)
                
            except Exception as e:
                st.error(f"Error processing table {table}: {str(e)}")

    if st.button("Go Back"):
        st.session_state.page = "input"
        st.rerun()
