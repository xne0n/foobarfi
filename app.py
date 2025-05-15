from dataclasses import asdict
import streamlit as st
from barfi.flow import ComputeEngine
from barfi.flow.schema.types import FlowSchema, FlowViewport
from barfi.flow.streamlit import st_flow
from barfi.config import SCHEMA_VERSION
from barfi.flow.schema import create_schema_manager
import json

st.set_page_config(
    page_title="Barfi Flow Editor",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Remove previous CSS - we'll use the component's built-in height parameter instead

from assets.blocks import base_blocks
# Select storage type
storage_type = st.sidebar.radio("Storage Type", ["file", "database"])

if storage_type == "file":
    # File-based storage
    schema_manager = create_schema_manager(
        storage_type="file", 
        filepath="./assets/"
    )
    st.sidebar.info("Using file-based storage (schemas.barfi)")
else:
    # Database storage - Use PostgreSQL
    db_config = {
        'dbname': 'dev_db',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432'
    }

    # Create PostgreSQL connection string
    conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    
    # Display connection information
    st.sidebar.subheader("PostgreSQL Connection")
    st.sidebar.write(f"Host: {db_config['host']}:{db_config['port']}")
    st.sidebar.write(f"Database: {db_config['dbname']}")
    
    try:
        # Create schema manager with PostgreSQL database
        schema_manager = create_schema_manager(
            storage_type="database",
            engine=conn_string,
            schema_table="flow_schema"  # Use the provided table name
        )
        st.sidebar.success("Connected to PostgreSQL database")
    except Exception as e:
        st.sidebar.error(f"Database connection error: {str(e)}")
        # Show detailed error information
        st.sidebar.exception(e)
        st.stop()

load_schema_name = st.selectbox("Schema name", [None] + schema_manager.schema_names)

if load_schema_name is not None:
    load_schema = schema_manager.load_schema(load_schema_name)
else:
    load_schema = FlowSchema(
        version=SCHEMA_VERSION,
        nodes=[],
        connections=[],
        viewport=FlowViewport(x=0, y=0, zoom=1),
    )

# Add a button to fetch the latest schema directly from the React Flow editor
fetch_schema_clicked = st.sidebar.button("Fetch current schema")

# Determine trigger command based on button click
trigger_cmd = "save" if fetch_schema_clicked else None

# Set a taller flow component using the height parameter
barfi_result = st_flow(
    blocks=base_blocks,
    editor_schema=load_schema,
    height=600,  # Specify a taller height
    trigger_command=trigger_cmd,
    key="flow-editor",  # Use a fixed key so the component instance is preserved across reruns
)

# If we clicked the fetch schema button, print the latest schema
if fetch_schema_clicked:
    st.write("### Current Flow Schema (JSON)")
    st.json(asdict(barfi_result.editor_schema))

compute_engine = ComputeEngine(base_blocks)

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["View Schema", "Save Schema", "Update Schema", "Inspect Execute Result", "Flow Paths"]
)

with tab1:
    tab1_1, tab1_2, tab1_3 = st.tabs(
        ["View as dict", "View as object", "View Node Info"]
    )
    with tab1_1:
        st.write(asdict(barfi_result))
    with tab1_2:
        st.write(barfi_result)
    with tab1_3:
        st.write(
            [
                (n.name, n.options, n.inputs, n.outputs)
                for n in barfi_result.editor_schema.nodes
            ]
        )
with tab2:
    with st.form("save_schema"):
        schema_name = st.text_input("Schema name")
        if st.form_submit_button("Save schema"):
            try:
                schema_manager.save_schema(schema_name, barfi_result.editor_schema)
                st.success(f"Schema '{schema_name}' successfully saved to {storage_type} storage")
            except Exception as e:
                st.error(f"Error saving schema: {str(e)}")
with tab3:
    with st.form("update_schema"):
        if load_schema_name is None:
            st.info("Please load a schema first before updating")
            st.form_submit_button("Update schema", disabled=True)
        else:
            if st.form_submit_button("Update schema"):
                try:
                    schema_manager.update_schema(load_schema_name, barfi_result.editor_schema)
                    st.success(f"Schema '{load_schema_name}' successfully updated")
                except Exception as e:
                    st.error(f"Error updating schema: {str(e)}")
with tab4:
    if barfi_result.command == "execute":
        flow_schema = barfi_result.editor_schema
        compute_engine.execute(flow_schema)
        result_block = flow_schema.block(node_label="Result-1")
        st.write(result_block)
        st.write(result_block.get_interface("Input 1"))
    else:
        st.write("No execute command was run.")

@st.dialog("Add Document")
def add_document_dialog():
    prompt_styles = ["Informative", "Concise", "Creative", "Formal", "Casual"]
    selected_styles = st.multiselect("Prompt style", prompt_styles)
    text_area_1 = st.text_area("Initial Text", height=150)
    
    with st.expander("Flow Path Details"):
        full_text = st.session_state.get('flow_full_text', 'Flow path data not available.')
        st.markdown(full_text)
        
    text_area_2 = st.text_area("Concluding Text", height=150)

    if st.button("Submit Document"):
        # Combine the texts and the flow path markdown
        combined_content = f"""{text_area_1}

--- Flow Path ---
{full_text}
--- End Flow Path ---

{text_area_2}"""
        st.session_state.doc_info = {"styles": selected_styles, "combined_content": combined_content}
        st.rerun()

with tab5:
    st.write("## Flow Paths and Document Addition")
    
    # Calculate flow paths and store 'full' in session state when tab5 is active
    from barfi.flow.schema.path_parser import parse_flow_schema
    try:
        df, df2, full = parse_flow_schema(asdict(barfi_result.editor_schema))
        st.session_state.flow_full_text = full
        # Store df and df2 as well if needed frequently, otherwise recalculate
        st.session_state.flow_df = df 
        st.session_state.flow_df2 = df2
    except Exception as e:
        st.warning(f"Could not parse flow schema for path details: {e}")
        st.session_state.flow_full_text = "Error parsing flow schema."
        st.session_state.flow_df = None
        st.session_state.flow_df2 = None


    if st.button("Add document"):
        add_document_dialog()

    # Display submitted info if available
    if "doc_info" in st.session_state:
        st.success(f"Document submitted with styles: {st.session_state.doc_info['styles']}")
        st.text("Combined Content:")
        st.text_area("Submitted Document", value=st.session_state.doc_info['combined_content'], height=300, disabled=True)
        # Optionally clear the state after displaying
        # del st.session_state.doc_info
        # del st.session_state.flow_full_text # Clear if it should only be generated once per submission cycle

    if st.button("Show Flow Paths DataFrame"):
        # Use the data stored in session state
        if st.session_state.get('flow_df') is not None:
            st.dataframe(st.session_state.flow_df)
            st.dataframe(st.session_state.flow_df2)
            st.text(st.session_state.flow_full_text)
        else:
            st.warning("Flow path dataframes are not available (likely due to parsing error or empty schema).")
