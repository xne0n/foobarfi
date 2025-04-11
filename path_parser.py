"""
Flow schema parsing utility for identifying single paths in flow diagrams.

This module provides functions to parse Barfi flow schemas into human-readable
text representations focused on single paths.
"""

import sys
from typing import Dict, List, Any, Set, Tuple, Optional
import json
from barfi.flow.schema.types import FlowSchema


def parse_single_path_from_lowest_x(flow_schema: FlowSchema) -> str:
    """
    Parses the process flow schema to find a single path based on the
    lowest X-coordinate node, tracing backwards to find its root,
    and then forwards until a branch or merge point.

    Args:
        flow_schema (FlowSchema): A FlowSchema object containing the process flow data.

    Returns:
        str: A formatted string describing the single identified process path.
    """
    nodes = flow_schema.nodes
    connections = flow_schema.connections

    if not nodes:
        return "Error: No nodes found in the flow schema."

    # --- 1. Preprocessing ---
    nodes_dict = {}
    min_x = sys.float_info.max
    lowest_x_node_id = None

    for node in nodes:
        node_id = node.id
        if not node_id:
            print(f"Warning: Found node without ID: {node.label}")
            continue
        nodes_dict[node_id] = node
        try:
            pos_x = float(node.position.x)
            if pos_x < min_x:
                min_x = pos_x
                lowest_x_node_id = node_id
        except (ValueError, TypeError):
             print(f"Warning: Could not parse X position for node {node_id}. Skipping for lowest X check.")

    if lowest_x_node_id is None:
        return "Error: Could not determine the node with the lowest X coordinate (check position data)."

    # Build incoming/outgoing connection lookups
    # outgoing: {'node_id': [{'to_node': id, 'from_interface': name}, ...]}
    outgoing_connections = {node_id: [] for node_id in nodes_dict}
    # incoming: {'node_id': [{'from_node': id, 'to_interface': name}, ...]}
    incoming_connections = {node_id: [] for node_id in nodes_dict}

    for conn in connections:
        output_node = conn.outputNode
        input_node = conn.inputNode
        output_interface = conn.outputNodeInterface
        input_interface = conn.inputNodeInterface

        if output_node and input_node and output_interface and output_node in nodes_dict:
            outgoing_connections[output_node].append({
                'to_node': input_node,
                'from_interface': output_interface
            })

        if input_node and output_node and input_interface and input_node in nodes_dict:
             incoming_connections[input_node].append({
                'from_node': output_node,
                'to_interface': input_interface
            })

    output_lines = []
    output_lines.append("--- Path Identification ---")
    output_lines.append(f"Node with lowest X coordinate: {lowest_x_node_id} (X={min_x:.2f})")

    # --- 2. Find the Root Node (Trace Backwards) ---
    output_lines.append("Tracing backwards to find root...")
    current_trace_back_id = lowest_x_node_id
    root_node_id = lowest_x_node_id
    visited_backward = {current_trace_back_id} # Prevent cycles in backward trace

    while True:
        incoming = incoming_connections.get(current_trace_back_id, [])
        num_incoming = len(incoming)

        if num_incoming == 0:
            output_lines.append(f"Found root node (no incoming connections): {current_trace_back_id}")
            root_node_id = current_trace_back_id
            break
        elif num_incoming == 1:
            from_node = incoming[0]['from_node']
            if from_node in visited_backward:
                 output_lines.append(f"Warning: Cycle detected during backward trace at node {from_node}. Stopping trace.")
                 # The node *before* the cycle start is the effective root for this logic
                 root_node_id = current_trace_back_id
                 break
            if from_node not in nodes_dict:
                 output_lines.append(f"Warning: Backward trace encountered missing node ID {from_node}. Stopping trace.")
                 root_node_id = current_trace_back_id # Node before the missing one is the effective root
                 break

            # Continue tracing back
            current_trace_back_id = from_node
            root_node_id = current_trace_back_id # Update potential root
            visited_backward.add(current_trace_back_id)
            # output_lines.append(f"... traced back to {current_trace_back_id}") # Optional detailed trace
        else: # num_incoming > 1
            output_lines.append(f"Stopped backward trace at node {current_trace_back_id} (multiple incoming connections - merge point).")
            # The node where we stopped *is* the furthest back we could uniquely trace
            root_node_id = current_trace_back_id
            break

    output_lines.append(f"Starting forward path from identified root: {root_node_id}")
    output_lines.append("\n--- Identified Process Path ---")

    # --- 3. Trace Forward Path ---
    current_node_id = root_node_id
    path_nodes_details = []
    visited_forward = set()
    step_counter = 0
    stop_reason = "Reached end of traceable path."

    while current_node_id is not None:
        if current_node_id not in nodes_dict:
            stop_reason = f"Encountered non-existent node ID {current_node_id}."
            break
        if current_node_id in visited_forward:
            stop_reason = f"Cycle detected. Re-encountered node {current_node_id}."
            # Add the node where the cycle was detected, then stop
            path_nodes_details.append(nodes_dict[current_node_id])
            break

        visited_forward.add(current_node_id)
        node_data = nodes_dict[current_node_id]
        path_nodes_details.append(node_data)
        step_counter += 1

        # Check conditions to continue/stop
        incoming = incoming_connections.get(current_node_id, [])
        outgoing = outgoing_connections.get(current_node_id, [])
        num_incoming = len(incoming)
        num_outgoing = len(outgoing)

        is_root_node = (current_node_id == root_node_id)

        # Stop conditions (applied after adding the current node):
        # 1. Not the root node AND has multiple incoming connections (merge point)
        if not is_root_node and num_incoming > 1:
            stop_reason = f"Stopped at '{node_data.label}' (Node ID: {current_node_id}) - Merge point (multiple inputs)."
            break
        # 2. Has zero or multiple outgoing connections (end of path or branch point)
        if num_outgoing != 1:
            if num_outgoing == 0:
                 stop_reason = f"Stopped at '{node_data.label}' (Node ID: {current_node_id}) - End of path (no outputs)."
            else: # num_outgoing > 1
                 stop_reason = f"Stopped at '{node_data.label}' (Node ID: {current_node_id}) - Branch point (multiple outputs)."
            break

        # If conditions met, continue to the single next node
        current_node_id = outgoing[0]['to_node']

    # --- 4. Format and Print Output ---
    if not path_nodes_details:
         output_lines.append("No forward path could be traced from the root node.")
    else:
        for i, node_data in enumerate(path_nodes_details):
            node_id = node_data.id
            template = node_data.filled_story_template or f"Step for Node {node_id} (No template found)"
             # Simple formatting for the step
            formatted_step = template.strip()
            if formatted_step and not formatted_step.endswith(('.', '?', '!')):
                formatted_step += '.'
            formatted_step = formatted_step[0].upper() + formatted_step[1:] if formatted_step else template

            output_lines.append(f"{i + 1}. {formatted_step}  (Node: {node_id})") # Add Node ID for reference

    output_lines.append(f"\nPath Description Summary: {stop_reason}")

    return "\n".join(output_lines)


def parse_single_path_from_lowest_x_json(json_data: str) -> str:
    """
    Parses the process flow JSON to find a single path based on the
    lowest X-coordinate node, tracing backwards to find its root,
    and then forwards until a branch or merge point.

    Args:
        json_data (str): A JSON string containing the process flow data.

    Returns:
        str: A formatted string describing the single identified process path.
    """
    try:
        data = json.loads(json_data)
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON provided. {e}"

    # Convert JSON data to FlowSchema
    from barfi.flow.schema.types import build_flow_schema_from_dict
    
    try:
        # Handle both direct schema and nested schemas
        if "editor_schema" in data:
            schema_dict = data["editor_schema"]
        else:
            schema_dict = data
            
        flow_schema = build_flow_schema_from_dict(schema_dict)
        return parse_single_path_from_lowest_x(flow_schema)
    except Exception as e:
        return f"Error: Failed to convert JSON to FlowSchema. {e}"

def parse_all_paths(flow_schema: FlowSchema) -> str:
    """
    Parses the process flow schema to find all distinct paths (Step Threads)
    through the flow, treating merge/branch points as single-node paths and
    handling "Section" nodes as chapter headers.

    Args:
        flow_schema (FlowSchema): A FlowSchema object containing the process flow data.

    Returns:
        str: A formatted string describing all identified Step Threads.
    """
    nodes = flow_schema.nodes
    connections = flow_schema.connections
    viewport = flow_schema.viewport # Viewport might not be used directly here anymore

    if not nodes:
        return "Error: No nodes found in the flow schema."

    # --- 1. Preprocessing ---
    nodes_dict = {}
    nodes_by_pos_x = []

    for node in nodes:
        node_id = node.id
        if not node_id:
            print(f"Warning: Found node without ID: {node.label}")
            continue
        nodes_dict[node_id] = node
        try:
            pos_x = float(node.position.x)
            nodes_by_pos_x.append((node_id, pos_x))
        except (ValueError, TypeError):
            print(f"Warning: Could not parse X position for node {node_id}. Skipping for position sorting.")

    # Sort nodes by X position (left to right)
    nodes_by_pos_x.sort(key=lambda x: x[1])

    # Build incoming/outgoing connection lookups
    outgoing_connections = {node_id: [] for node_id in nodes_dict}
    incoming_connections = {node_id: [] for node_id in nodes_dict}

    for conn in connections:
        output_node = conn.outputNode
        input_node = conn.inputNode
        output_interface = conn.outputNodeInterface
        input_interface = conn.inputNodeInterface

        if output_node and input_node and output_interface and output_node in nodes_dict:
            outgoing_connections[output_node].append({
                'to_node': input_node,
                'from_interface': output_interface,
                'to_interface': input_interface,
                'connection_id': conn.id
            })

        if input_node and output_node and input_interface and input_node in nodes_dict:
            incoming_connections[input_node].append({
                'from_node': output_node,
                'to_interface': input_interface,
                'from_interface': output_interface,
                'connection_id': conn.id
            })

    # --- 2. Identify special node types ---
    branch_points = set()  # Nodes with multiple outputs
    merge_points = set()   # Nodes with multiple inputs
    section_nodes = set()  # Nodes with type "Section"

    for node_id, node_data in nodes_dict.items():
        # Section nodes are NOT treated as branch points just for having multiple outputs
        # They allow flow-through.
        if len(outgoing_connections.get(node_id, [])) > 1 and node_data.type != "Section":
            branch_points.add(node_id)
        # Section nodes are NOT treated as merge points just for having multiple inputs.
        if len(incoming_connections.get(node_id, [])) > 1 and node_data.type != "Section":
            merge_points.add(node_id)
        # Identify Section nodes regardless of connections
        if hasattr(node_data, 'type') and node_data.type == "Section":
             section_nodes.add(node_id)

    junction_nodes = merge_points.union(branch_points) # Junctions are only true merge/branch points now

    # --- 3. Find Root Nodes ---
    root_nodes = set()
    for node_id in nodes_dict:
        incoming = incoming_connections.get(node_id, [])
        # A node is a root if it has NO incoming connections
        if not incoming:
            root_nodes.add(node_id)
        # OR if ALL incoming connections are from Section nodes
        elif all(conn.get('from_node') in section_nodes for conn in incoming):
             # Check if it's not a section itself, otherwise it just acts as a section header
             if node_id not in section_nodes:
                  root_nodes.add(node_id)

    # --- 4. Trace Paths (Revised Logic v2) ---
    complete_paths = []
    processed_nodes = set() # Nodes included in ANY path segment
    path_counter = 1

    # 4a. Create paths for non-section Junction nodes first
    junction_path_map = {} # junction_id -> temp path info
    initial_junction_paths = []
    for j_id in junction_nodes:
        if j_id in section_nodes: continue # Skip sections

        path_type = "unknown_junction"
        if j_id in merge_points and j_id in branch_points: path_type = "merge_and_branch_node"
        elif j_id in merge_points: path_type = "merge_node"
        elif j_id in branch_points: path_type = "branch_node"

        path_info = {"path": [j_id], "type": path_type, "id": path_counter, "_temp_id": path_counter}
        initial_junction_paths.append(path_info)
        junction_path_map[j_id] = path_info # Store temporary info
        processed_nodes.add(j_id) # Mark junction as processed
        path_counter += 1

    # 4b. Identify potential starting points for linear paths
    linear_starts = set()
    # Roots (non-junction, non-section)
    for r_id in root_nodes:
        if r_id not in junction_nodes and r_id not in section_nodes:
            linear_starts.add(r_id)
    # Nodes after junctions/sections (non-junction, non-section)
    nodes_to_check_after = list(junction_nodes) + list(section_nodes)
    for source_id in nodes_to_check_after:
        for conn in outgoing_connections.get(source_id, []):
            next_id = conn['to_node']
            if next_id and next_id in nodes_dict and next_id not in junction_nodes and next_id not in section_nodes:
                linear_starts.add(next_id)

    # 4c. Trace linear paths
    finished_linear_paths = []
    ordered_linear_starts = sorted(list(linear_starts), key=lambda nid:
                                   next((pos for nodeid, pos in nodes_by_pos_x if nodeid == nid), float('inf')))

    for start_node_id in ordered_linear_starts:
        # Crucially, check if start node is *already processed* before tracing
        if start_node_id in processed_nodes:
            continue

        # Start trace for this segment
        current_node_id = start_node_id
        current_path_nodes = [] # Start empty, add nodes as they are confirmed part of path
        current_connection_info = []
        visited_in_this_trace = set()

        path_ended = False
        while not path_ended:
            # Check BEFORE processing if current node is already processed or visited in this trace
            if current_node_id in processed_nodes or current_node_id in visited_in_this_trace:
                 # This trace overlaps/cycles, end the *previous* segment
                 path_ended = True
                 continue

            # Add current node to path and mark as processed/visited
            current_path_nodes.append(current_node_id)
            processed_nodes.add(current_node_id)
            visited_in_this_trace.add(current_node_id)

            # Determine outgoing connections and next node
            outgoing = outgoing_connections.get(current_node_id, [])
            num_outgoing = len(outgoing)
            next_node_id = None
            next_conn = None
            is_current_section = current_node_id in section_nodes # Should not happen for linear paths

            if num_outgoing >= 1:
                 next_conn = outgoing[0]
                 next_node_id = next_conn['to_node']
                 if num_outgoing > 1 and not is_current_section: # True branch
                      next_node_id = None
                      next_conn = None

            # --- Termination Checks for the *next* step --- 
            should_end_here = False
            end_type = "linear_unknown"
            ends_at_junction_id = None
            via_interface = None

            if num_outgoing == 0: # Endpoint
                should_end_here = True; end_type = "linear_endpoint"
            elif num_outgoing > 1 and not is_current_section: # Branch point (current node ends the path)
                should_end_here = True; end_type = "linear_to_branch"; ends_at_junction_id = current_node_id
            elif next_node_id is None and num_outgoing > 0: # Invalid target
                 should_end_here = True; end_type = "linear_error"
            elif next_node_id in junction_nodes: # Next is junction
                 should_end_here = True; end_type = "linear_to_junction"; ends_at_junction_id = next_node_id; via_interface = next_conn['from_interface'] if next_conn else None
            elif next_node_id in visited_in_this_trace: # Cycle detected
                 should_end_here = True; end_type = "linear_cycle"
            elif next_node_id in processed_nodes: # Reached node already covered by another path
                 # End the path here, connection will be made later if needed
                 should_end_here = True; end_type = "linear_reaches_processed"; ends_at_junction_id = next_node_id # Treat like junction end for connection logic

            # --- Store path if ending --- 
            if should_end_here:
                 if current_path_nodes: # Only store if path has nodes
                     path_info = {
                         "path": list(current_path_nodes),
                         "connection_info": list(current_connection_info),
                         "type": end_type,
                         "id": path_counter,
                         "_temp_id": path_counter,
                         "ends_at_junction": ends_at_junction_id,
                         "via_interface": via_interface
                     }
                     finished_linear_paths.append(path_info)
                     path_counter += 1
                 path_ended = True
            else:
                # Continue trace: add connection info for the *next* step
                if next_node_id is None: # Safety check
                    print(f"Error: Trace logic error at {current_node_id}")
                    path_ended = True; continue

                current_connection_info.append({
                    'from_node': current_node_id, 'to_node': next_node_id,
                    'from_interface': next_conn['from_interface'], 'to_interface': next_conn['to_interface']
                })
                current_node_id = next_node_id # Advance current node ID *for the next iteration*

    # Combine initial junction paths and finished linear paths
    complete_paths = initial_junction_paths + finished_linear_paths

    # --- 5. Sort paths and Re-ID ---
    def get_sort_key(path_info):
        path = path_info["path"]
        if not path: return (float('inf'), float('inf'))
        first_node_id = path[0]
        pos_x, pos_y = float('inf'), float('inf')
        if first_node_id in nodes_dict:
            try:
                pos_x = float(nodes_dict[first_node_id].position.x)
                pos_y = float(nodes_dict[first_node_id].position.y)
            except: pass
        # Prioritize paths starting with Sections? Maybe not needed if sorting by pos
        # is_section_start = first_node_id in section_nodes
        return (pos_x, pos_y) # Sort by X, then Y

    # Sort paths using the key
    complete_paths = sorted(complete_paths, key=get_sort_key)

    # Re-assign final sequential path IDs
    final_path_id_map = {} # Map old temp ID -> new final ID
    linear_path_node_map = {} # Map start node ID -> final linear path info
    junction_path_final_map = {} # Map junction node ID -> final junction path info

    for new_id, path_info in enumerate(complete_paths, 1):
        old_temp_id = path_info['_temp_id']
        path_info["id"] = new_id
        final_path_id_map[old_temp_id] = new_id

        path_nodes = path_info["path"]
        path_type = path_info["type"]

        if path_type.startswith("linear") and path_nodes:
            linear_path_node_map[path_nodes[0]] = path_info
        elif path_type in ["merge_node", "branch_node", "merge_and_branch_node"] and path_nodes:
             junction_path_final_map[path_nodes[0]] = path_info

    # --- 6. Build Connections using Final IDs ---
    nodes_in_paths = {} # Map node_id -> list of final path_ids
    for path_info in complete_paths:
        path_id = path_info["id"]
        for node_id in path_info["path"]:
            nodes_in_paths.setdefault(node_id, []).append(path_id)

    path_connections = {} # Map final_path_id -> {"to_paths": [...]}

    # 6a. Connect Linear Paths to Following Junctions
    for linear_path in complete_paths: # Iterate through original linear list
         old_temp_id = linear_path['_temp_id']
         if old_temp_id not in final_path_id_map: continue
         current_path_id = final_path_id_map[old_temp_id]

         # Connect if linear path ended before a junction or at a branch
         if linear_path["type"] == "linear_to_junction" or linear_path["type"] == "linear_to_branch":
            next_junction_id = linear_path["ends_at_junction"]
            interface = linear_path.get("via_interface") # Interface on the *source* node

            if next_junction_id in junction_path_final_map:
                target_path_id = junction_path_final_map[next_junction_id]["id"]
                path_connections.setdefault(current_path_id, {"to_paths": []})["to_paths"].append({
                    "path_id": target_path_id,
                    "via": "to_junction", # Generic connection type
                    "interface": interface or "output" # Provide default if missing
                })
            else:
                 # This case should ideally not happen if junctions are correctly identified
                 print(f"Warning: Target junction {next_junction_id} not found for linear path {current_path_id}.")

    # 6b. Connect Junctions to Following Paths (Linear or Junction)
    for junction_node_id, temp_junction_path_info in junction_path_final_map.items():
        if junction_node_id not in path_connections: continue
        current_path_id = path_connections[junction_node_id]["to_paths"][0]["path_id"] # Get final ID

        # Check actual outgoing connections from the graph data
        for conn in outgoing_connections.get(junction_node_id, []):
             next_node_id = conn['to_node']
             interface = conn['from_interface'] # Interface on the *junction* node

             target_path_id = None
             via_type = "unknown"

             if next_node_id in path_connections: # Connects to another junction
                 target_path_id = path_connections[next_node_id]["to_paths"][0]["path_id"]
                 via_type = "junction_to_junction"
             elif next_node_id in linear_path_node_map: # Connects to a linear path start
                 target_path_id = linear_path_node_map[next_node_id]["id"]
                 via_type = "junction_to_linear"
             # elif next_node_id in section_nodes:
             #     # How to handle connection *to* a section? Find the path containing it?
             #     # This gets complex if sections aren't path starts/ends.
             #     # For now, skip direct connection *to* sections in summary.
             #     print(f"Info: Skipping connection from junction {junction_node_id} to section {next_node_id} in summary.")
             #     continue
             elif next_node_id:
                  print(f"Warning: Node {next_node_id} after junction {junction_node_id} is not a junction or a known linear start.")
                  continue # Skip connection if target path type is unknown
             else:
                 continue # Skip if connection leads nowhere

             path_connections.setdefault(current_path_id, {"to_paths": []})["to_paths"].append({
                 "path_id": target_path_id,
                 "via": via_type,
                 "interface": interface
             })


    # --- 7. Format Output ---
    output_lines = ["--- All Flow Step Threads ---"]

    # Helper to format input details
    def format_input_info(node_id, current_step_thread_id, connections_for_node):
        input_details = []
        # Use GLOBAL incoming connections to the node_id
        all_incoming = incoming_connections.get(node_id, [])

        for conn in all_incoming:
            from_node_id = conn.get('from_node')
            input_interface = conn.get('to_interface', '')
            output_interface = conn.get('from_interface', '')
            if not (from_node_id and input_interface and output_interface): continue

            # Skip input if it comes from a Section node (sections are headers, not steps)
            if from_node_id in section_nodes:
                continue

            from_node_label = nodes_dict[from_node_id].label if from_node_id in nodes_dict else "Unknown"
            source_step_threads = nodes_in_paths.get(from_node_id, [])
            path_indicator = ""
            # Find threads containing the source *excluding* the current one
            relevant_source_step_threads = [p for p in source_step_threads if p != current_step_thread_id]

            if relevant_source_step_threads:
                # Reference the source Step Thread ID
                threads_str = ", ".join([f"Step Thread {p}" for p in sorted(relevant_source_step_threads)])
                path_indicator = f" (from {threads_str})"
            input_details.append(f'"{output_interface}" as "{input_interface}" from "{from_node_label}"{path_indicator}')

        if not input_details: return ""
        if len(input_details) == 1: return f'Using {input_details[0]}: '
        if len(input_details) == 2: return f'Using {input_details[0]} and {input_details[1]}: '
        return f'Using {", ".join(input_details[:-1])}, and {input_details[-1]}: '

    # Group threads by section for nested hierarchy
    section_threads = {}  # Map section_label -> list of thread_info
    section_order = []    # Keep track of section display order
    standalone_threads = []  # Threads not associated with any section
    
    # First pass: organize threads by their associated sections
    for path_info in complete_paths:
        path_nodes = path_info["path"]
        if not path_nodes:
            continue
            
        # Determine if this thread belongs to a section
        first_node_id = path_nodes[0]
        section_label = None
        
        # Check if the first node is a section
        if first_node_id in section_nodes:
            section_data = nodes_dict[first_node_id]
            section_label = section_data.label
            
            # Skip threads that are just section nodes themselves
            if len(path_nodes) == 1:
                continue
        
        # If no direct section start, check if any node in the path belongs to a section
        if not section_label:
            for node_id in path_nodes:
                # Check if this node has an incoming connection from a section
                for conn in incoming_connections.get(node_id, []):
                    from_node_id = conn.get('from_node')
                    if from_node_id in section_nodes:
                        section_data = nodes_dict[from_node_id]
                        section_label = section_data.label
                        break
                if section_label:
                    break
        
        # Store the thread in the appropriate section or as standalone
        if section_label:
            if section_label not in section_threads:
                section_threads[section_label] = []
                section_order.append(section_label)
            section_threads[section_label].append(path_info)
        else:
            standalone_threads.append(path_info)
    
    # Now output in nested chapter-like format
    
    # Print section-organized threads first
    for section_label in section_order:
        threads = section_threads[section_label]
        if not threads:
            continue
            
        # Print section header
        output_lines.append(f"\n==== Section: {section_label} ====")
        
        # Print each thread in this section with increased indentation
        for path_info in threads:
            step_thread_id = path_info["id"]
            path_type = path_info["type"]
            path_nodes = path_info["path"]
            
            output_lines.append(f"\n  == Step Thread {step_thread_id} ==")
            
            # --- Format Steps within the Thread ---
            step_number = 1
            start_index = 0
            
            # Skip the section node itself if it's the first node
            first_node_id = path_nodes[0]
            if first_node_id in section_nodes:
                start_index = 1
                
            for i in range(start_index, len(path_nodes)):
                node_id = path_nodes[i]
                node_data = nodes_dict[node_id]
                
                # Skip section nodes
                if node_id in section_nodes:
                    continue
                    
                # Format regular step or junction step
                template = node_data.filled_story_template or f"Node {node_data.name} ({node_id})"
                all_incoming = incoming_connections.get(node_id, [])
                input_info_str = format_input_info(node_id, step_thread_id, all_incoming)
                
                # Format and add the step line
                formatted_step = template.strip()
                if formatted_step and not formatted_step.endswith(('.', '?', '!')):
                    formatted_step += '.'
                formatted_step = formatted_step[0].upper() + formatted_step[1:] if formatted_step else template
                full_step = f"    {step_number}. {input_info_str}{formatted_step} ({node_data.label})"
                output_lines.append(full_step)
                step_number += 1
                
            # --- Format Connections ---
            if step_thread_id in path_connections:
                to_paths = path_connections[step_thread_id]["to_paths"]
                if to_paths:
                    output_lines.append("      Continues in:")
                    sorted_connections = sorted(to_paths, key=lambda x: x['path_id'])
                    for conn in sorted_connections:
                        output_lines.append(f"      - Step Thread {conn['path_id']} (via {conn['interface']} interface)")
    
    # Print standalone threads (not in any section)
    if standalone_threads:
        output_lines.append("\n==== Standalone Threads ====")
        
        for path_info in standalone_threads:
            step_thread_id = path_info["id"]
            path_nodes = path_info["path"]
            
            output_lines.append(f"\n  == Step Thread {step_thread_id} ==")
            
            # Format steps as above
            step_number = 1
            
            for i, node_id in enumerate(path_nodes):
                node_data = nodes_dict[node_id]
                
                # Skip section nodes (shouldn't happen in standalone threads)
                if node_id in section_nodes:
                    continue
                    
                template = node_data.filled_story_template or f"Node {node_data.name} ({node_id})"
                all_incoming = incoming_connections.get(node_id, [])
                input_info_str = format_input_info(node_id, step_thread_id, all_incoming)
                
                formatted_step = template.strip()
                if formatted_step and not formatted_step.endswith(('.', '?', '!')):
                    formatted_step += '.'
                formatted_step = formatted_step[0].upper() + formatted_step[1:] if formatted_step else template
                full_step = f"    {step_number}. {input_info_str}{formatted_step} ({node_data.label})"
                output_lines.append(full_step)
                step_number += 1
                
            # Format connections
            if step_thread_id in path_connections:
                to_paths = path_connections[step_thread_id]["to_paths"]
                if to_paths:
                    output_lines.append("      Continues in:")
                    sorted_connections = sorted(to_paths, key=lambda x: x['path_id'])
                    for conn in sorted_connections:
                        output_lines.append(f"      - Step Thread {conn['path_id']} (via {conn['interface']} interface)")

    # --- Summary Section ---
    output_lines.append(f"\nSummary: Identified {len(complete_paths)} distinct Step Threads/segments")
    
    # List Sections Found
    if section_nodes:
        output_lines.append("\nSections Found:")
        # Sort sections by Y then X position for readability
        section_info_list = []
        for sec_id in section_nodes:
             node = nodes_dict.get(sec_id)
             if node:
                  pos_x, pos_y = float('inf'), float('inf')
                  try:
                       pos_x = float(node.position.x)
                       pos_y = float(node.position.y)
                  except: pass
                  containing_threads = nodes_in_paths.get(sec_id, [])
                  thread_str = f" (contains {len(section_threads.get(node.label, []))} Step Threads)" if node.label in section_threads else ""
                  section_info_list.append((pos_y, pos_x, f"- {node.label}{thread_str} (Node ID: {sec_id})"))
        
        for _, _, line in sorted(section_info_list):
             output_lines.append(line)

    # Final join
    return "\n".join(output_lines)

def find_root(node_id: str, incoming_connections: Dict, nodes_dict: Dict) -> Optional[str]:
    """
    Helper function to trace backward from a node to find a root node.
    DEPRECATED - Path logic changed.
    """
    return node_id # Simplified return for now

def parse_all_paths_json(json_data: str) -> str:
    """
    Parses the process flow JSON to find all paths through the flow.
    
    Args:
        json_data (str): A JSON string containing the process flow data.
        
    Returns:
        str: A formatted string describing all identified process paths.
    """
    try:
        data = json.loads(json_data)
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON provided. {e}"
        
    # Convert JSON data to FlowSchema
    from barfi.flow.schema.types import build_flow_schema_from_dict
    
    try:
        # Handle both direct schema and nested schemas
        if "editor_schema" in data:
            schema_dict = data["editor_schema"]
        else:
            schema_dict = data
            
        flow_schema = build_flow_schema_from_dict(schema_dict)
        return parse_all_paths(flow_schema)
    except Exception as e:
        # Use traceback for detailed error
        import traceback
        tb_str = traceback.format_exc()
        print(f"Error details:\n{tb_str}")
        return f"Error: Failed to convert JSON to FlowSchema or parse paths. {e}"
