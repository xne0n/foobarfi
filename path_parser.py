import pandas as pd
from typing import Dict, List, Any

def parse_flow_schema(schema: Dict[str, Any]) -> pd.DataFrame:
    nodes = {str(node['id']): node for node in schema['nodes']}
    connections = schema.get('connections', [])

    # Build input/output maps
    input_map = {str(node['id']): [] for node in schema['nodes']}
    output_map = {str(node['id']): [] for node in schema['nodes']}
    for conn in connections:
        output_map[str(conn['outputNode'])].append(conn)
        input_map[str(conn['inputNode'])].append(conn)

    def connection_text(conn):
        return f"Using {conn['outputNodeInterface']} from this step as {conn['inputNodeInterface']}."

    def node_desc(node):
        return node.get('filled_story_template') or node.get('story_template') or node.get('name')

    def is_section(node):
        # Ensure node is not None before accessing 'type'
        return node and node.get('type', '').lower() == 'section'

    # Helper to find all root nodes (no incoming connections)
    def find_roots():
        return [nid for nid, ins in input_map.items() if not ins]

    # Recursive path finder (handles merges/branches)
    def trace_path(current, visited):
        paths = []
        if current not in nodes: # Check if node exists
             return []
        if current in visited:
            return []

        node = nodes[current]
        if is_section(node):
            return []

        # Mark current node as visited for this path trace
        v = visited.copy()
        v.add(current)

        # If this node is a merge (multiple inputs), trace each input as a separate path
        in_conns = input_map.get(current, [])
        if len(in_conns) > 1:
            for conn in in_conns:
                prev_nid = str(conn['outputNode'])
                # Avoid infinite loops by checking if prev_nid is already in visited
                if prev_nid not in visited:
                    subpaths = trace_path(prev_nid, v) # Pass updated visited set
                    for sp in subpaths:
                        # Add this node and connection to the end of each subpath
                        sp['nodes'].append(node)
                        sp['conns'].append(conn)
                        paths.append(sp)
            # If it's a merge point but has no valid incoming paths traced, treat as a root for this branch
            if not paths and not in_conns: # Check if it truly has no connections
                 paths.append({'nodes': [node], 'conns': []})
            elif not paths and in_conns: # If incoming connections exist but led to cycles/sections
                 paths.append({'nodes': [node], 'conns': []}) # Treat as start of a path segment

            return paths

        # Otherwise, traverse forward linearly
        path_nodes = [node]
        path_conns = []
        current_nid_fwd = current # Use a different variable for forward traversal

        while True: # Loop for forward traversal
            outs = output_map.get(current_nid_fwd, [])
            if len(outs) != 1: # Stop if branch or leaf
                 break
            conn = outs[0]
            next_node_id = str(conn['inputNode'])

            if next_node_id not in nodes: # Check node existence
                 break
            if next_node_id in v: # Stop if cycle detected
                 break
            next_node = nodes[next_node_id]
            if is_section(next_node): # Stop if next is a section
                 break

            path_conns.append(conn)
            path_nodes.append(next_node)
            v.add(next_node_id) # Add to visited set for this path
            current_nid_fwd = next_node_id # Move forward

        # If the path starts from a node with no incoming connections (a true root)
        if not in_conns:
             paths.append({'nodes': path_nodes, 'conns': path_conns})
        # If it starts from a node with one incoming connection (part of a larger path potentially)
        elif len(in_conns) == 1:
             prev_nid = str(in_conns[0]['outputNode'])
             if prev_nid not in visited: # Avoid cycles
                 subpaths = trace_path(prev_nid, v) # Trace back from the single input
                 for sp in subpaths:
                     sp['nodes'].extend(path_nodes) # Append current linear segment
                     sp['conns'].extend([in_conns[0]] + path_conns) # Prepend incoming conn
                     paths.append(sp)
             else: # Cycle detected, treat current segment as a path
                 paths.append({'nodes': path_nodes, 'conns': path_conns})

        # If no paths were generated (e.g., started on an isolated node), add it
        if not paths and not is_section(node):
             paths.append({'nodes': [node], 'conns': []})

        return paths


    # Find all root nodes and trace all paths
    all_paths_raw = []
    processed_starts = set()

    # Start tracing from true roots first
    for root in find_roots():
        if root not in processed_starts:
             paths = trace_path(root, set())
             all_paths_raw.extend(paths)
             processed_starts.add(root) # Mark root as processed

    # Trace from other nodes if they weren't part of paths found from roots
    # This handles disconnected graphs or nodes only reachable via merges
    for nid in nodes:
         if nid not in processed_starts and not is_section(nodes[nid]):
             # Check if this node was already included in any path found so far
             already_covered = False
             for p in all_paths_raw:
                 if any(str(n['id']) == nid for n in p['nodes']):
                     already_covered = True
                     break
             if not already_covered:
                 paths = trace_path(nid, set())
                 all_paths_raw.extend(paths)
                 processed_starts.add(nid) # Mark this start as processed


    # --- Deduplicate and Filter Subpaths ---
    unique_paths_data = []
    seen_node_sequences = set()

    for path in all_paths_raw:
        if not path['nodes']:
            continue
        node_ids_tuple = tuple(str(n['id']) for n in path['nodes'])
        if node_ids_tuple not in seen_node_sequences:
            unique_paths_data.append(path)
            seen_node_sequences.add(node_ids_tuple)

    # Filter subpaths
    path_node_ids = [ [str(n['id']) for n in path['nodes']] for path in unique_paths_data ]
    keep = [True] * len(unique_paths_data)

    def is_subpath(short, long):
        if not short or not long or len(short) > len(long): return False
        short_tuple = tuple(short)
        for i in range(len(long) - len(short) + 1):
            if tuple(long[i:i+len(short)]) == short_tuple:
                return True
        return False

    for i, ids_i in enumerate(path_node_ids):
        for j, ids_j in enumerate(path_node_ids):
            if i != j and is_subpath(ids_i, ids_j):
                keep[i] = False
                break

    maximal_paths = [path for path, k in zip(unique_paths_data, keep) if k]

    # --- Build DataFrame Rows ---
    final_rows = []
    path_order_map = {} # Map path_data object id to order

    # Add section rows first
    # Sort sections by X position for consistent ordering
    section_nodes_sorted = sorted([n for n in nodes.values() if is_section(n)], key=lambda n: n['position']['x'])
    current_section_stack_for_context = []
    section_id_map = {} # Map description back to ID for later update
    for node in section_nodes_sorted:
         # Logic to manage section stack based on X position for nesting context
         while current_section_stack_for_context and node['position']['x'] <= current_section_stack_for_context[-1]['position']['x']:
             current_section_stack_for_context.pop()
         current_section_stack_for_context.append(node)
         section_id_map[node_desc(node)] = str(node['id']) # Store ID by description

         final_rows.append({
             'Type': 'section',
             'Description': node_desc(node),
             'First Inputs': [], # Sections don't have these properties in the context of paths
             'Last Outputs': [],
             'Contained Paths': [] # Initialize
         })

    # Build path rows and map first node to order
    path_firstnode_to_order = {}
    for idx, path_data in enumerate(maximal_paths):
        order = idx + 1
        path_order_map[id(path_data)] = order # Use object id as key

        path_nodes = path_data['nodes']
        path_conns = path_data['conns']

        first_node_id = str(path_nodes[0]['id'])
        path_firstnode_to_order[first_node_id] = order

        first_inputs = [inp.get('name') for inp in path_nodes[0].get('inputs', []) if inp.get('name')]
        last_outputs = [out.get('name') for out in path_nodes[-1].get('outputs', []) if out.get('name')]
        descs = [node_desc(n) for n in path_nodes]

        # Improved readability description
        if descs:
            full_desc = f"Step 1: {descs[0]}:"
            # Adjust connection text logic to match nodes and connections correctly
            conn_idx = 0
            for i in range(len(path_nodes) - 1):
                 # Find the connection between node i and node i+1
                 current_node_id = str(path_nodes[i]['id'])
                 next_node_id_in_path = str(path_nodes[i+1]['id'])
                 found_conn = None
                 # Search in path_conns for the specific link
                 temp_conn_idx = 0
                 for k, c in enumerate(path_conns):
                      # Check both forward and backward possibilities due to trace logic
                      if str(c.get('outputNode')) == current_node_id and str(c.get('inputNode')) == next_node_id_in_path:
                           found_conn = c
                           # Ideally, remove conn from list or use index carefully if order matters
                           break
                      # If trace_path added connections in reverse for merge points:
                      if str(c.get('inputNode')) == current_node_id and str(c.get('outputNode')) == next_node_id_in_path:
                           # This case might occur if trace_path logic appends connections differently
                           # We need consistent connection direction representation
                           # For now, assume connections always point output -> input
                           pass # Adjust if needed based on trace_path output

                 if found_conn:
                      full_desc += f"\nthen Step {i+2}: {descs[i+1]}:\n    ↳ {connection_text(found_conn)}"
                 else:
                      # Fallback if connection not found (should not happen in valid path)
                      full_desc += f"\nthen Step {i+2}: {descs[i+1]}:"

        else:
            full_desc = ''

        final_rows.append({
            'Type': 'path',
            'Description': full_desc,
            'First Inputs': first_inputs,
            'Last Outputs': last_outputs,
            'Order': order
        })


    # --- Assign Paths to Sections (Backward Trace) ---
    # Helper function to find the owning section by tracing backwards (BFS)
    def find_owning_section(start_node_id):
        queue = [(start_node_id, [start_node_id])] # (current_node_id, path_taken_history)
        visited_trace = {} # Store first section found from each node: node_id -> section_id

        while queue:
            current_nid, path_hist = queue.pop(0)

            # If we already found the owning section starting from this node, return it
            if current_nid in visited_trace:
                 # If visited_trace[current_nid] is None, it means this branch doesn't lead to a section
                 # If it's a section_id, we return that
                 if visited_trace[current_nid] is not None:
                      return visited_trace[current_nid]
                 else:
                      continue # This branch is a dead end for finding sections

            # Check incoming connections
            in_conns = input_map.get(current_nid, [])
            found_section_on_this_level = None

            if not in_conns:
                # Reached a root node without finding a section on this branch
                visited_trace[current_nid] = None
                continue

            potential_parents = []
            for conn in in_conns:
                prev_nid = str(conn['outputNode'])
                if prev_nid in path_hist: # Avoid cycles within this specific trace
                    continue

                prev_node = nodes.get(prev_nid)
                if prev_node and is_section(prev_node):
                    # Found a section directly connected
                    found_section_on_this_level = prev_nid
                    # Don't return immediately, check all direct parents first
                    # If multiple direct sections, maybe return None or prioritize? For now, take the first found.
                    # Let's prioritize: if we find one, store it and continue BFS level
                    visited_trace[current_nid] = found_section_on_this_level
                    # We can actually return here, as BFS guarantees shortest path
                    return found_section_on_this_level

                # If not a section, add to potential parents to explore further back
                potential_parents.append(prev_nid)

            # If no direct section found, add valid parents to queue
            if found_section_on_this_level is None:
                 visited_trace[current_nid] = None # Mark as visited, no section found *yet* from here
                 for parent_nid in potential_parents:
                      if parent_nid not in visited_trace: # Only queue if not already processed
                           new_path_hist = path_hist + [parent_nid]
                           queue.append((parent_nid, new_path_hist))

        # If queue finishes and start_node_id wasn't resolved to a section
        return visited_trace.get(start_node_id, None)


    # Assign paths to sections
    section_to_orders = {sid: set() for sid in section_id_map.values()}

    for first_node_id, order in path_firstnode_to_order.items():
        owning_section_id = find_owning_section(first_node_id)
        if owning_section_id and owning_section_id in section_to_orders:
            section_to_orders[owning_section_id].add(order)

    # Update the section rows in the final_rows DataFrame
    for row in final_rows:
        if row['Type'] == 'section':
            section_desc = row['Description']
            matching_section_id = section_id_map.get(section_desc)
            if matching_section_id:
                row['Contained Paths'] = sorted(list(section_to_orders.get(matching_section_id, set())))

    return pd.DataFrame(final_rows)
