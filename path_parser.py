import pandas as pd
from typing import Dict, List, Any, Tuple, Union
import json

def parse_flow_schema(schema: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Parses a flow schema dict and returns:
    - sections_df: DataFrame of sections with summaries for root sections.
    - paths_df: DataFrame of individual paths with narratives.
    - full_flow_narrative: A single string combining all sorted root section narratives
                           and sorted lone path narratives.
    """
    nodes = schema.get('nodes', [])
    connections = schema.get('connections', [])

    # Build node lookup by id
    node_map = {str(n['id']): n for n in nodes}
    all_node_ids = set(node_map.keys())
    section_node_ids = {str(n['id']) for n in nodes if n.get('type', '').lower() == 'section'}

    # Build connection lookup
    outgoing = {}
    incoming = {}
    target_node_ids = set()
    for conn in connections:
        # Ensure IDs are strings for consistency
        out_id = str(conn['outputNode'])
        in_id = str(conn['inputNode'])
        target_node_ids.add(in_id)
        outgoing.setdefault(out_id, []).append((conn, in_id))
        incoming.setdefault(in_id, []).append((conn, out_id))

    # 1. Extract all section nodes and identify parents/roots
    section_details = {}
    child_section_ids = set()
    for sec_id in section_node_ids:
        parent_id = None
        for conn, source_id in incoming.get(sec_id, []):
            if source_id in section_node_ids:
                parent_id = source_id
                child_section_ids.add(sec_id)
                break
        sec = node_map[sec_id]
        section_details[sec_id] = {
            'section_id': sec['id'], # Keep original ID type if needed elsewhere
            'label': sec.get('label', ''),
            'description': sec.get('filled_story_template') or sec.get('story_template') or sec.get('description', ''),
            'position': sec.get('position', {}),
            'parent_section_id': parent_id,
            'is_root': True,
            'is_pure_trigger': False,
            'raw': sec
        }
    for sec_id in section_details:
         section_details[sec_id]['is_root'] = sec_id not in child_section_ids

    # 2. Identify ALL potential starting points for paths
    nodes_with_no_inputs = all_node_ids - target_node_ids
    lone_path_start_ids = nodes_with_no_inputs - section_node_ids
    all_potential_path_start_ids = section_node_ids | lone_path_start_ids

    # 3. Identify "Pure Trigger" sections
    pure_trigger_section_ids = set()
    for sec_id in section_node_ids:
        immediate_children_ids = {str(conn['inputNode']) for conn, _ in outgoing.get(sec_id, [])}
        # Check if all immediate children are *also* potential path starts (sections or lone starts)
        # AND that none of the children are the section itself (prevent self-loops defining pure trigger)
        is_pure = False
        if immediate_children_ids: # Only consider if it has outputs
             is_pure = True
             for child_id in immediate_children_ids:
                 # A pure trigger's outputs must lead ONLY to other sections or lone path starts
                 # It cannot lead to an intermediate node within its own conceptual 'flow'
                 if child_id not in all_potential_path_start_ids:
                     is_pure = False
                     break
                 # Also, ensure it doesn't just loop back to itself directly if that's the only output
                 # (Though the primary check is sufficient if self-loops aren't lone path starts)

        if is_pure:
             section_details[sec_id]['is_pure_trigger'] = True
             pure_trigger_section_ids.add(sec_id)


    # Finalize sections_df
    sections_df = pd.DataFrame(list(section_details.values()))

    # 4. Determine final starting points (exclude pure triggers)
    final_path_start_ids = all_potential_path_start_ids - pure_trigger_section_ids


    def get_node_desc(node):
        # Prioritize filled_story_template
        return node.get('filled_story_template') or node.get('story_template') or node.get('description', '') or f"Node {node.get('id')}"

    def get_node_label(node):
        return node.get('label') or node.get('name') or node.get('id')

    # --- Original Narrative Builder ---
    def build_narrative_recursive(node_id, indent=0, is_first_step_after_section=False):
        """Builds the recursive narrative for steps *after* the given node_id.
           Stops if the next node is a section."""
        outs = outgoing.get(node_id, [])
        if not outs:
            return "", [] # Narrative, node_ids_in_path

        narrative_parts = []
        node_ids_in_path = []

        if len(outs) > 1:
            # Branching
            branch_narr = f"{'  '*indent}Here we have now {len(outs)} outputs/options from '{get_node_label(node_map[node_id])}':"
            narrative_parts.append(branch_narr)
            branch_node_ids = []
            any_branch_continued = False # Track if any branch leads to non-section
            for conn, tgt_id in outs:
                next_node_id = str(conn['inputNode']) # Ensure string ID
                # Check if the next node is a section
                if next_node_id in section_node_ids:
                    # Stop this branch narrative here
                    label = get_node_label(node_map[next_node_id])
                    step = f"{'  '*(indent+1)}- Using '{conn['outputNodeInterface']}' leads to Section '{label}'."
                    narrative_parts.append(step)
                    # Do not recurse, do not add to branch_node_ids
                    continue

                # Continue narrative for non-section branch
                any_branch_continued = True
                next_input_name = conn['inputNodeInterface']
                next_node = node_map[next_node_id]
                label = get_node_label(next_node)
                desc = get_node_desc(next_node)
                desc_with_label = f"{desc} ({label})" if desc else f"({label})"
                step = f"{'  '*(indent+1)}- Using '{conn['outputNodeInterface']}' as '{next_input_name}' to '{label}', we do: {desc_with_label}"
                sub_narrative, sub_nodes = build_narrative_recursive(next_node_id, indent + 2, is_first_step_after_section=False)
                if sub_narrative:
                    step += f"\\n{sub_narrative}"
                narrative_parts.append(step)
                branch_node_ids.extend([next_node_id] + sub_nodes)

            # Only add collected nodes if at least one branch continued
            if any_branch_continued:
                node_ids_in_path.extend(branch_node_ids)

        else:
            # Linear step
            conn, tgt_id = outs[0]
            next_node_id = str(conn['inputNode']) # Ensure string ID
            # Check if the next node is a section
            if next_node_id in section_node_ids:
                 # Stop the path here, don't describe the section transition
                 return "", [] # Return empty narrative and nodes for this path end

            # Continue narrative for non-section step
            next_input_name = conn['inputNodeInterface']
            next_node = node_map[next_node_id]
            label = get_node_label(next_node)
            desc = get_node_desc(next_node)
            desc_with_label = f"{desc} ({label})" if desc else f"({label})"

            if is_first_step_after_section:
                step = f"{'  '*indent}{desc_with_label}"
            else:
                step = f"{'  '*indent}Then, using '{conn['outputNodeInterface']}' from '{get_node_label(node_map[node_id])}' as '{next_input_name}' to '{label}', we do: {desc_with_label}"

            sub_narrative, sub_nodes = build_narrative_recursive(next_node_id, indent, is_first_step_after_section=False)
            if sub_narrative:
                step += f"\\n{sub_narrative}"
            narrative_parts.append(step)
            node_ids_in_path.extend([next_node_id] + sub_nodes)

        # Filter out empty strings that might result from stopped branches
        final_narrative_parts = [part for part in narrative_parts if part]
        # Check if branching narrative only contains the header and stopped branches
        if len(outs) > 1 and len(final_narrative_parts) == 1 and final_narrative_parts[0].startswith(f"{'  '*indent}Here we have"):
             # If only the header remains after stopping all branches, return empty
             return "", []


        return "\\n".join(final_narrative_parts), node_ids_in_path


    # --- New Indented Narrative Builder ---
    def build_indented_narrative_recursive(node_id, indent_level):
        """Builds the recursive indented narrative using filled_story_template.
           Stops if the next node is a section. Returns a list of strings."""
        outs = outgoing.get(node_id, [])
        if not outs:
            return []

        narrative_lines = []
        indent_str = "  " * indent_level

        for conn, _ in outs:
            next_node_id = str(conn['inputNode']) # Ensure string ID

            # Stop if the next node is a section
            if next_node_id in section_node_ids:
                continue # Skip this branch entirely for the narrative

            # Proceed with non-section nodes
            next_node = node_map[next_node_id]
            output_label = conn['outputNodeInterface']
            next_node_desc = get_node_desc(next_node) # Uses filled_story_template first

            line = f"{indent_str}- {output_label} : {next_node_desc}"
            narrative_lines.append(line)

            # Recurse for the next level
            sub_lines = build_indented_narrative_recursive(next_node_id, indent_level + 1)
            narrative_lines.extend(sub_lines)

        return narrative_lines


    path_rows = []
    # Iterate over FINAL identified starting points (excluding pure triggers)
    for start_id in final_path_start_ids:
        start_node = node_map[start_id]
        start_label = get_node_label(start_node)
        start_desc = get_node_desc(start_node) # Uses filled_story_template first
        parent_section_id = None
        is_section_start = start_id in section_node_ids

        # --- Generate Original Narrative ---
        original_narrative = ""
        nodes_in_path_ids_original = []
        if is_section_start:
            full_narrative, nodes_in_path_ids_original = build_narrative_recursive(start_id, 0, is_first_step_after_section=True)
            original_narrative = full_narrative
            parent_section_id = section_details.get(start_id, {}).get('parent_section_id')
        else: # Lone path start
             start_desc_with_label = f"{start_desc} ({start_label})" if start_desc else f"({start_label})"
             initial_narrative_str = f"Starting from '{start_label}': {start_desc_with_label}"
             full_narrative, nodes_in_path_ids_original = build_narrative_recursive(start_id, 0, is_first_step_after_section=False)
             original_narrative = initial_narrative_str
             if full_narrative:
                 original_narrative += f"\\n{full_narrative}"

        # --- Generate New Indented Narrative ---
        # We need a way to collect nodes *only* along the paths that don't hit sections
        # Let's modify the indented builder slightly to return nodes too
        def build_indented_narrative_and_nodes_recursive(node_id, indent_level):
            outs = outgoing.get(node_id, [])
            lines = []
            node_ids = []
            indent_str = "  " * indent_level
            for conn, _ in outs:
                next_node_id = str(conn['inputNode'])
                if next_node_id in section_node_ids:
                    continue
                next_node = node_map[next_node_id]
                output_label = conn['outputNodeInterface']
                next_node_desc = get_node_desc(next_node)
                line = f"{indent_str}- {output_label} : {next_node_desc}"
                lines.append(line)
                current_branch_nodes = [next_node_id]
                sub_lines, sub_nodes = build_indented_narrative_and_nodes_recursive(next_node_id, indent_level + 1)
                lines.extend(sub_lines)
                current_branch_nodes.extend(sub_nodes)
                node_ids.extend(current_branch_nodes) # Add nodes from this valid branch
            return lines, node_ids

        # Regenerate indented narrative along with node IDs, adjusting start based on section
        if is_section_start:
            # If starting from a section, the narrative begins directly with its outputs at indent level 0
            following_indented_lines, following_node_ids = build_indented_narrative_and_nodes_recursive(start_id, 0) # Start indent at 0
            final_indented_narrative = "\n".join(following_indented_lines) # Use actual newline
            # Node IDs only include the children found by the recursive call
            nodes_in_indented_path_ids = following_node_ids
        else:
            # If starting from a non-section node, include its description first at indent level 0
            initial_indented_line = f"- {start_desc}" # Indent level 0
            # Subsequent lines start at indent level 1
            following_indented_lines, following_node_ids = build_indented_narrative_and_nodes_recursive(start_id, 1) # Start indent at 1
            final_indented_narrative = "\n".join([initial_indented_line] + following_indented_lines) # Use actual newline
            # Node IDs include the start node plus children
            nodes_in_indented_path_ids = [start_id] + following_node_ids


        # Only add row if *either* narrative has content (original might exist even if indented stops immediately)
        # Or maybe only if indented has content? Let's stick to original logic: add if original path existed.
        # Re-evaluate: The user wants the *new* format. Add row only if the new format is non-trivial.
        # A non-trivial indented narrative always has at least the starting line.
        # Let's add the row if the path starts. The narrative might just be the first node.
        # Use the original logic's condition based on `original_narrative` for row inclusion consistency
        if original_narrative: # Keep condition based on original narrative for row inclusion consistency
            # Get labels for the path nodes (always include the start node)
            # Use nodes from the *original* path for 'path_nodes' and 'raw_path' for consistency
            unique_ordered_nodes_original = list(dict.fromkeys([start_id] + nodes_in_path_ids_original))
            path_labels_original = [get_node_label(node_map[nid]) for nid in unique_ordered_nodes_original]
            raw_path_original = [node_map[nid] for nid in unique_ordered_nodes_original] # Include start node in raw path

            # Get nodes for the indented path (unique and ordered)
            unique_ordered_nodes_indented = list(dict.fromkeys(nodes_in_indented_path_ids))


            path_rows.append({
                'start_node_id': start_id,
                'start_node_label': start_label, # Add label for lone path header
                'start_node_description': start_desc, # Add description for sorting/header
                'parent_section_id': parent_section_id, # From original logic
                'is_section_start': is_section_start,
                'path_nodes': path_labels_original, # Based on original full path
                'parsed_text': original_narrative, # Original narrative
                'indented_narrative': final_indented_narrative, # New indented narrative
                'raw_path_nodes_original': raw_path_original, # Raw nodes for original path
                'raw_path_nodes_indented': [node_map[nid] for nid in unique_ordered_nodes_indented] # Raw nodes for indented path
            })

    paths_df = pd.DataFrame(path_rows)
    # Reorder columns if desired
    if not paths_df.empty:
        paths_df = paths_df[[
            'start_node_id', 'start_node_label', 'start_node_description', # Added columns
            'parent_section_id', 'is_section_start',
            'path_nodes', 'parsed_text', 'indented_narrative',
            'raw_path_nodes_original', 'raw_path_nodes_indented'
        ]]

    # --- Build Full Section Narratives (Hierarchical) ---

    def build_full_section_narrative(section_id, current_sections_df, current_paths_df, current_node_map, indent_level=0):
        indent_str = "  " * indent_level
        narrative_parts = []

        # Get current section details
        try:
            section_row = current_sections_df[current_sections_df['section_id'] == section_id].iloc[0]
        except IndexError:
            return ""

        # Use description (filled_story_template priority) directly as header, indented
        section_desc = section_row['description']
        if section_desc:  # Only add header if description exists
             # Use markdown bullet for section header
             header = f"{indent_str}- {section_desc}"
             narrative_parts.append(header)

        # Find paths starting directly from this section
        section_paths = current_paths_df[current_paths_df['start_node_id'] == section_id]
        if not section_paths.empty:
            path_narrative = section_paths.iloc[0]['indented_narrative']
            if path_narrative:
                # Re-indent the existing path narrative relative to the section header (add one level)
                # The path narrative already starts with correct relative indentation (level 0 for section outputs)
                # So we just need to prepend the section's indent + one more level
                path_indent_str = "  " * (indent_level + 1)
                reindented_path_narrative = "\n".join([f"{path_indent_str}{line.lstrip()}" for line in path_narrative.split('\n') if line.strip()])
                narrative_parts.append(reindented_path_narrative)

        # Find child sections
        child_sections = current_sections_df[current_sections_df['parent_section_id'] == section_id]
        child_sections = child_sections.sort_values(by='label') # Keep sorting by label for consistent order

        for _, child_row in child_sections.iterrows():
            child_id = child_row['section_id']
            # Recursively build narrative for child section, increase indent
            child_narrative = build_full_section_narrative(child_id, current_sections_df, current_paths_df, current_node_map, indent_level + 1)
            if child_narrative:
                narrative_parts.append(child_narrative)

        # Join parts with single newline
        return "\n".join(narrative_parts)

    # Add the new column to sections_df
    sections_df['full_section_narrative'] = None # Initialize column

    # Identify and sort root sections
    root_sections = sections_df[sections_df['is_root'] == True]

    # Generate the full narrative for each root section
    for index, root_row in root_sections.iterrows():
        root_id = root_row['section_id']
        full_narrative = build_full_section_narrative(root_id, sections_df, paths_df, node_map, indent_level=0)
        sections_df.loc[index, 'full_section_narrative'] = full_narrative

    # --- Assemble Final Combined Narrative ---
    # Prepare a flat list of all lines, preserving indentation depth
    all_lines: List[str] = []

    # Sort root sections: 'Start'/'Main' first, then alphanumeric by description label
    def sort_key(desc: Union[str, None]) -> str:
        low = desc.lower() if isinstance(desc, str) else ''
        window = low[:20]
        if 'start' in window or 'main' in window:
            return '!'
        return desc if isinstance(desc, str) else '~~~'
    sections_df['__sort_key'] = sections_df['description'].apply(sort_key)
    sorted_root_sections = sections_df[sections_df['is_root'] == True].sort_values(
        by=['__sort_key', 'label'], ascending=True
    )
    sections_df.drop(columns=['__sort_key'], inplace=True)

    # Append each root section's lines
    for _, root_row in sorted_root_sections.iterrows():
        narrative = root_row['full_section_narrative']
        if isinstance(narrative, str) and narrative.strip():
            for line in narrative.split("\n"):
                all_lines.append(line)
            # Blank line separates sections
            all_lines.append("")

    # Prepare lone paths sorted by description
    lone_paths = paths_df[paths_df['is_section_start'] == False].copy()
    lone_paths.sort_values(by='start_node_description', ascending=True, inplace=True, na_position='last')
    if not lone_paths.empty:
        all_lines.append("Additional instructions :")
        for _, lone_row in lone_paths.iterrows():
            path_narrative = lone_row['indented_narrative']
            if isinstance(path_narrative, str) and path_narrative.strip():
                for line in path_narrative.split("\n"):
                    all_lines.append(line)
                all_lines.append("")

    # Join all lines with single newline, preserving indentation
    full_flow_narrative = "\n".join(all_lines).rstrip()

    return sections_df, paths_df, full_flow_narrative


