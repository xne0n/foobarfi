import pandas as pd
from typing import Dict, List, Any, Tuple
import json

def parse_flow_schema(schema: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parses a flow schema dict and returns two DataFrames:
    - sections_df: Each row is a section node, including parent and trigger info.
    - paths_df: Each row is a path starting from sections (excluding pure triggers)
                OR from lone paths. Narratives stop if they encounter another section.
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
            'section_id': sec['id'],
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
    # sections_df = pd.DataFrame(list(section_details.values())) # Build later after trigger check

    # 2. Identify ALL potential starting points for paths
    nodes_with_no_inputs = all_node_ids - target_node_ids
    lone_path_start_ids = nodes_with_no_inputs - section_node_ids
    all_potential_path_start_ids = section_node_ids | lone_path_start_ids

    # 3. Identify "Pure Trigger" sections
    pure_trigger_section_ids = set()
    for sec_id in section_node_ids:
        immediate_children = {tgt_id for conn, tgt_id in outgoing.get(sec_id, [])}
        if immediate_children and immediate_children.issubset(all_potential_path_start_ids):
             section_details[sec_id]['is_pure_trigger'] = True
             pure_trigger_section_ids.add(sec_id)

    # Finalize sections_df
    sections_df = pd.DataFrame(list(section_details.values()))

    # 4. Determine final starting points (exclude pure triggers)
    final_path_start_ids = all_potential_path_start_ids - pure_trigger_section_ids


    def get_node_desc(node):
        return node.get('filled_story_template') or node.get('story_template') or node.get('description', '')

    def get_node_label(node):
        return node.get('label') or node.get('name') or node.get('id')

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
                next_node_id = conn['inputNode']
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
                    step += f"\n{sub_narrative}"
                narrative_parts.append(step)
                branch_node_ids.extend([next_node_id] + sub_nodes)

            # Only add collected nodes if at least one branch continued
            if any_branch_continued:
                node_ids_in_path.extend(branch_node_ids)

        else:
            # Linear step
            conn, tgt_id = outs[0]
            next_node_id = conn['inputNode']
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
                step += f"\n{sub_narrative}"
            narrative_parts.append(step)
            node_ids_in_path.extend([next_node_id] + sub_nodes)

        # Filter out empty strings that might result from stopped branches
        final_narrative_parts = [part for part in narrative_parts if part]
        # Check if branching narrative only contains the header and stopped branches
        if len(outs) > 1 and len(final_narrative_parts) == 1 and final_narrative_parts[0].startswith(f"{'  '*indent}Here we have"):
             # If only the header remains after stopping all branches, return empty
             return "", []


        return "\n".join(final_narrative_parts), node_ids_in_path


    path_rows = []
    # Iterate over FINAL identified starting points (excluding pure triggers)
    for start_id in final_path_start_ids:
        start_node = node_map[start_id]
        start_label = get_node_label(start_node)
        start_desc = get_node_desc(start_node)
        parent_section_id = None
        is_section_start = start_id in section_node_ids
        final_narrative = ""
        nodes_in_path_ids = []

        if is_section_start:
            # For sections, the narrative starts directly with the first node's description
            full_narrative, nodes_in_path_ids = build_narrative_recursive(start_id, 0, is_first_step_after_section=True)
            final_narrative = full_narrative
            parent_section_id = section_details.get(start_id, {}).get('parent_section_id')
        else: # Lone path start
             start_desc_with_label = f"{start_desc} ({start_label})" if start_desc else f"({start_label})"
             initial_narrative_str = f"Starting from '{start_label}': {start_desc_with_label}"
             full_narrative, nodes_in_path_ids = build_narrative_recursive(start_id, 0, is_first_step_after_section=False)
             final_narrative = initial_narrative_str
             if full_narrative:
                 final_narrative += f"\n{full_narrative}"

        # Only add row if narrative is not empty (i.e., path didn't immediately stop)
        if final_narrative:
            # Get labels for the path nodes (always include the start node)
            unique_ordered_nodes = list(dict.fromkeys([start_id] + nodes_in_path_ids))
            path_labels = [get_node_label(node_map[nid]) for nid in unique_ordered_nodes]

            path_rows.append({
                'start_node_id': start_id,
                'parent_section_id': parent_section_id,
                'is_section_start': is_section_start,
                'path_nodes': path_labels,
                'parsed_text': final_narrative,
                'raw_path': [node_map[nid] for nid in unique_ordered_nodes if nid != start_id]
            })

    paths_df = pd.DataFrame(path_rows)
    return sections_df, paths_df

 
