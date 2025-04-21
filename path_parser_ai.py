import pandas as pd
from sogenai_chat_model import SocGenAIChatModel
import time
from typing import Tuple # Import Tuple
 
# Updated function signature and logic
def process_schema_with_ai(sections_df: pd.DataFrame, paths_df: pd.DataFrame, ai_model: SocGenAIChatModel) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processes the parsed schema DataFrames using an AI model, primarily to generate
    summaries for root sections based on their full hierarchical narrative.

    Args:
        sections_df: DataFrame containing section information, including 'full_section_narrative' for roots.
        paths_df: DataFrame containing individual path information.
        ai_model: An instance of the AI model for generation.

    Returns:
        A tuple containing:
        - sections_df: The input sections DataFrame potentially updated with AI-generated summaries.
        - paths_df: The original paths DataFrame (as paths are not modified here).
    """
    sections = sections_df.copy()
    # paths = paths_df.copy() # No longer modifying paths_df directly

    # 1. Summarize each root section using its full_section_narrative
    summaries = {} # Use dict for easier mapping back
    # Iterate through root sections that have a narrative generated
    root_sections_with_narrative = sections[
        sections['is_root'].fillna(False) & sections['full_section_narrative'].notna() & (sections['full_section_narrative'] != '')
    ]

    print(f"Generating summaries for {len(root_sections_with_narrative)} root sections...") # Add print statement

    for index, sec in root_sections_with_narrative.iterrows():
        sec_id = sec['section_id']
        full_narrative = sec['full_section_narrative']

        # Check if narrative is substantial enough to summarize
        if not full_narrative or len(full_narrative.split()) < 5: # Skip very short/empty narratives
             print(f"Skipping summary for section {sec.get('label', sec_id)} due to short narrative.")
             summaries[index] = "Narrative too short to summarize." # Placeholder or skip
             continue

        prompt = (
            "Please provide a concise summary of the following section narrative:\n\n"
            f"{full_narrative}"
        )
        try:
            print(f"Sending prompt for section: {sec.get('label', sec_id)}") # Debug print
            resp = generate_with_retry(ai_model, [prompt])
            summary = resp.generations[0][0].text.strip()
            summaries[index] = summary
            print(f"Received summary for section: {sec.get('label', sec_id)}") # Debug print
        except Exception as e:
            print(f"Error generating summary for section {sec.get('label', sec_id)}: {e}")
            summaries[index] = f"Error during summary generation: {e}" # Store error message

    # Add summaries to the sections DataFrame
    # Ensure the 'summary' column exists
    if 'summary' not in sections.columns:
        sections['summary'] = None # Or pd.NA

    # Map summaries back using the index
    summary_series = pd.Series(summaries)
    sections['summary'] = summary_series.combine_first(sections['summary']) # Update existing column, keeping old values if no new summary

    print("Summary generation complete.")

    # No longer rewriting paths or checking lone path coverage here
    # Return the updated sections and the original paths
    return sections, paths_df


def generate_with_retry(ai_model: SocGenAIChatModel, prompts: list[str], retries: int = 3, base_wait: float = 1.0):
    """Call ai_model.generate with retries on errors (e.g., rate limits)."""
    for attempt in range(retries):
        try:
            # Assuming ai_model.generate returns an object with .generations[0][0].text
            return ai_model.generate(prompts)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}") # Add print for errors
            if attempt < retries - 1:
                wait_time = base_wait * (2 ** attempt)
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                continue
            else:
                print("Max retries reached. Raising exception.")
                raise
