import pandas as pd
from sogenai_chat_model import SocGenAIChatModel
import time
 
def process_schema_with_ai(sections_df: pd.DataFrame, paths_df: pd.DataFrame, ai_model: SocGenAIChatModel) -> tuple[pd.DataFrame, pd.DataFrame]:
    sections = sections_df.copy()
    paths = paths_df.copy()

    # 1. Sort sections: 'Start' or 'Main' first, then alphanumeric by description
    def sort_key(desc: str) -> str:
        low = desc.lower() if isinstance(desc, str) else ''
        # detect 'start' or 'main' within the first 20 characters
        window = low[:20]
        if 'start' in window or 'main' in window:
            return ''
        return desc or ''

    sections['__sort_key'] = sections['description'].map(sort_key)
    sections = sections.sort_values(['__sort_key', 'description'], ascending=True)
    sections = sections.drop(columns=['__sort_key'])

    # 2. Rewrite steps for each path
    rewritten = []
    for _, row in paths.iterrows():
        text = row.get('parsed_text', '') or ''
        prompt = (
            "Rewrite the following steps in a clear, structured format (e.g., numbered or bullet list):\n"
            f"{text}"
        )
        resp = generate_with_retry(ai_model, [prompt])
        steps = resp.generations[0][0].text.strip()
        rewritten.append(steps)
    paths['rewritten_steps'] = rewritten

    # 3. Summarize each section using rewritten steps
    summaries = []
    for _, sec in sections.iterrows():
        sec_id = sec['section_id']
        desc = sec.get('description', '') or ''
        # Collect all rewritten steps for this section
        subset = paths[paths['parent_section_id'] == sec_id]
        texts = subset['rewritten_steps'].tolist()
        combined = "\n---\n".join(texts) if texts else ''
        prompt = (
            f"Section Description: {desc}\n"
            f"Associated Rewritten Steps:\n{combined}\n\n"
            "Please provide a concise summary of this section."
        )
        resp = generate_with_retry(ai_model, [prompt])
        summary = resp.generations[0][0].text.strip()
        summaries.append(summary)
    sections['summary'] = summaries

    # 4. Check coverage of lone paths
    uncovered = paths[(paths['is_section_start'] == False) & (paths['rewritten_steps'].isna())]
    if not uncovered.empty:
        print(f"Warning: {len(uncovered)} lone path(s) were not rewritten.")

    return sections, paths


def generate_with_retry(ai_model: SocGenAIChatModel, prompts: list[str], retries: int = 3, base_wait: float = 1.0):
    """Call ai_model.generate with retries on errors (e.g., rate limits)."""
    for attempt in range(retries):
        try:
            return ai_model.generate(prompts)
        except Exception:
            if attempt < retries - 1:
                time.sleep(base_wait * (2 ** attempt))
                continue
            raise
