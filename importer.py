import argparse
import difflib
import json
import os
import re
import uuid
from html import escape
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import markdown
import requests
from bs4 import BeautifulSoup, NavigableString
from nanoid import generate


# Main API client class for interacting with Omnivore's GraphQL API
class OmnivoreAPI:
    def __init__(self, api_url: str, api_key: str, verify_certs: bool = True):
        """Initialize API client with URL and authentication key"""
        self.api_url = api_url
        self.api_key = api_key
        self.verify_certs = verify_certs;

    def gql_request(self, query: str, retry: bool = False) -> Dict:
        """
        Make a GraphQL request to Omnivore API
        Handles authentication and basic error checking
        Optionally retries failed requests once if retry=True
        """
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': self.api_key
        }
        for n in range(retry * 1 + 1):
            try:       
                response = requests.post(
                    self.api_url,
                    headers=headers,
                    data=query,
                    verify=self.verify_certs
                )
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                continue
            else:
                break
                
        if 'data' not in data:
            raise Exception(f'No response data: {data}')
        return data['data']

    def get_all_highlighted_articles(self) -> str:
        """
        Generate GraphQL query to fetch all articles that contain highlights
        Returns formatted JSON string with the query
        """
        return json.dumps({
            "query": """
            query Search {
              search(query: "has:highlights") {
                ... on SearchSuccess {
                  edges {
                    node {
                      id
                      highlights {
                        id
                        quote
                      }
                    }
                  }
                }
              }
            }
            """
        })

    def save_page_mutation(self, url: str, content: str, title: str, 
                          labels: List[Dict], source: str = "api_import") -> str:
        """
        Create GraphQL mutation to save a new page/article
        Includes URL, content, title, labels and generates a unique client ID
        Returns formatted JSON string with the mutation
        """
        return json.dumps({
            "query": """
            mutation SavePage($input: SavePageInput!) {
                savePage(input: $input) {
                    ... on SaveSuccess {
                        url
                        clientRequestId
                    }
                    ... on SaveError {
                        errorCodes
                    }
                }
            }
            """,
            "variables": {
                "input": {
                    "url": url,
                    "originalContent": content,
                    "title": title,
                    "source": source,
                    "labels": labels,
                    "clientRequestId": str(uuid.uuid4())
                }
            }
        })

    def save_url_mutation(self, url: str, labels: List[Dict], source: str = "api_import") -> str:
        """
        Create GraphQL mutation to save a new page/article using only URL
        Returns formatted JSON string with the mutation
        """
        return json.dumps({
            "query": """
            mutation SaveUrl($input: SaveUrlInput!) {
                saveUrl(input: $input) {
                    ... on SaveSuccess {
                        url
                        clientRequestId
                    }
                    ... on SaveError {
                        errorCodes
                    }
                }
            }
            """,
            "variables": {
                "input": {
                    "url": url,
                    "source": source,
                    "labels": labels,
                    "clientRequestId": str(uuid.uuid4())
                }
            }
        })

    def update_page_mutation(self, page_id: str, metadata: Dict) -> str:
        """
        Create GraphQL mutation to update page metadata
        Handles description, author, timestamps, and preview image
        Only includes non-None values in the mutation
        """
        input_data = {
            "pageId": page_id,
            "title": metadata["title"],
            "description": metadata["description"],
            "byline": metadata["author"],
            "savedAt": metadata["savedAt"],
            "publishedAt": metadata["publishedAt"],
            "previewImage": metadata["thumbnail"]
        }
        
        return json.dumps({
            "query": """
            mutation UpdatePage($input: UpdatePageInput!) {
                updatePage(input: $input) {
                    ... on UpdatePageError {
                        errorCodes
                    }
                }
            }
            """,
            "variables": {
                "input": {k: v for k, v in input_data.items() if v is not None}
            }
        })

    def set_reading_progress_mutation(self, page_id: str, progress: int) -> str:
        """
        Create GraphQL mutation to update reading progress
        Progress is an integer percentage (0-100)
        """
        return json.dumps({
            "query": """
            mutation SaveArticleReadingProgress($input: SaveArticleReadingProgressInput!) {
                saveArticleReadingProgress(input: $input) {
                    ... on SaveArticleReadingProgressError {
                        errorCodes
                    }
                }
            }
            """,
            "variables": {
                "input": {
                    "id": page_id,
                    "readingProgressPercent": progress
                }
            }
        })


    def archive_mutation(self, page_id: str) -> str:
        """
        Create GraphQL mutation to set page to archived
        """
        
        return json.dumps({
            "query": """
            mutation SetLinkArchived($input: ArchiveLinkInput!) {
                setLinkArchived(input: $input) {
                    ... on ArchiveLinkError {
                        errorCodes
                    }
                }
            }
            """,
            "variables": {
                "input": {
                    "linkId": page_id,
                    "archived": True
                }
            }
        })

    def update_highlight_mutation(self, highlight_id: str, annotation: str) -> str:
        """
        Create GraphQL mutation to update an existing highlight
        Can add or modify the annotation (note) attached to the highlight
        """
        return json.dumps({
            "query": """
            mutation UpdateHighlight($input: UpdateHighlightInput!) {
                updateHighlight(input: $input) {
                    ... on UpdateHighlightSuccess {
                        highlight {
                            id
                        }
                    }
                    ... on UpdateHighlightError {
                        errorCodes
                    }
                }
            }
            """,
            "variables": {
                "input": {
                    "highlightId": highlight_id,
                    "annotation": annotation,
                }
            }
        })

    def set_label_for_highlight_mutation(self, highlight_id: str, labels: Dict) -> str:
        """
        Create GraphQL mutation to set labels for a highlight
        Labels help organize and categorize highlights
        """
        return json.dumps({
            "query": """
            mutation SetLabelsForHighlight($input: SetLabelsForHighlightInput!) {
              setLabelsForHighlight(input: $input) {
                ... on SetLabelsSuccess {
                  labels {
                    id
                  }
                }
              }
            }
            """,
            "variables": {
                "input": {
                    "highlightId": highlight_id,
                    "labels": labels
                }
            }
        })

    def create_highlight_mutation(self, page_id: str, quote: str, annotation: Optional[str] = None) -> str:
        """
        Create GraphQL mutation to save a new highlight
        Generates unique IDs for the highlight
        Can include an optional annotation (note)
        """
        highlight_id = str(uuid.uuid4())
        short_id = generate(size=8)
        
        return json.dumps({
            "query": """
            mutation CreateHighlight($input: CreateHighlightInput!) {
                createHighlight(input: $input) {
                    ... on CreateHighlightSuccess {
                        highlight {
                            id
                        }
                    }
                    ... on CreateHighlightError {
                        errorCodes
                    }
                }
            }
            """,
            "variables": {
                "input": {
                    "id": highlight_id,
                    "articleId": page_id,
                    "shortId": short_id,
                    "quote": quote,
                    "annotation": annotation,
                    "type": "HIGHLIGHT"
                }
            }
        })

    def create_note_mutation(self, page_id: str, note: str) -> str:
        """
        Create GraphQL mutation to save an article-level note
        Similar to create_highlight but specifically for standalone notes
        """
        note_id = str(uuid.uuid4())
        short_id = generate(size=8)
        
        return json.dumps({
            "query": """
            mutation CreateHighlight($input: CreateHighlightInput!) {
                createHighlight(input: $input) {
                    ... on CreateHighlightSuccess {
                        highlight {
                            id
                        }
                    }
                    ... on CreateHighlightError {
                        errorCodes
                    }
                }
            }
            """,
            "variables": {
                "input": {
                    "id": note_id,
                    "articleId": page_id,
                    "shortId": short_id,
                    "annotation": note,
                    "type": "NOTE"
                }
            }
        })

def html_to_text_map(html_content: str) -> Tuple[str, list]:
    """
    Convert HTML to plain text while maintaining a mapping of text positions 
    to HTML positions.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    plain_text = []
    position_map = []
    html_pos = 0
    
    EXCLUDED_TAGS = {'style', 'script'}
    
    def process_node(node):
        nonlocal html_pos
        
        if isinstance(node, NavigableString):
            if node.parent.name not in EXCLUDED_TAGS:
                node_str = str(node)
                # Skip whitespace-only strings
                if not node_str.strip():
                    html_pos += len(node_str)
                    return
                    
                # Find position of this text in original HTML
                original_pos = html_pos
                while html_pos < len(html_content):
                    if html_content[html_pos:].startswith(escape(node_str)):
                        break
                    html_pos += 1
                
                # If we couldn't find the exact text, revert and try with stripped version
                if html_pos >= len(html_content):
                    html_pos = original_pos
                    node_str = node_str.strip()
                    while html_pos < len(html_content):
                        if html_content[html_pos:].startswith(escape(node_str)):
                            break
                        html_pos += 1
                
                # Map each character position
                for i, char in enumerate(node_str):
                    plain_text.append(char)
                    position_map.append(html_pos + i)
                
                html_pos += len(node_str)
        
        if hasattr(node, 'children'):
            for child in node.children:
                process_node(child)
    
    process_node(soup)
    return ''.join(plain_text), position_map

def find_best_match(text: str, pattern: str, cutoff: float = 0.6) -> Tuple[Optional[int], Optional[int], float]:
    """
    Find the best fuzzy match with more precise boundary detection.
    """
    text_words = text.split()
    pattern_words = pattern.split()
    window_size = len(pattern_words)
    best_start_index = None
    best_end_index = None
    best_ratio = 0.0
    
    # Try exact match first
    if pattern in text:
        start = text.index(pattern)
        return start, start + len(pattern), 1.0
    
    # Get the first word of the pattern for boundary checking
    first_pattern_word = pattern_words[0] if pattern_words else ""
    
    # Sliding window over tokenized words with variable window size
    for window in range(window_size - 1, window_size + 2):  # Try different window sizes
        for i in range(len(text_words) - window + 1):
            candidate_words = text_words[i:i + window]
            candidate = ' '.join(candidate_words)
            
            # Compute similarity ratio
            ratio = difflib.SequenceMatcher(None, candidate, pattern).ratio()
            
            if ratio > best_ratio and ratio >= cutoff:
                # Calculate character-level indices in original text
                prefix = ' '.join(text_words[:i])
                char_start_index = len(prefix) + (1 if i > 0 else 0)
                char_end_index = char_start_index + len(candidate)
                
                # Only adjust start boundary if we don't have an exact match for the first word
                if not text_words[i].startswith(first_pattern_word):
                    while (char_start_index > 0 and 
                           text[char_start_index - 1].isalnum()):
                        char_start_index -= 1
                
                # Adjust end boundary if needed
                while (char_end_index < len(text) and 
                       text[char_end_index - 1].isalnum()):
                    char_end_index += 1
                
                best_start_index = char_start_index
                best_end_index = char_end_index
                best_ratio = ratio
    
    return best_start_index, best_end_index, best_ratio

def find_markdown_in_html(html_content: str, markdown_content: str) -> Tuple[Optional[str], Optional[int], Optional[int], float]:
    """
    Find the portion of HTML that corresponds to the given Markdown content.
    """
    # Convert markdown to text
    md_html = markdown.markdown(markdown_content)
    md_soup = BeautifulSoup(md_html, 'html.parser')
    md_text = md_soup.get_text()
    
    # Convert HTML to text while maintaining position mapping
    html_text, position_map = html_to_text_map(html_content)
    
    # Find best matching span in the text
    start_idx, end_idx, similarity = find_best_match(html_text, md_text)
    
    if start_idx is None:
        return None, None, None, 0
    
    # Adjust indices to ensure they're within bounds
    start_idx = max(0, min(start_idx, len(position_map) - 1))
    end_idx = max(0, min(end_idx, len(position_map) - 1))
    
    # Convert text positions back to HTML positions
    html_start = position_map[start_idx]
    html_end = position_map[end_idx]
    
    # Extract the HTML span
    matching_html = html_content[html_start:html_end]
    
    return matching_html, html_start, html_end, similarity

def clean_html(html_content):
    """
    Clean and standardize HTML content
    - Wraps content in html/body tags
    - Strips data- attributes
    - Normalizes whitespace
    - Replace proxy with original URL
    """
    html_content = "<html><body>" + html_content + "</body></html>"

    proxy_regex = r'https://proxy-prod\.omnivore-image-cache\.app/\d+x\d+,[A-Za-z0-9_-]+/'

    html_content = re.sub(proxy_regex, '', html_content)

    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove data attributes
    for tag in soup.find_all(lambda t: any(i.startswith('data-') for i in t.attrs)):
        for attr in list(tag.attrs):
            if attr.startswith('data-'):
                del tag.attrs[attr]

    return str(soup)

def find_closest_match(target: str, candidates: list[str]) -> str:
    """
    Find the most similar string in a list compared to a target string
    Uses SequenceMatcher to calculate similarity ratios
    Useful for matching highlight quotes that might have slight differences
    """
    if not candidates:
        raise ValueError("Candidates list cannot be empty")
        
    def similarity(a: str, b: str) -> float:
        return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    closest = max(candidates, key=lambda x: similarity(target, x))
    
    return closest

def add_highlight_tag(content: str, highlight: Dict) -> str:
    """
    Add Omnivore highlight markers to HTML content
    Inserts span tags at the start and end of highlighted text
    Returns modified HTML string
    """
    output = (content[:highlight["start_index"]] +
             '<span data-omnivore-highlight-start="true"></span>' +
             content[highlight["start_index"]:highlight["end_index"]] +
             '<span data-omnivore-highlight-end="true"></span>' +
             content[highlight["end_index"]:])
    return output

def parse_highlights_file(file_path: str) -> Dict:
    """
    Parse a markdown file containing highlights and notes
    Format:
    - Article notes are plain text
    - Highlights start with >
    - Labels start with #
    - Notes follow highlights without special markers
    Returns dict with article_note and list of highlights
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        return {"article_note": None, "highlights": []}

    blocks = re.split(r'\n\n+', content.strip())
    
    result = {
        "article_note": None,
        "highlights": []
    }
    
    current_highlight = None
    
    for block in blocks:
        lines = block.strip().split('\n')
        
        # Parse quotes, labels, and notes according to their markers
        if lines[0].startswith('>'):
            if current_highlight:
                result["highlights"].append(current_highlight)
            
            quote = '\n'.join([x.lstrip('> ').strip() for x in lines])
            current_highlight = {
                "quote": quote,
                "labels": [],
                "notes": None
            }
            
        elif lines[0].startswith('#'):
            if current_highlight:
                label = lines[0].lstrip('#').strip()
                current_highlight["labels"].append({
                    "name": label
                })
                
        else:
            if not current_highlight:
                if result["article_note"]:
                    result["article_note"] = result["article_note"] + "\n\n" + block.strip()
                else:
                    result["article_note"] = block.strip()
            else:
                if current_highlight["notes"]:
                    current_highlight["notes"] = current_highlight["notes"] + "\n\n" + block.strip()
                else:
                    current_highlight["notes"] = block.strip()
    
    if current_highlight:
        result["highlights"].append(current_highlight)
        
    return result


def save_page(api, metadata: Dict, content: Optional[str] = None, 
              highlights_data: Optional[Dict] = None) -> str:
    """
    Save a new page to Omnivore with optional content and highlights
    Returns the page ID if successful
    """
    labels = [{"name": label} for label in metadata.get("labels", [])]
    
    if content:
        if highlights_data:
            content = process_highlights_in_content(content, highlights_data)
                
        save_page_mutation = api.save_page_mutation(
            url=metadata["url"],
            content=content,
            title=metadata["title"],
            labels=labels
        )
        result = api.gql_request(save_page_mutation)
    else:
        save_url_mutation = api.save_url_mutation(
            url=metadata["url"],
            labels=labels
        )
        result = api.gql_request(save_url_mutation)
    
    page_id = result["savePage"].get("clientRequestId")
    if not page_id:
        raise Exception(f"Failed to save article: {metadata['title']}")
        
    print(f"Successfully imported with ID: {page_id}")
    return page_id

def process_highlights_in_content(content: str, highlights_data: Dict) -> str:
    """
    Process and add highlight tags to the content
    Returns modified content with highlight tags
    """
    for highlight in highlights_data["highlights"]:
        highlight["html"], highlight["start_index"], highlight["end_index"], highlight["ratio"] = \
            find_markdown_in_html(content, highlight["quote"])
        if highlight["start_index"]:
            content = add_highlight_tag(content, highlight)
        else:
            print(f"Failed to import highlight: {highlight['quote'][:50]}...")
    return content

def update_page_metadata(api, page_id: str, metadata: Dict):
    """Update page metadata including reading progress"""
    metadata_mutation = api.update_page_mutation(
        page_id=page_id,
        metadata=metadata
    )
    api.gql_request(metadata_mutation)

    if metadata["state"] == "Archived":
        archive_mutation = api.archive_mutation(
            page_id = page_id)
        api.gql_request(archive_mutation)
    
    if metadata["readingProgress"] > 0:
        reading_progress_mutation = api.set_reading_progress_mutation(
            page_id=page_id,
            progress=metadata["readingProgress"]
        )
        api.gql_request(reading_progress_mutation)

def process_article_note(api, page_id: str, article_note: str):
    """Add article-level note if it exists"""
    if article_note:
        note_mutation = api.create_note_mutation(
            page_id=page_id,
            note=article_note
        )
        api.gql_request(note_mutation)

def process_highlights(api, page_id: str, highlights_data: Dict):
    """Process and add highlights, their notes, and labels"""
    if not highlights_data or not highlights_data["highlights"]:
        return
        
    highlights_query = api.get_all_highlighted_articles()
    all_highlights = api.gql_request(highlights_query, retry=True)
    page_highlights = [x["node"]["highlights"] for x in all_highlights['search']['edges'] 
                      if x['node']['id'] == page_id]
    
    if not page_highlights:
        return
        
    for highlight in highlights_data["highlights"]:
        highlight_quotes = [x["quote"] for x in page_highlights[0] if x["quote"]]
        closest_match = find_closest_match(highlight["quote"], highlight_quotes)
        highlight_result = [x for x in page_highlights[0] if x["quote"] == closest_match]
        
        if not highlight_result:
            continue
            
        if highlight["notes"]:
            highlight_note_mutation = api.update_highlight_mutation(
                highlight_id=highlight_result[0]["id"],
                annotation=highlight["notes"]
            )
            api.gql_request(highlight_note_mutation)
            
        if highlight["labels"]:
            highlight_label_mutation = api.set_label_for_highlight_mutation(
                highlight_id=highlight_result[0]["id"],
                labels=highlight["labels"]
            )
            api.gql_request(highlight_label_mutation)

def import_article(api, metadata: Dict, content: Optional[str] = None,
                  highlights_data: Optional[Dict] = None) -> str:
    """
    Import a single article with its content, highlights, and labels
    Main orchestration function that calls other specialized functions
    """
    # Save the page and get its ID
    page_id = save_page(api, metadata, content, highlights_data)
    
    # Update page metadata and reading progress
    update_page_metadata(api, page_id, metadata)
    
    # Process highlights if they exist
    if highlights_data:
        # Add article-level note
        process_article_note(api, page_id, highlights_data["article_note"])
        
        # Process highlights, their notes and labels
        process_highlights(api, page_id, highlights_data)
    
    return page_id

def import_folder(api, folder_path: str):
    """Import an entire folder of content into Omnivore"""

    path = Path(folder_path)
    # Load metadata
    metadata = []

    
    # Iterate through all JSON files in the directory
    for json_file in path.glob('*.json'):
        try:
            with open(json_file, 'r', encoding='utf-8') as file:
                # Load JSON data
                data = json.load(file)
                
                # If the data is already a list, extend combined_data
                if isinstance(data, list):
                    metadata.extend(data)
                # If it's a single object, append it
                else:
                    metadata.append(data)
                    
            print(f"Successfully processed: {json_file.name}")
        except json.JSONDecodeError as e:
            print(f"Error reading {json_file.name}: Invalid JSON format - {str(e)}")
        except Exception as e:
            print(f"Error processing {json_file.name}: {str(e)}")
            
    content_dir = os.path.join(folder_path, "content")
    highlights_dir = os.path.join(folder_path, "highlights")
    
    for article in metadata:
        url = article['url']
        slug = article['slug']
        
        print(f"\nImporting: {article['title']}")
        
        # Get content if available
        content_file = os.path.join(content_dir, f"{slug}.html")
        content = None
        if os.path.exists(content_file):
            with open(content_file, 'r', encoding='utf-8') as f:
                content = clean_html(f.read())
        
        # Get highlights if available
        highlights_file = os.path.join(highlights_dir, f"{slug}.md")
        highlights_data = parse_highlights_file(highlights_file)
        
        try:
            page_id = import_article(
                api=api,
                metadata=article,
                content=content,
                highlights_data=highlights_data
            )
        except Exception as e:
            print(f"Failed to import {article['title']}: {str(e)}")

def parse_args():
    parser = argparse.ArgumentParser(description='Import articles and highlights into Omnivore')
    parser.add_argument('--api-key', required=True,
                      help='Omnivore API key from instance you will be importing to (found at /settings/api)')
    parser.add_argument('--api-url', default='https://api-prod.omnivore.app/api/graphql',
                      help='Omnivore API endpoint (default: https://api-prod.omnivore.app/api/graphql)')
    parser.add_argument('--folder', required=True,
                      help='Path to folder containing contents of extracted archive')
    parser.add_argument('--ignore-invalid-certs', action="store_true",
                      help='Bypass validating TSL certificates during API calls')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    try:
        importer = OmnivoreAPI(args.api_url, args.api_key, not args.ignore_invalid_certs)
        import_folder(importer, args.folder)
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)
