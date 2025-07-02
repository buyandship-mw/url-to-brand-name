# prompting.py

def build_prompt(input_text: str) -> str:
    """
    Generate a prompt for the model based on the item name or URL.
    """
    return (
        f'Extract the brand name: "{input_text}"\n\n'
        'Respond in JSON: {"name": <value>} or {"error": <reason>}'
    )


def build_url_assess_prompt(url: str) -> str:
    """Prompt asking if the brand can be derived from the URL alone."""
    return (
        "Is the brand/IP name ambiguous from this URL alone? Respond Yes or No.\n"
        f"URL: {url}"
    )


def build_url_extract_prompt(url: str) -> str:
    """Prompt to extract a brand directly from the URL."""
    return (
        "Extract the best name to represent the brand of this product.\n"
        f'"{url}"\n\n'
        'Respond in JSON: {"name": <value>} or {"error": "no brand"}'
    )
