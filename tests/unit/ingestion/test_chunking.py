from services.ingestion.chunking.recursive import split_text_recursive
from services.ingestion.chunking.markdown import split_text_markdown

def test_split_text_recursive():
    text = "Hello world! " * 100
    chunks = split_text_recursive(text, chunk_size=50, chunk_overlap=10)
    assert len(chunks) > 1
    assert all(len(c) <= 50 for c in chunks)

def test_split_text_markdown():
    markdown_text = "# Header 1\nSome text here\n## Header 2\nOther text here"
    chunks = split_text_markdown(markdown_text, chunk_size=100, chunk_overlap=10)
    assert len(chunks) >= 1
