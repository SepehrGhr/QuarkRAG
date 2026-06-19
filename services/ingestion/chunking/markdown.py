from langchain_text_splitters import MarkdownTextSplitter

def split_text_markdown(text: str, chunk_size: int = 512, chunk_overlap: int = 64) -> list[str]:
    splitter = MarkdownTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    )
    return splitter.split_text(text)
