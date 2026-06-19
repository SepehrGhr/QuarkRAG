from langchain_text_splitters import RecursiveCharacterTextSplitter

def split_text_recursive(text: str, chunk_size: int = 512, chunk_overlap: int = 64) -> list[str]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len
    )
    return splitter.split_text(text)
