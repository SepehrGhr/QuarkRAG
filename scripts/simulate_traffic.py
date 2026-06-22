import os
import json
import time

# We will use urllib for queries, but we need requests for multipart/form-data.
# Let's try importing requests, or print a helpful error if it's missing.
try:
    import requests
except ImportError:
    print("Please install the 'requests' library to run this script: pip install requests")
    exit(1)

API_BASE_URL = "http://localhost:8000"
DUMMY_DATA_DIR = os.path.join(os.path.dirname(__file__), "dummy_data")

# Dummy content for the files containing non-general, fictional facts to test RAG retrieval
DOCUMENTS = {
    "quarkrag_secret_architecture.txt": (
        "The QuarkRAG system uses a secret engine called 'Nebula-9' for vector compression, developed in 2026. "
        "Nebula-9 achieves a 99.8% compression ratio by utilizing a proprietary algorithm called 'QuantumSqueeze'. "
        "The primary maintainer of this engine is a researcher named Dr. Elysia Vance, who operates from a remote laboratory in Svalbard. "
        "Nebula-9 has a known limitation where it requires exactly 12 volts of power to stabilize."
    ),
    "company_policy_v21.txt": (
        "According to the ACME Corp Policy v21.4, employees are allowed to claim expenses for 'interstellar travel' "
        "up to 5,000 Galactic Credits per quarter, provided they get approval from the Chief Exploration Officer, Captain Leo Starfinder. "
        "Standard office catering must include at least 20% Martian potatoes to support the local Martian agriculture initiatives. "
        "All official reports must be written in the 'Zeta-Font' typeface to maintain corporate identity."
    ),
    "strange_materials.txt": (
        "In 2025, scientists synthesized a new material called 'Vibranium-Lite'. Unlike standard Vibranium, "
        "Vibranium-Lite melts at exactly 42 degrees Celsius and becomes superconductive when exposed to cherry soda. "
        "It was discovered by accident during a picnic at the CERN cafeteria by a student named Marcus."
    )
}

QUERIES = [
    {"question": "What secret engine does QuarkRAG use for vector compression?", "namespace": "presentation", "top_k": 3},
    {"question": "Who is the primary maintainer of the Nebula-9 engine?", "namespace": "presentation", "top_k": 3},
    {"question": "Where is Dr. Elysia Vance's laboratory located?", "namespace": "presentation", "top_k": 3},
    {"question": "How many Galactic Credits can employees claim for interstellar travel under ACME Policy v21.4?", "namespace": "presentation", "top_k": 3},
    {"question": "Who must approve interstellar travel expenses at ACME?", "namespace": "presentation", "top_k": 3},
    {"question": "Why does ACME office catering need to include Martian potatoes?", "namespace": "presentation", "top_k": 3},
    {"question": "What is the melting point of Vibranium-Lite?", "namespace": "presentation", "top_k": 3},
    {"question": "How does Vibranium-Lite become superconductive?", "namespace": "presentation", "top_k": 3},
]

def setup_dummy_data():
    """Create the dummy data directory and files."""
    if not os.path.exists(DUMMY_DATA_DIR):
        os.makedirs(DUMMY_DATA_DIR)
        print(f"Created directory: {DUMMY_DATA_DIR}")
    
    for filename, content in DOCUMENTS.items():
        filepath = os.path.join(DUMMY_DATA_DIR, filename)
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"Created file: {filepath}")

def upload_documents():
    """Upload all dummy documents to the ingestion service."""
    print("\n--- Starting Document Ingestion ---")
    for filename in DOCUMENTS.keys():
        filepath = os.path.join(DUMMY_DATA_DIR, filename)
        url = f"{API_BASE_URL}/documents/upload"
        
        with open(filepath, 'rb') as f:
            files = {'file': (filename, f, 'text/plain')}
            data = {
                'namespace': 'presentation',
                'chunking_strategy': 'recursive'
            }
            try:
                start_time = time.time()
                response = requests.post(url, files=files, data=data)
                latency = (time.time() - start_time) * 1000
                print(f"[Upload] {filename} -> Status: {response.status_code}, Latency: {latency:.2f}ms")
            except Exception as e:
                print(f"[Error] Failed to upload {filename}: {e}")
        
        # Slight delay to stagger ingestion metrics
        time.sleep(0.5)

def run_queries():
    """Run standard queries, repeating some to trigger cache hits."""
    print("\n--- Starting Query Phase ---")
    
    # Send all unique queries first
    for q in QUERIES:
        send_query(q, label="Initial Query")
        time.sleep(1) # Delay between requests
    
    print("\n--- Triggering Cache Hits ---")
    # Pick a few queries and repeat them rapidly to ensure cache hits
    for i in range(3):
        q = QUERIES[i]
        for _ in range(4):
            send_query(q, label="Cache Hit Test")
            time.sleep(0.2)
    
    print("\n--- Simulating Errors ---")
    # Send a malformed request to trigger a 422 Unprocessable Entity
    send_query({"invalid_field": "test"}, label="Malformed Request")
    
    # Send a query to a non-existent namespace (assuming it might fail or return empty)
    send_query({"question": "What is dark matter?", "namespace": "unknown_namespace_404", "top_k": 3}, label="Unknown Namespace")

def send_query(payload, label="Query"):
    """Helper to send a JSON POST request to the query endpoint."""
    url = f"{API_BASE_URL}/query"
    try:
        start_time = time.time()
        response = requests.post(url, json=payload)
        latency = (time.time() - start_time) * 1000
        print(f"[{label}] Status: {response.status_code}, Latency: {latency:.2f}ms | Payload: {json.dumps(payload)[:50]}...")
    except Exception as e:
        print(f"[Error] Query failed: {e}")

if __name__ == "__main__":
    print("Setting up test data...")
    setup_dummy_data()
    
    # Give the user a moment to start recording/checking dashboards if they want
    print("\nData created! Will start sending traffic in 3 seconds...")
    time.sleep(3)
    
    upload_documents()
    
    # Wait for ingestion/embedding pipeline to finish processing via Kafka
    print("\nWaiting 10 seconds for Kafka to process embeddings...")
    time.sleep(10)
    
    run_queries()
    print("\nTraffic generation complete! Check your Grafana and Jaeger dashboards.")
