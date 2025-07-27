import os
import requests
import uuid

# --- Hardcoded Configuration ---
PORT_CLIENT_ID = os.getenv("PORT_CLIENT_ID")
PORT_CLIENT_SECRET = os.getenv("PORT_CLIENT_SECRET")
PORT_API_URL = "https://api.getport.io/v1"
PR_BLUEPRINT = "githubPullRequest"
REPO_ENTITY_IDENTIFIER = "PowerToys"

def get_port_api_token():
    # ... (copy the get_port_api_token function here) ...
    print("Requesting new Port API access token...")
    credentials = {'clientId': PORT_CLIENT_ID, 'clientSecret': PORT_CLIENT_SECRET}
    token_response = requests.post(f"{PORT_API_URL}/auth/access_token", json=credentials)
    token_response.raise_for_status()
    print("Successfully received new token.")
    return token_response.json()['accessToken']

def upsert_entities_in_bulk(access_token, blueprint_id, entities):
    # ... (copy the upsert_entities_in_bulk function with the runId logic here) ...
    if not entities: return
    headers = {'Authorization': f'Bearer {access_token}'}
    payload = {"entities": entities}
    run_id = str(uuid.uuid4())
    print(f"Upserting {len(entities)} entities to blueprint {blueprint_id} with runId: {run_id}...")
    api_url = f"{PORT_API_URL}/blueprints/{blueprint_id}/entities/bulk?runId={run_id}"
    response = requests.post(api_url, json=payload, headers=headers)
    if not response.ok:
        print(f"Error: {response.status_code} {response.text}", file=sys.stderr)
    else:
        print(f"✅ Success!")
    return response

def main():
    print("--- Running Bare Metal Test ---")
    port_token = get_port_api_token()

    # Create one single, perfect, hardcoded Pull Request entity
    hardcoded_pr = {
        "identifier": "999999", # A unique, fake ID
        "title": "BARE METAL DIAGNOSTIC TEST",
        "properties": {
            "url": "https://example.com",
            "status": "open",
            "creator": "diagnostic-script",
            "createdAt": "2023-01-01T12:00:00Z",
            "updatedAt": "2023-01-01T12:00:00Z",
            # The properties we are testing
            "assignees": ["alex.chen"],
            "reviewers": ["brenda.smith", "carlos.garcia"]
        },
        "relations": {
            "repository": REPO_ENTITY_IDENTIFIER
        }
    }
    
    # Send just this one entity to Port
    upsert_entities_in_bulk(port_token, PR_BLUEPRINT, [hardcoded_pr])

if __name__ == "__main__":
    main()
