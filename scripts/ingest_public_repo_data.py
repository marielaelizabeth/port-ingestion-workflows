import os
import sys
import requests

# --- Configuration ---
# Fetch credentials from GitHub Actions secrets
PORT_CLIENT_ID = os.getenv("PORT_CLIENT_ID")
PORT_CLIENT_SECRET = os.getenv("PORT_CLIENT_SECRET")
GH_TOKEN = os.getenv("GH_PAT")

# The public repository we want to ingest data from
TARGET_REPO = "microsoft/PowerToys"
REPO_ENTITY_IDENTIFIER = "PowerToys"

# Port API and Blueprint configuration
PORT_API_URL = "https://api.getport.io/v1"
PR_BLUEPRINT = "githubPullRequest"  # The identifier of your PR blueprint
ISSUE_BLUEPRINT = "githubIssue"   # The identifier of your Issue blueprint

# --- Helper Functions ---

def get_port_api_token():
    """Gets a Port API access token."""
    credentials = {'clientId': PORT_CLIENT_ID, 'clientSecret': PORT_CLIENT_SECRET}
    token_response = requests.post(f"{PORT_API_URL}/auth/access_token", json=credentials)
    token_response.raise_for_status()
    return token_response.json()['accessToken']

def fetch_github_data(endpoint):
    """Fetches paginated data from the GitHub API."""
    all_data = []
    url = f"https://api.github.com/repos/{TARGET_REPO}/{endpoint}"
    headers = {
        'Authorization': f'token {GH_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    params = {'state': 'all', 'per_page': 100} # Fetch all states, 100 per page

    while url:
        print(f"Fetching from {url}...")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        all_data.extend(response.json())
        
        # Handle pagination
        if 'next' in response.links:
            url = response.links['next']['url']
        else:
            url = None
        # Subsequent requests don't need params
        params = None
            
    return all_data

def upsert_port_entity(access_token, blueprint_id, entity_payload):
    """Creates or updates an entity in Port."""
    headers = {'Authorization': f'Bearer {access_token}'}
    # Using upsert=true is key to avoid duplicates on subsequent runs
    # Using create_missing_related_entities=true will automatically create the 'vercel' repo entity if it doesn't exist
    response = requests.post(
        f"{PORT_API_URL}/blueprints/{blueprint_id}/entities?upsert=true&create_missing_related_entities=true",
        json=entity_payload,
        headers=headers
    )
    # If the response is not successful, print the error and continue
    if not response.ok:
        print(f"Error upserting entity {entity_payload.get('identifier')}: {response.status_code} {response.text}")
    return response

# --- Main Logic ---

def main():
    if not all([PORT_CLIENT_ID, PORT_CLIENT_SECRET, GH_TOKEN]):
        print("Error: Missing required secrets. Please check repository configuration.", file=sys.stderr)
        sys.exit(1)

    print("Authenticating with Port...")
    port_token = get_port_api_token()
    
    # Ingest Pull Requests
    print("\n--- Ingesting Pull Requests ---")
    pull_requests = fetch_github_data("pulls")
    print(f"Found {len(pull_requests)} pull requests.")
    
    for pr in pull_requests:
        entity = {
            "identifier": str(pr["number"]),
            "title": pr["title"],
            "properties": {
                "url": pr["html_url"],
                "status": pr["state"],
                "creator": pr.get("user", {}).get("login"),
                "createdAt": pr["created_at"],
                "updatedAt": pr["updated_at"]
            },
            "relations": {
                "repository": REPO_ENTITY_IDENTIFIER
            }
        }
        upsert_port_entity(port_token, PR_BLUEPRINT, entity)
    print("Finished ingesting Pull Requests.")

    # Ingest Issues
    print("\n--- Ingesting Issues ---")
    issues = fetch_github_data("issues")
    print(f"Found {len(issues)} issues.")

    for issue in issues:
        # GitHub's API returns PRs in the Issues endpoint, so we skip them.
        if 'pull_request' in issue:
            continue
            
        entity = {
            "identifier": str(issue["number"]),
            "title": issue["title"],
            "properties": {
                "url": issue["html_url"],
                "status": issue["state"],
                "creator": issue.get("user", {}).get("login")
            },
            "relations": {
                "repository": REPO_ENTITY_IDENTIFIER
            }
        }
        upsert_port_entity(port_token, ISSUE_BLUEPRINT, entity)
    print("Finished ingesting Issues.")
    print("\nIngestion complete!")

if __name__ == "__main__":
    main()
